"""公開免費官方資料版：python -m streamlit run app.py。規格見README。"""
from __future__ import annotations
import hashlib, io, json, math, re, sqlite3, time, zipfile
from datetime import datetime, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit
import pandas as pd
import requests
import streamlit as st
import altair as alt
from filelock import FileLock
ROOT=Path(__file__).resolve().parent
CACHE=ROOT/'cache';CACHE.mkdir(exist_ok=True)
API='https://openapi.twse.com.tw/v1'
RWD='https://www.twse.com.tw/rwd/zh'
HOME='https://www.twse.com.tw/res/data/zh/home/values.json'
WEIGHTS={'M':10,'I':15,'F':23,'V':5,'T':22,'L':10,'H':5,'R':10}
MODEL_VERSION='V5-Intraday-Preview'
MAX_STOP_PCT=.08
MAX_ENTRY_EXTENSION_ATR=3.0
MIN_PULLBACK_RR=2.0
NAMES={'M':'大盤','I':'產業','F':'營運','V':'估值','T':'趨勢','L':'量能','H':'籌碼','R':'進場品質'}
class SourceError(RuntimeError):pass
class Halt(SourceError):pass
def today():return datetime.now(ZoneInfo('Asia/Taipei')).date()
class Client:
    """跨程序序列化；磁碟快取；15秒最小間隔；200次/日；限制即停止。"""
    def __init__(self, offline=False):
        self.offline = offline
        self.observed=[];self.failures=[];self.progress=None
        self.seed_note=bootstrap_seed()
    def get(self, url, params=None, method='GET', ttl=86400):
        # 所有來源為官方公開資料，不使用token
        key = hashlib.sha256(json.dumps([url,params,method],sort_keys=True).encode()).hexdigest()
        with FileLock(str(CACHE/'network.lock'), timeout=120):
            db = sqlite3.connect(CACHE/'http.sqlite')
            try:
                db.execute('CREATE TABLE IF NOT EXISTS cache (k TEXT PRIMARY KEY, ts REAL, body TEXT)')
                db.execute('CREATE TABLE IF NOT EXISTS control (k TEXT PRIMARY KEY, v TEXT)')
                cached = db.execute('SELECT ts,body FROM cache WHERE k=?',(key,)).fetchone()
                if cached and time.time()-cached[0] < ttl:
                    self.observed.append({'來源':url,'原始擷取時間UTC':datetime.fromtimestamp(cached[0],ZoneInfo('UTC')).isoformat(),'模式':'快取'})
                    return cached[1]
                if self.offline:
                    if cached:
                        self.observed.append({'來源':url,'原始擷取時間UTC':datetime.fromtimestamp(cached[0],ZoneInfo('UTC')).isoformat(),'模式':'離線快取（可能過期）'})
                        return cached[1]
                    raise SourceError('離線模式沒有這筆快取')
                def read(k, default):
                    r=db.execute('SELECT v FROM control WHERE k=?',(k,)).fetchone()
                    return r[0] if r else default
                def write(k,v): db.execute('INSERT OR REPLACE INTO control VALUES (?,?)',(k,str(v)))
                if float(read('blocked_until',0)) > time.time():
                    raise Halt('來源先前回覆限制或拒絕，已停止全體網路請求；冷卻期尚未結束。')
                endpoint=urlsplit(url).netloc+urlsplit(url).path
                query_date=str((params or {}).get('date','')) if isinstance(params,dict) else ''
                label=endpoint+(' 日期='+query_date if query_date else '')
                headers={'User-Agent':'TaiwanWaveDashboard/1.0 (personal research; cached; rate-limited)'}
                # 最多三次；每次實際嘗試均計入預算。POST與SSL錯誤不自動重試。
                attempts=3 if method=='GET' else 1
                for attempt in range(attempts):
                    if float(read('blocked_until',0))>time.time():
                        raise Halt('來源冷卻期尚未結束；已停止請求。')
                    day=today().isoformat();count=int(read('count:'+day,0))
                    if count>=200:raise Halt('本機今日200次請求預算用完；已保存的快取不受影響。')
                    interval=[15,30,60][attempt]
                    time.sleep(max(0,interval-(time.time()-float(read('last',0)))))
                    write('last',time.time());write('count:'+day,count+1);db.commit()
                    if self.progress:
                        self.progress(f'連線 {attempt+1}/{attempts}：{label}'+('；暫時失敗後退避重試' if attempt else ''))
                    try:
                        r=requests.request(method,url,params=params if method=='GET' else None,
                                           data=params if method=='POST' else None,headers=headers,timeout=(15,60))
                        break
                    except requests.RequestException as exc:
                        if isinstance(exc,requests.exceptions.SSLError):reason='SSL憑證／安全連線失敗';retryable=False
                        elif isinstance(exc,requests.exceptions.ConnectTimeout):reason='建立連線逾時';retryable=True
                        elif isinstance(exc,requests.exceptions.ReadTimeout):reason='等待來源資料逾時';retryable=True
                        elif isinstance(exc,requests.exceptions.Timeout):reason='連線／讀取逾時';retryable=True
                        elif isinstance(exc,requests.exceptions.ConnectionError):reason='無法連線（可能為DNS、連線重設或代理連線問題）';retryable=True
                        else:reason='請求失敗（'+type(exc).__name__+'）';retryable=False
                        self.failures.append({'來源':endpoint,'查詢日期':query_date,'嘗試次數':attempt+1,
                                              '原因':reason,'時間UTC':datetime.now(ZoneInfo('UTC')).isoformat()})
                        if not retryable or attempt+1>=attempts:
                            raise SourceError(f'{reason}；已嘗試{attempt+1}次。來源：{label}。'
                                              '已成功下載資料仍在快取；稍後重新按查詢即可續補，這則訊息不代表已確認IP遭封鎖。') from None
                if r.status_code in (401,402,403,429):
                    write('blocked_until',time.time()+86400);db.commit()
                    raise Halt(f'來源回覆HTTP {r.status_code}；程式停止全體請求24小時（本機保護設定，非官方封鎖期限）。來源：{label}')
                if r.status_code >= 500:
                    write('blocked_until',time.time()+900);db.commit()
                    raise Halt('來源伺服器異常；程式停止請求15分鐘。來源：'+label+'。')
                if not r.ok: raise SourceError(f'來源回覆HTTP {r.status_code}，不重試。來源：{label}')
                r.encoding='utf-8';body=r.text
                db.execute('INSERT OR REPLACE INTO cache VALUES (?,?,?)',(key,time.time(),body));db.commit()
                self.observed.append({'來源':url,'原始擷取時間UTC':datetime.now(ZoneInfo('UTC')).isoformat(),'模式':'新下載'})
                return body
            finally: db.close()

def cache_database():
    db=sqlite3.connect(CACHE/'http.sqlite')
    db.execute('CREATE TABLE IF NOT EXISTS cache (k TEXT PRIMARY KEY, ts REAL, body TEXT)')
    db.execute('CREATE TABLE IF NOT EXISTS control (k TEXT PRIMARY KEY, v TEXT)')
    return db


def export_cache():
    """只備份公開原始資料；不包含限流計數、鎖、持股或登入資料。"""
    output=io.BytesIO()
    with FileLock(str(CACHE/'network.lock'),timeout=120):
        db=cache_database()
        try:
            count=db.execute('SELECT COUNT(*) FROM cache').fetchone()[0]
            if not count:raise SourceError('尚無行情快取，請先完成一次查詢。')
            with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
                z.writestr('manifest.json',json.dumps({'format':'taiwan-wave-public-cache','version':1,'records':count,'exported_utc':datetime.now(ZoneInfo('UTC')).isoformat()}))
                with z.open('cache.jsonl','w') as stream:
                    for key,ts,body in db.execute('SELECT k,ts,body FROM cache ORDER BY k'):
                        row={'key':key,'ts':ts,'body':body}
                        stream.write((json.dumps(row,ensure_ascii=False)+'\n').encode('utf-8'))
        finally:db.close()
    return output.getvalue()


def restore_cache(data,seed_signature=None):
    """逐筆合併，保留較新資料與現有封鎖/請求計數；不解壓任意檔案。"""
    if len(data)>100*1024*1024:raise SourceError('備份壓縮檔超過100MB限制')
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            if sorted(z.namelist())!=['cache.jsonl','manifest.json']:
                raise SourceError('僅接受本程式匯出的快取備份，不解壓其他檔案。')
            if sum(x.file_size for x in z.infolist())>256*1024*1024:
                raise SourceError('備份解壓後超過256MB限制')
            manifest=json.loads(z.read('manifest.json'))
            if not isinstance(manifest,dict):raise SourceError('備份說明格式無效')
            if manifest.get('format')!='taiwan-wave-public-cache' or manifest.get('version')!=1:
                raise SourceError('快取備份格式不相容')
            expected=manifest.get('records')
            if not isinstance(expected,int) or not 0<=expected<=2000:raise SourceError('備份筆數異常')
            with FileLock(str(CACHE/'network.lock'),timeout=120):
                db=cache_database();count=0
                try:
                    with z.open('cache.jsonl') as stream:
                        while True:
                            line=stream.readline(20*1024*1024+1)
                            if not line:break
                            if len(line)>20*1024*1024:raise SourceError('備份單筆資料過大')
                            row=json.loads(line)
                            if not isinstance(row,dict):raise SourceError('快取紀錄格式無效')
                            key=row.get('key');ts=row.get('ts');body=row.get('body')
                            if not isinstance(key,str) or not re.fullmatch('[0-9a-f]{64}',key):raise SourceError('快取索引無效')
                            if not isinstance(ts,(float,int)) or not math.isfinite(ts) or ts<0 or ts>time.time()+300:
                                raise SourceError('快取擷取時間無效')
                            if not isinstance(body,str) or len(body)>10*1024*1024:raise SourceError('快取資料無效')
                            db.execute('INSERT INTO cache VALUES (?,?,?) ON CONFLICT(k) DO UPDATE SET ts=excluded.ts,body=excluded.body WHERE excluded.ts>cache.ts',(key,float(ts),body))
                            count+=1
                            if count>2000:raise SourceError('備份筆數超限')
                    if count!=expected:raise SourceError('備份筆數不符')
                    changed=db.total_changes
                    if seed_signature:
                        db.execute('INSERT OR REPLACE INTO control VALUES (?,?)',('seed_signature',seed_signature))
                    db.commit()
                    return changed
                except Exception:
                    db.rollback();raise
                finally:db.close()
    except (zipfile.BadZipFile,KeyError,ValueError,UnicodeError,TypeError) as exc:
        raise SourceError('無法讀取快取備份，請使用本程式匯出的data_cache.zip。') from exc


def bootstrap_seed():
    seed=ROOT/'data_cache.zip'
    if not seed.is_file():return None
    try:
        info=seed.stat();signature=str(info.st_size)+':'+str(info.st_mtime_ns)
        if info.st_size>100*1024*1024:raise SourceError('備份壓縮檔超過100MB限制')
        with FileLock(str(CACHE/'network.lock'),timeout=120):
            db=cache_database()
            try:r=db.execute('SELECT v FROM control WHERE k=?',('seed_signature',)).fetchone()
            finally:db.close()
        if r and r[0]==signature:return '已載入專案內的快取備份'
        changed=restore_cache(seed.read_bytes(),seed_signature=signature)
        return f'已從專案備份合併{changed}筆快取；仍依日期檢查新資料'
    except (SourceError,OSError) as exc:
        return '專案快取備份未載入：'+str(exc)



def number(x):
    # 原版每個數字都呼叫pandas；改用等價的快速純數值轉換。
    try:return float(str(x).replace(',','').replace('%','').replace('−','-').strip())
    except (TypeError,ValueError):return float('nan')


def historical_ttl(dt, refresh_recent=False):
    # 已發布歷史預設沿用；只有使用者要求複查更正才重抓最近7日。
    if refresh_recent and 0<=(today()-dt).days<=7:return 0
    return 21600 if dt>=today() else math.inf

def normdate(v):
    s=re.sub(r'[^0-9]','',str(v))
    if len(s)==7:s=str(int(s[:3])+1911)+s[3:]
    return datetime.strptime(s,'%Y%m%d').date()
def score4(conditions):
    return 25*sum(bool(x) for x in conditions)
def js(client,url,params=None,ttl=86400):
    try:return json.loads(client.get(url,params,ttl=ttl))
    except json.JSONDecodeError:raise SourceError('來源未回傳有效JSON；停止本次操作，不自動重試。') from None

def snapshot(client,path,ttl=86400):
    data=js(client,API+path,ttl=ttl)
    if not isinstance(data,list) or not data:raise SourceError('官方公開資料空白：'+path)
    return pd.DataFrame(data)

def registry(client,include_funds=True,refresh=False):
    ttl=0 if refresh else 7*86400
    companies=snapshot(client,'/opendata/t187ap03_L',ttl)
    funds=snapshot(client,'/opendata/t187ap47_L',ttl) if include_funds else pd.DataFrame(columns=['基金代號','基金簡稱','基金類型'])
    for d,k in [(companies,'公司代號'),(funds,'基金代號')]:
        if k not in d:raise SourceError('官方商品分類格式改變')
        d[k]=d[k].astype(str)
    return companies,funds

def homepage(client):
    j=js(client,HOME,ttl=21600)
    rows=pd.DataFrame(j.get('ranking',{}).get('market',[]))
    if len(rows)!=20 or not {'DATE','NO','NAME','AMT'}<=set(rows):
        raise SourceError('官網首頁市值榜不是20筆或欄位改變，不補名額。')
    days={normdate(x) for x in rows.DATE}
    if len(days)!=1:raise SourceError('首頁榜單日期不一致')
    day=days.pop()
    rows=rows.rename(columns={'NO':'code','NAME':'name','AMT':'市值億元'})
    rows['code']=rows.code.astype(str);rows['首頁名次']=range(1,21);rows['排名日']=str(day)
    rows=rows[['code','name','首頁名次','市值億元','排名日']]
    return rows,day

def lists(client,companies,funds):
    rows,day=homepage(client)
    etf_ids=set(funds.loc[funds['基金類型'].astype(str).str.contains('交易所交易基金|指數股票型|ETF',regex=True),'基金代號'])
    company_ids=set(companies['公司代號'])
    unknown=set(rows.code)-etf_ids-company_ids
    if unknown:raise SourceError('首頁商品無法分類：'+','.join(sorted(unknown)))
    moved=rows[rows.code.isin(etf_ids)].copy();stocks=rows[rows.code.isin(company_ids)].copy()
    if len(stocks)+len(moved)!=20:raise SourceError('首頁商品分類不唯一')
    d=snapshot(client,'/ETFReport/ETFRank')
    required={'ETFsSecurityCode','ETFsName','ETFsNumberofTradingAccounts'}
    if not required<=set(d):raise SourceError('定期定額ETF排行格式改變')
    d=d.rename(columns={'ETFsSecurityCode':'code','ETFsName':'name','ETFsNumberofTradingAccounts':'定期定額戶數'})
    d['code']=d.code.astype(str);d['定期定額戶數']=d['定期定額戶數'].map(number)
    popular=d.dropna(subset=['定期定額戶數']).sort_values('定期定額戶數',ascending=False).head(15)
    if len(popular)!=15:raise SourceError('定期定額ETF不足15檔')
    popular=popular[['code','name','定期定額戶數']].copy();popular['定期定額名次']=range(1,16)
    popular['來源']='定期定額前15';moved['來源']='首頁市值榜移入'
    etfs=merge_etfs(popular,moved)
    return stocks,etfs,day

def merge_etfs(popular,moved):
    result=[]
    for code,g in pd.concat([popular,moved],ignore_index=True).groupby('code',sort=False):
        row={'code':str(code)}
        for col in g.columns:
            vals=g[col].dropna()
            if len(vals):row[col]=vals.iloc[0]
        row['來源']='＋'.join(dict.fromkeys(g['來源'].dropna()))
        result.append(row)
    return pd.DataFrame(result)

def tables(payload):
    return [t for t in payload.get('tables',[]) if t.get('fields') and t.get('data')]

def parse_market(payload):
    if payload.get('stat')!='OK':return None
    day=normdate(payload['date']);prices=None;indices={}
    for t in tables(payload):
        fields=t['fields']
        if {'證券代號','收盤價','漲跌價差'}<=set(fields):
            raw=pd.DataFrame(t['data'],columns=fields)
            ren={'證券代號':'code','證券名稱':'name','開盤價':'open','最高價':'high','最低價':'low','收盤價':'close','成交股數':'volume','成交金額':'money','漲跌價差':'spread'}
            prices=raw.rename(columns=ren)
            for c in ['open','high','low','close','volume','money','spread']:prices[c]=prices[c].map(number)
            prices['sign']=raw['漲跌(+/-)'].astype(str).map(lambda x:-1 if '-' in re.sub('<[^>]+>','',x) else 1)
            prices['reference']=prices.close-prices.sign*prices.spread
            prices['date']=day;prices['code']=prices.code.astype(str)
            prices=prices[['date','code','name','open','high','low','close','volume','money','reference']]
        if '指數' in fields and '收盤指數' in fields:
            for r in t['data']:
                if r[0]=='發行量加權股價指數':indices['close']=number(r[1])
    if prices is None or 'close' not in indices:raise SourceError('官方每日行情缺少價格或加權指數')
    return prices,dict(date=day,**indices)

def history(client,end,progress=None,refresh_recent=False):
    frames=[];indices=[];cursor=end;checked=0
    # 最多160日，找85個市場交易日。週末略過，國定休市回應快取。
    while len(frames)<85 and checked<160:
        if cursor.weekday()<5:
            ttl=historical_ttl(cursor,refresh_recent)
            payload=js(client,RWD+'/afterTrading/MI_INDEX',{'date':cursor.strftime('%Y%m%d'),'type':'ALLBUT0999','response':'json'},ttl)
            parsed=parse_market(payload)
            if parsed:
                p,idx=parsed
                if idx['date']!=cursor:raise SourceError('歷史行情回傳日期與查詢不符')
                frames.append(p);indices.append(idx)
                if progress:
                    downloads=sum(x['模式']=='新下載' for x in getattr(client,'observed',[]))
                    progress(f'讀取行情：{len(frames)}/85 個交易日；本次已下載{downloads}筆，其餘優先沿用快取')
            elif '沒有符合' not in str(payload.get('stat','')):
                raise SourceError('行情來源回覆異常：'+str(payload.get('stat',''))[:100])
        cursor-=timedelta(days=1);checked+=1
    if len(frames)<83:raise SourceError('歷史市場資料不足83交易日')
    raw=pd.concat(frames,ignore_index=True).sort_values(['code','date'])
    raw=raw.drop_duplicates(['code','date'])
    index=pd.DataFrame(indices).sort_values('date').set_index('date')
    return raw,index

def frame_digest(frame):
    # 對完整資料雜湊，確保少數歷史更正也會使特徵快取失效。
    digest=hashlib.sha256(repr(tuple(frame.columns)).encode())
    digest.update(pd.util.hash_pandas_object(frame,index=True).values.tobytes())
    return digest.hexdigest()

@st.cache_data(show_spinner=False,max_entries=2,hash_funcs={pd.DataFrame:frame_digest})
def comparable(raw):
    out=[]
    for code,g in raw.groupby('code',sort=False):
        g=g.sort_values('date').copy();valid=g.close.notna() & (g.close>0) & g.reference.notna() & (g.reference>0)
        # 遇停牌/無效參考價不填補，不把不連續交易當連續交易。
        if not valid.all():continue
        chain=(g.close/g.reference).cumprod();scale=g.close.iloc[-1]/chain.iloc[-1]
        factor=chain*scale/g.close
        for c in ['open','high','low','close']:g['adj_'+c]=g[c]*factor
        g['ma20']=g.adj_close.rolling(20).mean();g['ma60']=g.adj_close.rolling(60).mean()
        g['ret20']=g.adj_close.pct_change(20);g['ret60']=g.adj_close.pct_change(60)
        prev=g.adj_close.shift()
        tr=pd.concat([g.adj_high-g.adj_low,(g.adj_high-prev).abs(),(g.adj_low-prev).abs()],axis=1).max(axis=1)
        g['atr']=wilder_average(tr,14);g['money20']=g.money.rolling(20).mean();g['money5']=g.money.rolling(5).mean()
        g['change']=g.adj_close.diff();out.append(g)
    if not out:raise SourceError('無法建立可比較價格')
    return pd.concat(out,ignore_index=True)

def parse_t86(j,expected):
    if j.get('stat')!='OK':raise SourceError('法人日報尚未發布或查詢失敗')
    if normdate(j['date'])!=expected:raise SourceError('法人資料日期不符')
    d=pd.DataFrame(j['data'],columns=j['fields'])
    fields={'證券代號':'code','外陸資買賣超股數(不含外資自營商)':'foreign','投信買賣超股數':'trust'}
    if not set(fields)<=set(d):raise SourceError('法人欄位改變')
    d=d.rename(columns=fields)[['code','foreign','trust']]
    d['code']=d.code.astype(str)
    for c in ['foreign','trust']:d[c]=d[c].map(number)
    d['date']=expected;return d

def chips(client,dates,refresh_recent=False):
    frames=[]
    for dt in dates:
        j=js(client,RWD+'/fund/T86',{'date':dt.strftime('%Y%m%d'),'selectType':'ALLBUT0999','response':'json'},historical_ttl(dt,refresh_recent))
        frames.append(parse_t86(j,dt))
    return pd.concat(frames,ignore_index=True)

def fundamentals(client,companies=None,codes=None):
    rev=snapshot(client,'/opendata/t187ap05_L');profit=snapshot(client,'/opendata/t187ap17_L')
    needed=None
    if companies is not None and codes is not None:
        wanted=companies[companies['公司代號'].astype(str).isin(set(map(str,codes)))]
        needed=set(wanted.loc[wanted['產業別'].astype(str).isin(['17','金融保險業']),'公司代號'].astype(str))
    finance=[]
    for suffix in ['fh','basi','ins','bd']:
        if needed is not None and not needed:break
        f=snapshot(client,'/opendata/t187ap06_L_'+suffix)
        eps=[k for k in f if '基本每股盈餘' in k]
        if len(eps)!=1:raise SourceError('金融財報EPS欄位改變：'+suffix)
        f['__financial_eps']=f[eps[0]].map(number)
        finance.append(f)
        if needed is not None:needed-=set(f['公司代號'].astype(str))
    if finance:profit=pd.concat([profit,*finance],ignore_index=True)
    valuation=snapshot(client,'/exchangeReport/BWIBBU_ALL')
    return rev,profit,valuation

def col_contains(frame,needles):
    matches=[c for c in frame.columns if all(n in str(c) for n in needles)]
    if len(matches)!=1:raise SourceError('無法唯一辨識欄位：'+','.join(needles))
    return matches[0]

def stock_factors(code,companies,rev,profit,valuation):
    info=companies[companies['公司代號']==code]
    if len(info)!=1:raise SourceError('目前免費版僅支援上市公司；未支援上櫃/興櫃')
    industry=str(info.iloc[0]['產業別']);peer_ids=set(companies.loc[companies['產業別'].astype(str)==industry,'公司代號'])-{code}
    def getrow(d):
        k='公司代號'
        found=d[d[k].astype(str)==code]
        if len(found)!=1:raise SourceError('該股缺營收或營益資料，無法提供八面向完整分數')
        return found.iloc[0]
    a=getrow(rev);b=getrow(profit)
    yoy=number(a['營業收入-去年同月增減(%)']);cum=number(a['累計營業收入-前期比較增減(%)'])
    # 金融業無一般營業利益率時，採稅後純益率，規則預先固定。
    margin_col='__financial_eps' if industry in {'17','金融保險業'} else col_contains(profit,['營業利益率'])
    margin=number(b[margin_col])
    if any(pd.isna(x) for x in [yoy,cum,margin]):raise SourceError('營運數值無效，不用假分數補齊')
    F=score4([yoy>0,cum>0,yoy>cum,margin>0])
    vc='Code' if 'Code' in valuation else '證券代號'
    pe='PEratio' if 'PEratio' in valuation else col_contains(valuation,['本益比'])
    pb='PBratio' if 'PBratio' in valuation else col_contains(valuation,['股價淨值比'])
    v=valuation.copy();v[vc]=v[vc].astype(str);v[pe]=v[pe].map(number);v[pb]=v[pb].map(number)
    row=v[v[vc]==code]
    if len(row)!=1:raise SourceError('缺官方估值資料')
    peers=v[v[vc].isin(peer_ids)];p=number(row.iloc[0][pe]);q=number(row.iloc[0][pb])
    pp=peers.loc[peers[pe]>0,pe];pq=peers.loc[peers[pb]>0,pb]
    if len(pq)<3:raise SourceError('有效同業淨值估值不足3家，不跨業硬湊')
    use_pb_only=industry=='17' or pd.isna(p) or p<=0 or len(pp)<3
    if use_pb_only:
        if pd.isna(q) or q<=0:raise SourceError('無適用正值估值指標')
        V=score4([q<pq.quantile(z) for z in [.75,.5,.25,.1]])
        vdesc='P/B對同業75/50/25/10百分位；本益比不適用時固定改用淨值比'
    else:
        if pd.isna(q) or q<=0:raise SourceError('股價淨值比無效')
        V=score4([p<pp.median(),p<pp.quantile(.25),q<pq.median(),q<pq.quantile(.25)])
        vdesc='P/E與P/B各比較同業中位數及25百分位'
    meta={'營收年月':str(a.get('資料年月','')),'財報年度':str(b.get('年度','')),'財報季別':str(b.get('季別','')),
          '基本面擷取日':str(today()),'營收出表日期':str(a.get('出表日期','')),'財報出表日期':str(b.get('出表日期','')),'估值資料日':str(row.iloc[0].get('Date','')), '營收年增%':yoy,'累計營收年增%':cum,'獲利檢查數值':margin,'獲利檢查指標':'基本EPS' if margin_col=='__financial_eps' else '營業利益率%','估值規則':vdesc,'同業數':len(peer_ids)}
    return F,V,peer_ids,meta

def temperature(values):
    if len(values)!=3 or any(pd.isna(x) for x in values):return '資料不足'
    a,b,c=values
    if max(values)-min(values)<=2:return '⚪ 持平'
    if b>=a and c>=b:return '🔴 持續升溫'
    if b<=a and c<=b:return '🔵 持續降溫'
    return '🟠 回升' if c>b else '🟣 回落'

def wilder_average(values,period=14):
    """首period筆取SMA作種子，其後使用Wilder遞推；不以首筆直接當種子。"""
    values=pd.Series(values,dtype=float);out=pd.Series(float('nan'),index=values.index)
    if len(values)<period:return out
    seed=values.iloc[:period]
    if seed.isna().any():return out
    previous=float(seed.mean());out.iloc[period-1]=previous
    for i in range(period,len(values)):
        previous=(previous*(period-1)+float(values.iloc[i]))/period
        out.iloc[i]=previous
    return out


def strength_score(parts):
    """M/I/F/V/T/L/H依原權重比例換算0–100；R只屬進場品質，不混入強度。"""
    return round(sum(WEIGHTS[k]*parts[k] for k in WEIGHTS if k!='R')/(100-WEIGHTS['R']),2)


def breakout_observation(hist, market_ret20, market_ret60, peer_ret20, peer_ret60, F):
    a=hist.sort_values('date').reset_index(drop=True)
    x=a.iloc[-1];active=None;last_failure=None
    # 掃描全部已載入且有60日前史的日期；事件不因超過10天而自動消失。
    for i in range(60,len(a)):
        bar=a.iloc[i]
        if active is not None:
            if bar.adj_close<active['level']:
                last_failure={**active,'failed_on':bar.date,'failed_index':i};active=None
            else:continue
        prior=a.iloc[i-60:i];level=float(prior.adj_high.max())
        if bar.adj_close>level and a.iloc[i-1].adj_close<=level:
            base=float(a.iloc[i-20:i].volume.mean());span=float(bar.adj_high-bar.adj_low)
            pre=a.iloc[i-20:i];pre10=pre.tail(10);pre_atr=float(pre.iloc[-1].atr)
            tight=bool(pre_atr>0 and pre10.adj_high.max()-pre10.adj_low.min()<=4*pre_atr)
            contracting=bool(pre.atr.tail(5).mean()<=pre.atr.mean())
            active={'index':i,'date':bar.date,'level':level,'atr':float(bar.atr),
                    'volume':float(bar.volume),'volume_ratio':float(bar.volume/base) if base>0 else 0,
                    'close_location':float((bar.adj_close-bar.adj_low)/span) if span>0 else 0,
                    'consolidation':tight and contracting,'tight_range':tight,'contracting_atr':contracting}
    trend=bool(x.ma20>x.ma60 and x.ma20>a.iloc[-21].ma20 and x.ma60>a.iloc[-21].ma60)
    relative=bool(x.ret20>market_ret20 and x.ret60>market_ret60 and
                  x.ret20>peer_ret20 and x.ret60>peer_ret60)
    data={'active':active,'trend':trend,'relative':relative,'confirmed':False,'strong':False,
          'failed':False,'retest':'尚未觀察','status':'無有效突破事件','reasons':[],
          'F_support':F>=75,'support_count':0,'consolidation':False}
    if active is None:
        if last_failure is not None and len(a)-1-last_failure['failed_index']<=4:
            data.update(failed=True,status='突破失敗警示',event=last_failure,
                        reasons=['於'+str(last_failure['failed_on'])+'收盤跌回鎖定突破基準'])
        return data
    confirmed=len(a)-active['index']>=2
    volume_ok=active['volume_ratio']>=1.2;close_ok=active['close_location']>=.7
    checks={'均線持續上升':trend,'相對大盤與同業強勢':relative,
            '突破量至少前20日均量1.2倍':volume_ok,'突破收盤位置至少70%':close_ok}
    count=sum(checks.values());reasons=[name+'未符合' for name,ok in checks.items() if not ok]
    if not confirmed:reasons.insert(0,'尚未連續兩日收盤守住突破區')
    retest='尚未觀察'
    for i in range(active['index']+1,len(a)):
        bar=a.iloc[i];avg=a.iloc[max(0,i-20):i].volume.mean()
        if (bar.adj_low<=active['level']+.5*active['atr'] and bar.adj_close>=active['level']
                and bar.volume<active['volume'] and bar.volume<avg):
            retest='量縮回測中'
            if i<len(a)-1 and (a.iloc[i+1:].adj_close>bar.adj_close).any():
                retest='量縮回測後回升';break
    data.update(confirmed=confirmed,strong=count>=3,retest=retest,reasons=reasons,
                volume_ok=volume_ok,close_ok=close_ok,support_count=int(count),
                consolidation=active['consolidation'])
    data['status']='突破待確認' if not confirmed else ('突破延續較強' if count>=3 else '突破延續證據較少')
    return data


def classify_setup(hist,observation):
    """先辨識形態，再評估進場；沒有突破不等於回檔。"""
    a=hist.sort_values('date');x=a.iloc[-1];old=a.iloc[-21]
    if observation.get('active'):return '突破型'
    up=bool(x.adj_close>x.ma60 and x.ma20>x.ma60 and x.ma20>old.ma20)
    if not up:return '盤整／弱勢'
    pull=bool(a.adj_high.tail(20).max()-x.adj_close>=.5*x.atr and
              abs(x.adj_close-x.ma20)<=2*x.atr)
    return '回檔型' if pull else '趨勢延續型'


def liquidity_score(hist,observation,setup):
    a=hist.sort_values('date');x=a.iloc[-1];last=a.tail(20)
    common=[x.money20>=1e8,(last.volume>0).all()]
    if setup=='回檔型':
        falls=last.loc[last.change<0,'volume'];ups=last.loc[last.change>0,'volume']
        recent=a.tail(5);down=recent.loc[recent.change<0,'volume']
        path=[len(falls)>0 and len(ups)>0 and falls.mean()<ups.mean(),
              len(down)>0 and down.mean()<a.iloc[-21:-1].volume.mean()]
        label='回檔：跌日量小於漲日量；近5日跌日均量低於此前20日均量'
    else:
        event=observation.get('active')
        up=last.loc[last.change>0,'volume'].mean();down=last.loc[last.change<0,'volume'].mean()
        path=[x.money5>=x.money20,event['volume_ratio']>=1.2 if event else up>down]
        label='突破：5日均額不低於20日、突破量至少1.2倍' if event else '一般：5日均額不低於20日、漲日均量大於跌日'
    return score4(common+path),label


def _risk_distance_points(distance_atr):
    return 100 if distance_atr<=2 else 75 if distance_atr<=3 else 50 if distance_atr<=4 else 0


def risk_assessment(hist, observation):
    a=hist.sort_values('date').reset_index(drop=True);x=a.iloc[-1];prior=a.iloc[:-1]
    scale=float(x.close/x.adj_close);entry=float(x.close);atr=float(x.atr*scale)
    event=observation.get('active');setup=classify_setup(a,observation)
    target=None;rr=None;protected=None;initial_stop=None
    if event:
        stop=(event['level']-.5*event['atr'])*scale
        guards=[stop]
        for i in range(event['index']+1,len(a)):
            w=a.iloc[max(0,i-10):i]
            guards.append((float(w.adj_low.min())-.5*float(a.iloc[i-1].atr))*scale)
        protected=max(guards);initial_stop=stop;stop=protected
    else:
        stop=float(prior.adj_low.tail(20).min()*scale-.5*atr)
        if setup=='回檔型':
            candidate=float(prior.adj_high.tail(60).max()*scale)
            if candidate>entry:target=candidate
    distance=entry-stop;valid=bool(entry>0 and stop>0 and distance>0 and atr>0)
    cost=entry*(2*.001425+.003+.001)
    if target is not None and valid:rr=(target-entry-cost)/(distance+cost)
    extension=abs(float(x.adj_close-x.ma20))*scale/atr if atr>0 else float('inf')
    risk_atr=distance/atr if valid else float('inf')
    distance_points=_risk_distance_points(risk_atr) if valid else 0
    if setup=='回檔型':
        position_points=0 if rr is None else 100 if rr>=3 else 75 if rr>=2 else 50 if rr>=1.5 else 25 if rr>=1 else 0
        position='回檔位置可評估' if rr is not None and rr>=MIN_PULLBACK_RR else '上方空間不足'
        trigger=bool(x.adj_close>prior.iloc[-1].adj_close and x.adj_close>=x.ma20-.5*x.atr)
    elif setup in {'突破型','趨勢延續型'}:
        position_points=100 if extension<=2 else 75 if extension<=3 else 50 if extension<=4 else 0
        position='位置可評估' if extension<=MAX_ENTRY_EXTENSION_ATR else '離均線偏遠'
        trigger=True
    else:position_points=0;position='尚無進場型態';trigger=False
    R=round(.5*distance_points+.5*position_points,2) if valid else 0
    reasons=[]
    if not valid:reasons.append('停損／ATR無效')
    if valid and distance/entry>MAX_STOP_PCT:reasons.append('停損距離超過8%')
    if protected is not None and protected>=entry:reasons.append('現價不高於移動保護價')
    gate=not reasons
    return R,{'評估路徑':setup,'參考停損':round(stop,2),'突破初始停損':round(initial_stop,2) if initial_stop is not None else None,'參考目標':round(target,2) if target is not None else None,
              '扣成本風報比':round(rr,2) if rr is not None else None,'追價過遠':extension>MAX_ENTRY_EXTENSION_ATR,
              'R說明':'進場品質＝停損距離分50%＋位置分50%；不再重複計距突破價條件。',
              '突破風控通過':bool(event and gate),'風控通過':gate,'風控結果':'通過' if gate else '未通過',
              '移動保護參考價':round(protected,2) if protected is not None else None,
              '風控未符合':'；'.join(reasons),'停損距離ATR':round(risk_atr,2) if valid else None,
              '停損距離%':round(distance/entry*100,2) if valid else None,'距MA20_ATR':round(extension,2) if atr>0 else None,
              '進場位置':position,'位置通過':bool(rr is not None and rr>=MIN_PULLBACK_RR) if setup=='回檔型' else bool(setup=='突破型' and extension<=MAX_ENTRY_EXTENSION_ATR),
              '回檔止跌確認':trigger,'風險距離分':distance_points,'位置分':position_points}


def advice(total,parts,observation=None,risk=None):
    observation=observation or {};risk=risk or {};strength=strength_score(parts)
    if observation.get('failed'):
        return '突破失敗，暫不追價','檢查原停損與交易理由；不因警示一律賣出'
    if not risk.get('風控通過',False):return '風控未通過，等待','核對原停損／移動保護價；不以高分抵銷'
    if strength<60:return '不進場','依退出規則評估'
    if strength<70:return '不進場','減碼評估'
    if strength<80:return '觀察','續抱檢查'
    if parts['M']<50 or parts['T']<75:
        return '強度高，但大盤／趨勢門檻未齊','檢查趨勢與持有理由'
    route=risk.get('評估路徑')
    if route not in {'突破型','回檔型'}:
        return ('趨勢延續，等待整理或回檔' if route=='趨勢延續型' else '尚無進場型態，觀察'),'依趨勢與原停損檢查'
    if route=='突破型' and not observation.get('confirmed'):
        return '突破待確認，等待收盤站穩','觀察能否守住突破區'
    if not risk.get('位置通過',False):
        return ('強勢，等待回檔' if risk.get('追價過遠') else '位置未通過，等待'),'續抱檢查原停損與移動保護價'
    if route=='回檔型' and not risk.get('回檔止跌確認',False):
        return '回檔位置可評估，等待止跌','觀察收盤回升與MA20附近承接'
    tier=0 if strength<85 else 1 if strength<90 else 2
    if route=='突破型':
        count=observation.get('support_count',0)
        if count<2:return '突破已確認，延續證據較少，觀察','核對量價與相對強弱'
        if count==2:tier=0
        elif not observation.get('consolidation',False):tier=min(tier,1)
    labels=['試單候選','分批建倉候選','標準建倉候選']
    return route+'／'+labels[tier],'續抱檢查；加碼須另通過位置與風控'


def stock_score(code,dt,prices,index,companies,rev,profit,val,flow):
    hist=prices[(prices.code==code)&(prices.date<=dt)].sort_values('date')
    if len(hist)<81 or hist.date.iloc[-1]!=dt:raise SourceError('該股連續有效比較價不足81交易日，或當日無行情')
    market_dates=set(index.loc[:dt].tail(81).index)
    if not market_dates<=set(hist.date):raise SourceError('該股存在缺價/停牌，不以前日填補')
    x=hist.iloc[-1];old=hist.iloc[-21];idx=index.loc[:dt].copy()
    if len(idx)<81:raise SourceError('指數歷史不足')
    ic=idx.close;im20=ic.rolling(20).mean();im60=ic.rolling(60).mean()
    r20=ic.iloc[-1]/ic.iloc[-21]-1;r60=ic.iloc[-1]/ic.iloc[-61]-1
    valid_ids=set(companies['公司代號'])
    cross=prices[(prices.date==dt)&prices.code.isin(valid_ids)].dropna(subset=['ma60','ret60','ret20'])
    if len(cross)<300:raise SourceError('市場廣度有效樣本不足300家')
    breadth=(cross.adj_close>cross.ma60).mean()
    F,V,peers,meta=stock_factors(code,companies,rev,profit,val)
    peer=cross[cross.code.isin(peers)]
    if len(peer)<3:raise SourceError('有效同業行情不足3家')
    parts={}
    parts['M']=score4([ic.iloc[-1]>im60.iloc[-1],im20.iloc[-1]>im60.iloc[-1],im60.iloc[-1]>im60.iloc[-21],breadth>.5])
    parts['I']=score4([peer.ret20.median()>r20,peer.ret60.median()>r60,(peer.adj_close>peer.ma60).mean()>.5,peer.ret60.median()>0])
    parts['F']=F;parts['V']=V
    parts['T']=score4([x.adj_close>x.ma60,x.ma20>x.ma60,x.ret60>r60,x.ma20>old.ma20])
    last=hist.tail(20)
    dates=list(index.loc[:dt].tail(20).index)
    f=flow[(flow.code==code)&flow.date.isin(dates)].sort_values('date')
    if len(f)!=20 or f[['foreign','trust']].isna().any().any():
        raise SourceError('該股法人近20日資料不齊；不把缺資料當0買賣超')
    net20=f.foreign.sum()+f.trust.sum();net5=f.tail(5).foreign.sum()+f.tail(5).trust.sum()
    denom=last.volume.sum()
    parts['H']=score4([f.foreign.sum()>0,f.trust.sum()>0,net5>0,denom>0 and net20/denom>=.01])
    observation=breakout_observation(hist,r20,r60,float(peer.ret20.median()),float(peer.ret60.median()),F)
    parts['R'],risk=risk_assessment(hist,observation)
    parts['L'],liquidity_rule=liquidity_score(hist,observation,risk['評估路徑'])
    total=round(sum(WEIGHTS[k]*parts[k]/100 for k in WEIGHTS),2)
    buy,hold=advice(total,parts,observation,risk)
    event=observation.get('active') or observation.get('event')
    scale=float(x.close/x.adj_close)
    meta.update({'市場廣度樣本':len(cross),'產業樣本':len(peer),'收盤':float(x.close),**risk,
                 '模型版本':MODEL_VERSION,'股票強度':strength_score(parts),'量能規則':liquidity_rule,
                 '突破延續符合數':observation['support_count'],'突破前整理':observation['consolidation'],
                 '創高觀察':observation['status'],'均線延續':observation['trend'],'相對強勢':observation['relative'],
                 '營運支持':observation['F_support'],'回測承接':observation['retest'],
                 '突破站穩':observation['confirmed'],'突破日':str(event['date']) if event else None,
                 '突破基準價':round(event['level']*scale,2) if event else None,
                 '突破日量比':round(event['volume_ratio'],2) if event else None,
                 '未符合條件':'；'.join(observation['reasons']),
                 '未持有':buy,'已持有':hold})
    return total,parts,meta

def etf_score(code,dt,prices):
    a=prices[(prices.code==code)&(prices.date<=dt)].sort_values('date')
    if len(a)<81 or a.date.iloc[-1]!=dt:raise SourceError('ETF歷史有效比較價不足81日或無當日價格')
    x=a.iloc[-1];old=a.iloc[-21]
    trend=score4([x.adj_close>x.ma20,x.adj_close>x.ma60,x.ma20>x.ma60,x.ma60>old.ma60])
    momentum=score4([x.ret20>0,x.ret60>0,x.adj_close>a.adj_close.tail(20).median(),x.adj_close>a.adj_close.tail(60).median()])
    last=a.tail(20);up=last.loc[last.change>0,'volume'].mean();down=last.loc[last.change<0,'volume'].mean()
    liquidity=score4([x.money20>=1e8,x.money20>=5e8,x.money5>=x.money20,up>down])
    atr=x.atr/x.adj_close
    stability=score4([atr<=.04,atr<=.03,atr<=.02,atr<=.01])
    parts={'趨勢':trend,'動能':momentum,'量能':liquidity,'價格穩定':stability}
    total=round(.4*trend+.25*momentum+.2*liquidity+.15*stability,2)
    technical=round((.4*trend+.25*momentum+.2*liquidity)/.85,2)
    return total,parts,{'模型版本':MODEL_VERSION,'ETF技術強度':technical,'波動ATR%':round(float(atr)*100,2),
                       '未持有':'ETF技術強度與波動分開觀察','已持有':'另查淨值折溢價與產品風險'}

def batch(rows,dates,prices,index,context,is_etf=False):
    companies,rev,profit,val,flow=context
    results=[];detail=[]
    for item in rows.to_dict('records'):
        code=str(item['code']);record=dict(item);values=[];strengths=[]
        try:
            for dt in dates:
                total,parts,meta=etf_score(code,dt,prices) if is_etf else stock_score(code,dt,prices,index,companies,rev,profit,val,flow)
                values.append(total);strengths.append(meta['ETF技術強度' if is_etf else '股票強度'])
                detail.append({'代碼':code,'日期':str(dt),'總分':total,**{NAMES.get(k,k):v for k,v in parts.items()},**meta})
            record.update({'強度三日圖':strengths,'近三日強度平均':round(sum(strengths)/3,2),
                           '強度狀態':temperature(strengths),'強度三日變化':round(strengths[-1]-strengths[0],2),
                           '強度序列':' → '.join(str(x) for x in strengths),'三日圖':values,'當日分數':values[-1],'近三日平均':round(sum(values)/3,2),'三日變化':round(values[-1]-values[0],2),
                           '狀態':temperature(values),'未持有':meta['未持有'],'已持有':meta['已持有'],'評分日':str(dates[-1]),
                           '日期序列':' → '.join(str(x) for x in dates),'分數序列':' → '.join(str(x) for x in values)})
            if not is_etf:
                for k in ['模型版本','股票強度','進場位置','風控結果','突破延續符合數','突破前整理','量能規則','創高觀察','評估路徑','突破基準價','突破站穩','回測承接','參考停損','移動保護參考價','未符合條件','風控未符合']:
                    record[k]=meta.get(k)
            else:
                for k in ['模型版本','ETF技術強度','波動ATR%']:record[k]=meta.get(k)
        except (SourceError,KeyError,ValueError) as exc:
            record.update({'狀態':'無法評分','錯誤':str(exc)})
        results.append(record)
    return pd.DataFrame(results),pd.DataFrame(detail)

def display_results(table,detail,key):
    if table.empty:st.info('本區沒有標的');return
    priority=['code','name','股票強度','ETF技術強度','強度三日圖','近三日強度平均','強度狀態','進場位置','風控結果','創高觀察','評估路徑','當日分數','三日圖','近三日平均','狀態','未持有','已持有']
    table=table[[k for k in priority if k in table]+[k for k in table if k not in priority]]
    st.dataframe(table,use_container_width=True,hide_index=True,column_config={
        '強度三日圖':st.column_config.LineChartColumn('近三日強度升降溫',y_min=0,y_max=100,width='medium'),
        '三日圖':st.column_config.LineChartColumn('近三日綜合分',y_min=0,y_max=100,width='medium'),
        '當日分數':st.column_config.NumberColumn('八面向綜合分／ETF綜合分',format='%.2f'),
        '股票強度':st.column_config.NumberColumn('股票強度（進場分級依此）',format='%.2f'),
        'ETF技術強度':st.column_config.NumberColumn(format='%.2f'),
        '近三日平均':st.column_config.NumberColumn('近三日綜合分平均',format='%.2f'),
        '近三日強度平均':st.column_config.NumberColumn(format='%.2f'),
        '狀態':st.column_config.TextColumn('綜合分狀態')})
    export=table.drop(columns=['三日圖','強度三日圖'],errors='ignore')
    st.download_button('下載本區CSV',export.to_csv(index=False).encode('utf-8-sig'),file_name=key+'.csv',key=key+'_csv')
    if not detail.empty:
        with st.expander('八面向／ETF分項、價格與資料日期'):
            code=st.selectbox('選擇代碼',list(dict.fromkeys(detail['代碼'])),key=key+'_select')
            chosen=detail[detail['代碼']==code]
            if '創高觀察' in chosen:
                latest=chosen.iloc[-1]
                st.write('最新創高觀察：'+str(latest['創高觀察'])+'｜'+str(latest['評估路徑']))
                st.caption('延續條件未符合：'+(str(latest.get('未符合條件','')) or '無')+'；風控未符合：'+(str(latest.get('風控未符合','')) or '無'))
            strength_column='股票強度' if '股票強度' in chosen else 'ETF技術強度'
            chart=alt.Chart(chosen.assign(日期=pd.to_datetime(chosen['日期']))).mark_line(point=True).encode(
                x=alt.X('日期:T',title='評分日期'),y=alt.Y(strength_column+':Q',scale=alt.Scale(domain=[0,100])),tooltip=['日期:T',strength_column+':Q','總分:Q'])
            st.altair_chart(chart,use_container_width=True)
            st.dataframe(chosen,hide_index=True,use_container_width=True)
            st.download_button('下載分項CSV',detail.to_csv(index=False).encode('utf-8-sig'),file_name=key+'_detail.csv',key=key+'_detail')

# 盤中只查單檔；MIS為公開網站報價回應，非具可用性承諾的OpenAPI。
MIS='https://mis.twse.com.tw/stock/api/getStockInfo.jsp'

def parse_intraday_quote(payload,code,now=None):
    now=now or datetime.now(ZoneInfo('Asia/Taipei'))
    now=now.astimezone(ZoneInfo('Asia/Taipei'))
    rows=payload.get('msgArray') if isinstance(payload,dict) else None
    if not isinstance(rows,list):raise SourceError('盤中來源未回傳行情陣列；不以收盤價或買賣掛單價替代。')
    matches=[r for r in rows if str(r.get('c',''))==str(code) and r.get('ex')=='tse']
    if len(matches)!=1:raise SourceError('沒有唯一的上市盤中報價；可能休市、停牌或來源拒絕，未產生預估分。')
    r=matches[0]
    try:stamp=datetime.strptime(str(r['d'])+' '+str(r['t']),'%Y%m%d %H:%M:%S').replace(tzinfo=ZoneInfo('Asia/Taipei'))
    except (KeyError,ValueError):raise SourceError('盤中報價時間缺少／格式改變；停止評分。') from None
    if stamp.date()!=now.date():raise SourceError('盤中來源不是今天的資料；不把舊報價當即時行情。')
    minute=now.hour*60+now.minute
    if now.weekday()>=5 or not 540<=minute<810:
        raise SourceError('目前非一般盤中時段09:00–13:30；請使用收盤模式。休市與特殊交易另以來源為準。')
    age=(now-stamp).total_seconds()
    # 收盤前五分鐘最後成交可能停在13:25；不使用試撮價替代實際成交。
    auction=minute>=805
    max_age=420 if auction else 180
    if age < -60 or age>max_age:
        raise SourceError(f'報價時間落後約{max(0,age)/60:.1f}分鐘或超前，超過本模式接受範圍；未產生預估分。')
    values={k:number(r.get(k)) for k in ['z','o','h','l','y','v']}
    if any(pd.isna(v) or not math.isfinite(v) or v<=0 for v in values.values()):
        raise SourceError('盤中成交價／OHLC／昨參考價／累計量無效；不改拿買賣掛單或昨收充數。')
    if not (values['l']<=min(values['o'],values['z'])<=max(values['o'],values['z'])<=values['h']):
        raise SourceError('盤中高低價與成交價不一致，暫停評分。')
    # MIS一般整股v為累計張數；本程式上市普通股／ETF每張1000股，與日報股數統一。
    return {'code':str(code),'time':stamp,'fetched_at':now,'age_seconds':max(0,age),
            'price':values['z'],'open':values['o'],'high':values['h'],'low':values['l'],
            'reference':values['y'],'volume':values['v']*1000,
            'phase':'收盤集合競價，僅參考最後成交' if auction else '開盤前30分鐘，訊號不穩定' if minute<570 else '一般盤中',
            'auction':auction,'early':minute<570}


def load_intraday_quote(client,code,now=None):
    if not re.fullmatch(r'[0-9A-Z]{4,8}',str(code)):raise SourceError('盤中證券代碼格式不符。')
    if client.offline:raise SourceError('盤中模式不能使用離線快取；可改用收盤模式。')
    requested_now=now
    now=now or datetime.now(ZoneInfo('Asia/Taipei'))
    minute=now.hour*60+now.minute
    if now.weekday()>=5 or not 540<=minute<810:raise SourceError('目前非一般盤中時段09:00–13:30，未向盤中來源發出請求。')
    payload=js(client,MIS,{'ex_ch':'tse_'+str(code)+'.tw','json':'1','delay':'0'},ttl=60)
    return parse_intraday_quote(payload,code,now=requested_now or datetime.now(ZoneInfo('Asia/Taipei')))


def intraday_bar(hist,quote):
    """只暫估一根日K；ATR固定使用上個完整日，避免半日波幅縮小風險尺度。"""
    a=hist.sort_values('date').reset_index(drop=True).copy();prev=a.iloc[-1]
    if quote['time'].date()<=prev.date:raise SourceError('盤中日期未晚於收盤基準；避免同一天重複加入日K。')
    if abs(quote['reference']/float(prev.close)-1)>.001:
        raise SourceError('盤中昨參考價與收盤基準不符，可能缺少交易日或遇除權息／公司行動；先補齊比較價，暫不預估。')
    factor=float(prev.adj_close)/quote['reference']
    row=prev.to_dict();row.update(date=quote['time'].date(),close=quote['price'],open=quote['open'],
                                 high=quote['high'],low=quote['low'],reference=quote['reference'],volume=quote['volume'])
    for col,key in [('close','price'),('open','open'),('high','high'),('low','low')]:row['adj_'+col]=quote[key]*factor
    for n in [20,60]:
        row['ma'+str(n)]=(a['adj_close'].tail(n-1).sum()+row['adj_close'])/n
        row['ret'+str(n)]=row['adj_close']/a.iloc[-n].adj_close-1
    row['atr']=float(prev.atr) # 固定完整日ATR；不是當天完整ATR
    row['money']=float('nan');row['money5']=float('nan');row['money20']=float('nan')
    row['change']=row['adj_close']-prev.adj_close
    return pd.concat([a,pd.DataFrame([row])],ignore_index=True)


def intraday_liquidity_bounds(hist,quote,setup,observation,is_etf=False):
    """固定當前價格，只對尚未確定的當日量能條件給可變範圍；不是收盤信賴區間。"""
    a=hist.sort_values('date');past19=float(a.money.tail(19).sum());past4=float(a.money.tail(4).sum())
    # 成交金額未直接取得，以日內最低價×已成交股數作已發生金額下界；不是精確成交額。
    amount_floor=quote['low']*quote['volume']
    min20=(past19+amount_floor)/20
    min5_ge20=3*amount_floor>=past19-4*past4
    def uncertain(proven):return (25,25) if proven else (0,25)
    bounds=[uncertain(min20>=1e8)]
    if is_etf:
        bounds+=[uncertain(min20>=5e8),uncertain(min5_ge20),(0,25)]
    else:
        traded=bool((a.volume.tail(19)>0).all() and quote['volume']>0)
        bounds.append((25,25) if traded else (0,0))
        if setup=='突破型':
            bounds.append(uncertain(min5_ge20))
            event=observation.get('active')
            if event and event['date']<quote['time'].date():
                points=25 if event['volume_ratio']>=1.2 else 0;bounds.append((points,points))
            else:bounds.append(uncertain(quote['volume']>=1.2*a.volume.tail(20).mean()))
        elif setup=='回檔型':bounds.extend([(0,25),(0,25)])
        else:bounds.extend([uncertain(min5_ge20),(0,25)])
    return sum(x[0] for x in bounds),sum(x[1] for x in bounds)


def intraday_estimate(code,dt,prices,index,context,quote,is_etf=False):
    hist=prices[(prices.code==code)&(prices.date<=dt)].sort_values('date').reset_index(drop=True)
    if len(hist)<81 or hist.date.iloc[-1]!=dt:raise SourceError('盤中模式也需要完整的前一交易日基準與至少81日行情。')
    live=intraday_bar(hist,quote);x=live.iloc[-1];old=live.iloc[-21]
    ratio=quote['volume']/hist.volume.tail(20).mean()
    common={'代碼':code,'模式':'盤中暫估，非收盤分','收盤基準日':str(dt),
            '報價時間':quote['time'].isoformat(),'查詢時間':quote['fetched_at'].isoformat(),
            '報價落後秒數':round(quote['age_seconds'],1),'時段':quote['phase'],
            '最後成交價':quote['price'],'累計成交股數':int(quote['volume']),
            '累計量／前20日全天均量':round(float(ratio),3),
            '量能說明':'此比例僅代表已成交量；沒有同時段歷史量，不線性推估全天量，也不以未完成量當作量縮。',
            'ATR使用日':str(dt),'估算MA20':round(float(x.ma20*x.close/x.adj_close),2),
            '估算MA60':round(float(x.ma60*x.close/x.adj_close),2),
            '公司行動限制':'只檢查昨參考價一致性；不是完整公司行動驗證。'}
    if is_etf:
        _,base,meta=etf_score(code,dt,prices)
        trend=score4([x.adj_close>x.ma20,x.adj_close>x.ma60,x.ma20>x.ma60,x.ma60>old.ma60])
        momentum=score4([x.ret20>0,x.ret60>0,x.adj_close>live.adj_close.tail(20).median(),x.adj_close>live.adj_close.tail(60).median()])
        lo,hi=intraday_liquidity_bounds(hist,quote,'ETF',{},True)
        low=round((.4*trend+.25*momentum+.2*lo)/.85,2);high=round((.4*trend+.25*momentum+.2*hi)/.85,2)
        detail=[{'面向':'趨勢','分數／區間':str(trend),'資料狀態':'盤中價格暫估'},
                {'面向':'動能','分數／區間':str(momentum),'資料狀態':'盤中價格暫估'},
                {'面向':'量能','分數／區間':f'{lo}–{hi}','資料狀態':'未定條件列範圍，不假設全天成交量'},
                {'面向':'價格穩定','分數／區間':str(base['價格穩定']),'資料狀態':'維持收盤基準；不混入ETF技術強度'}]
        common.update({'強度下緣':low,'強度上緣':high,'觀察結論':'ETF盤中技術觀察；不套用個股進場門檻',
                       '前收盤強度':meta['ETF技術強度'],'進場位置':'另查淨值折溢價與產品風險','風控結果':'本模式未評估ETF產品風險'})
    else:
        companies,rev,profit,val,flow=context
        _,parts,meta=stock_score(code,dt,prices,index,companies,rev,profit,val,flow)
        peers=set(companies.loc[companies['產業別'].astype(str)==str(companies.loc[companies['公司代號']==code,'產業別'].iloc[0]),'公司代號'])-{code}
        cross=prices[(prices.date==dt)&prices.code.isin(peers)]
        ic=index.loc[:dt].close;r20=ic.iloc[-1]/ic.iloc[-21]-1;r60=ic.iloc[-1]/ic.iloc[-61]-1
        obs=breakout_observation(hist,r20,r60,float(cross.ret20.median()),float(cross.ret60.median()),parts['F'])
        already=obs.get('active');level=float(hist.adj_high.tail(60).max())
        triggered=already is None and x.adj_close>level
        if triggered:
            span=x.adj_high-x.adj_low
            obs.update(active={'index':len(hist),'date':quote['time'].date(),'level':level,'atr':float(hist.iloc[-1].atr),
                               'volume':quote['volume'],'volume_ratio':float(ratio),
                               'close_location':float((x.adj_close-x.adj_low)/span) if span>0 else 0,
                               'consolidation':False},confirmed=False,strong=False)
        # 三項價格趨勢重算；相對大盤條件沿用同日收盤對照，不把今日個股與昨日指數硬比。
        parts['T']=score4([x.adj_close>x.ma60,x.ma20>x.ma60,hist.iloc[-1].ret60>r60,x.ma20>old.ma20])
        parts['R'],risk=risk_assessment(live,obs)
        lo,hi=intraday_liquidity_bounds(hist,quote,risk['評估路徑'],obs)
        fixed=sum(WEIGHTS[k]*parts[k] for k in WEIGHTS if k not in {'R','L'})
        low=round((fixed+WEIGHTS['L']*lo)/90,2);high=round((fixed+WEIGHTS['L']*hi)/90,2)
        under=bool(already and x.adj_close<already['level'])
        conclusion='盤中觀察；需待收盤確認'
        if quote['early']:conclusion='開盤前30分鐘，只觀察，不做進場確認'
        elif quote['auction']:conclusion='收盤集合競價中，等待實際收盤成交'
        elif obs.get('failed'):conclusion='前收盤有突破失敗警示，盤中僅觀察'
        elif under:conclusion='盤中跌回突破基準，核對原停損；尚非收盤失敗確認'
        elif not risk['風控通過']:conclusion='盤中風控未通過，等待'
        elif high<80:conclusion='盤中強度情境仍低於80，觀察'
        elif low<80:conclusion='量能情境跨越80分，尚不足以確認'
        elif parts['M']<50 or parts['T']<75:conclusion='前收盤大盤／盤中趨勢條件未齊'
        elif not risk['位置通過']:conclusion='盤中強度較高，但進場位置待改善'
        elif risk['評估路徑'] not in {'回檔型','突破型'}:conclusion='趨勢觀察，尚無可辨識的進場型態'
        else:conclusion='盤中候選觀察，等待收盤與正式資料確認'
        detail=[]
        for k in WEIGHTS:
            status={'M':'前收盤大盤與廣度','I':'前收盤同業行情','F':'本次最新營收／財報快照',
                    'V':'最新官方估值快照，未按盤中價格重算','T':'3項盤中暫估；相對大盤維持前收盤對照',
                    'L':'量能尚未完成；分數區間不是收盤預測','H':'前收盤法人；不推測今日法人',
                    'R':'當下位置＋前收盤ATR；不是收盤買點確認'}[k]
            detail.append({'面向':NAMES[k],'分數／區間':f'{lo}–{hi}' if k=='L' else str(parts[k]),'資料狀態':status})
        common.update({'強度下緣':low,'強度上緣':high,'前收盤強度':meta['股票強度'],
                       '觀察結論':conclusion,'進場位置':risk['進場位置'],'風控結果':risk['風控結果'],
                       '參考停損':risk['參考停損'],'風控未符合':risk['風控未符合'],
                       '營收年月':meta.get('營收年月',''),'財報年度':meta.get('財報年度',''),'財報季別':meta.get('財報季別',''),
                       '估值資料日':meta.get('估值資料日',''),'法人／大盤／同業基準日':str(dt),
                       '盤中型態':risk['評估路徑'],'今日突破':'盤中觸發，當日未收盤' if triggered else '沿用既有事件／尚未觸發',
                       '前收盤已確認突破':bool(already and obs['confirmed'])})
    common['區間定義']='固定此刻價格與已知基準，只讓未定量能條件變動；不是未來收盤分數信賴區間。'
    return common,pd.DataFrame(detail)


def display_intraday(result,detail):
    st.subheader('第一區｜盤中預估（獨立於三日收盤圖）')
    st.caption('這是上次按下盤中預估時的快照，不會自動更新；報價落後秒數以當次查詢時計。請核對下方報價時間。')
    stamp=datetime.fromisoformat(result['報價時間'])
    snapshot_age=(datetime.now(ZoneInfo('Asia/Taipei'))-stamp).total_seconds()
    if snapshot_age>180:
        st.warning(f'這份報價距現在約{snapshot_age/60:.1f}分鐘；可能為舊快照或集合競價期間最後成交，請勿當作目前即時狀態。')
    st.warning('非即時交易等級資訊、非收盤確認。大盤／同業／法人採前收盤，最新基本面與估值另有資料日期；不把混合時間的估分稱為全市場即時評分。')
    st.write('**'+result['觀察結論']+'**')
    a,b,c=st.columns(3)
    a.metric('盤中強度：量能情境區間',f"{result['強度下緣']:.2f}–{result['強度上緣']:.2f}")
    b.metric('前收盤強度',f"{result['前收盤強度']:.2f}")
    c.metric('最後成交價',str(result['最後成交價']))
    st.caption('報價時間：'+result['報價時間']+'｜'+result['時段']+'｜收盤基準：'+result['收盤基準日'])
    st.info(result['區間定義'])
    st.caption(result['量能說明'])
    st.dataframe(detail,hide_index=True,use_container_width=True)
    with st.expander('盤中價位、量能、資料時間與限制'):
        st.dataframe(pd.DataFrame([result]),hide_index=True,use_container_width=True)
    st.download_button('下載盤中預估CSV',pd.DataFrame([result]).to_csv(index=False).encode('utf-8-sig'),file_name='intraday_estimate.csv',key='intraday_csv')


def main():
    st.set_page_config(page_title='台股波段｜公開免費版',layout='wide')
    if st.session_state.get('model_version')!=MODEL_VERSION:
        st.session_state.pop('outputs',None);st.session_state.pop('intraday_output',None);st.session_state['model_version']=MODEL_VERSION
    st.title('台股波段｜公開免費官方資料版')
    st.caption('V5｜股票強度、進場位置、風控分開。八面向權重保留；進場分級改依不含R的股票強度。未回測，分數不代表勝率。')
    st.info('先看股票強度，再看型態、位置與風控。F與R不再有75分否決門檻；突破量價證據用於分級。綜合分與股票強度不同，新舊分數不可直接比較。')
    st.caption('T只評趨勢；回檔量縮與突破放量分開計分。ATR改用Wilder14。股價過熱不直接視為賣出；實際持倉須核對原停損。')
    st.warning('三日個股分數為「以最新公開基本面與估值橫截面重估」：不是當時已知資訊的歷史回測。比較價依官方收盤價及參考價重建，不是供應商總報酬還原價。')
    with st.sidebar:
        st.header('查詢設定')
        cutoff=st.date_input('資料截止日',value=today(),max_value=today())
        offline=st.checkbox('只用本機快取',value=False)
        refresh_recent=st.checkbox('複查最近7日官方更正（會重新下載）',value=False)
        st.caption('首次建立85日資料仍較久；已有快取時，只下載缺少日期。支援上市股票與ETF。')
        st.caption('所有請求至少間隔15秒、每日最多200次；遇限制立即停止。不能保證IP不被來源限制。')
        st.caption('資料來源：證交所官方OpenAPI與官網公開JSON；原始資料皆會快取，不需手動證據CSV。')
        st.link_button('官方OpenAPI文件','https://openapi.twse.com.tw/')
    c=Client(offline)
    with st.sidebar:
        if c.seed_note:st.caption(c.seed_note)
        with st.expander('雲端快取備份／還原'):
            st.caption('雲端可能遺失快取。完成查詢後匯出備份；可還原，或把data_cache.zip上傳GitHub與app.py同層，重啟後自動載入。只備份公開資料，不改限流設定。')
            incoming=st.file_uploader('選擇本程式匯出的data_cache.zip',type=['zip'])
            if st.button('還原備份'):
                try:
                    if incoming is None:raise SourceError('請先選擇備份檔')
                    n=restore_cache(incoming.getvalue());comparable.clear()
                    st.success(f'已合併{n}筆較新快取；請重新查詢')
                except SourceError as exc:st.error(str(exc))
            if st.button('準備下載快取備份'):
                try:st.session_state['cache_backup']=export_cache()
                except SourceError as exc:st.error(str(exc))
            if 'cache_backup' in st.session_state:
                st.download_button('下載data_cache.zip',st.session_state['cache_backup'],file_name='data_cache.zip',mime='application/zip')
    st.subheader('第一區｜股票中文名稱／代碼')
    query=st.text_input('上市股票完整簡稱或代碼，例如台積電、2330')
    one=st.button('計算這一檔（收盤）')
    intraday_one=st.button('盤中預估這一檔（手動刷新，報價快取60秒）')
    st.caption('盤中預估僅第一區單檔；第二、三區維持收盤評分。不自動輪詢，未接可靠同時段歷史量之前，不以線性放大量當作全天量。')
    single=one or intraday_one
    st.subheader('第二區｜首頁市值榜20筆中的非ETF')
    st.caption('直接取首頁原榜，保留名次與日期；ETF移到第三區，不補名額。')
    both=st.button('更新第二區與第三區')
    st.subheader('第三區｜定期定額前15 ETF＋首頁榜移入ETF')
    st.caption('按代碼合併去重。保留ETF綜合分，另列排除穩定分的ETF技術強度與ATR波動%；不同產品風險不可只按綜合分比較。')
    only_etf=st.button('僅更新第三區')
    if single or both or only_etf:
        notice=st.empty()
        try:
            c.progress=notice.info
            if intraday_one:
                st.session_state.pop('intraday_output',None)
                live_now=datetime.now(ZoneInfo('Asia/Taipei'))
                if cutoff!=today():raise SourceError('盤中預估請將截止日設為今天。')
                if offline:raise SourceError('盤中預估不能使用離線模式。')
                if live_now.weekday()>=5 or not 540<=live_now.hour*60+live_now.minute<810:
                    raise SourceError('目前非一般盤中時段09:00–13:30，請使用收盤模式。')
            with st.spinner('讀取公開資料與快取；首次建立歷史較久，可保留視窗等待。'):
                companies,funds=registry(c,include_funds=not single,refresh=refresh_recent)
                if single:
                    q=query.strip()
                    a=companies[(companies['公司代號']==q)|(companies['公司簡稱']==q)]
                    if len(a)==1:
                        row=a.iloc[0];selected=pd.DataFrame([{'code':str(row['公司代號']),'name':row['公司簡稱']}]);is_etf=False
                    else:
                        funds=snapshot(c,'/opendata/t187ap47_L',ttl=0 if refresh_recent else 7*86400)
                        funds['基金代號']=funds['基金代號'].astype(str)
                        b=funds[(funds['基金代號']==q)|(funds['基金簡稱']==q)]
                        if len(b)!=1:raise SourceError('查無唯一上市名稱／代碼，目前未支援上櫃與興櫃')
                        row=b.iloc[0];selected=pd.DataFrame([{'code':str(row['基金代號']),'name':row['基金簡稱']}]);is_etf=True
                    if intraday_one:rankday=today()-timedelta(days=1)
                    else:_,rankday=homepage(c)
                else:
                    stocks,etfs,rankday=lists(c,companies,funds)
                if cutoff<rankday and (both or only_etf):raise SourceError('首頁榜單僅最新快照，請選最新截止日，不能當歷史排名')
                now=datetime.now(ZoneInfo('Asia/Taipei'));end=min(cutoff,rankday)
                if end==today() and now.hour<21:end-=timedelta(days=1)
                raw,index=history(c,end,notice.info,refresh_recent=refresh_recent);prices=comparable(raw)
                dates=list(index.index[-3:])
                need_stock=both or (single and not is_etf)
                if need_stock:
                    notice.info('補齊營收、營益、估值與法人資料（已下載日期直接沿用）…')
                    codes=list(stocks.code) if both else list(selected.code)
                    rev,profit,val=fundamentals(c,companies,codes)
                    flow=chips(c,list(index.index[-22:]),refresh_recent=refresh_recent)
                else:rev=profit=val=flow=pd.DataFrame()
                context=(companies,rev,profit,val,flow)
                if 'outputs' not in st.session_state:st.session_state.outputs={}
                if intraday_one:
                    quote=load_intraday_quote(c,str(selected.iloc[0].code))
                    st.session_state['intraday_output']=intraday_estimate(str(selected.iloc[0].code),dates[-1],prices,index,context,quote,is_etf)
                if one:st.session_state.outputs['第一區']=batch(selected,dates,prices,index,context,is_etf)
                if both:st.session_state.outputs['第二區']=batch(stocks,dates,prices,index,context)
                if both or only_etf:st.session_state.outputs['第三區']=batch(etfs,dates,prices,index,context,True)
                st.session_state['sources']=c.observed
                downloaded=sum(x['模式']=='新下載' for x in c.observed)
                reused=len(c.observed)-downloaded
                if downloaded:st.session_state.pop('cache_backup',None)
                st.session_state['speed_note']=f'本次成功下載{downloaded}次、失敗嘗試{len(c.failures)}次、快取命中{reused}次。歷史資料未強制複查時沿用原值；官方更正可勾選複查。'
                st.session_state['data_note']=f'行情與籌碼基準日：{dates[-1]}；三日收盤：'+ '、'.join(map(str,dates))+(f'；首頁排名日：{rankday}' if not intraday_one else '；盤中預估與收盤結果分開顯示')+'；基本面為本次可取得最新快照。'
            notice.success('計算完成；缺資料的標的會明確報錯，不自行填分。')
        except (SourceError,ValueError,KeyError,requests.RequestException) as exc:
            notice.error(str(exc))
        finally:
            st.session_state['network_errors']=c.failures
            st.session_state['sources']=c.observed
    if 'intraday_output' in st.session_state:
        display_intraday(*st.session_state['intraday_output'])
    if st.session_state.get('network_errors'):
        with st.expander('本次暫時連線失敗紀錄（可能已重試成功）'):
            st.dataframe(pd.DataFrame(st.session_state['network_errors']),hide_index=True)
    if 'outputs' in st.session_state:
        st.info(st.session_state.get('data_note',''))
        st.caption(st.session_state.get('speed_note',''))
        with st.expander('來源與快取擷取時間'):
            st.dataframe(pd.DataFrame(st.session_state.get('sources',[])),hide_index=True)
        st.caption('強度三日圖為主要觀察；綜合分另列，兩者均0–100。🔴升溫　🔵降溫　⚪持平　🟠回升　🟣回落；持平＝三日最高最低差≤2分。所有圖固定0–100分。')
        for key,(table,detail) in st.session_state.outputs.items():
            st.subheader(key+'結果（修改設定後須重新計算）')
            display_results(table,detail,key)

if __name__=='__main__':main()
