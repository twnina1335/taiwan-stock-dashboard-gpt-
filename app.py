"""公開免費官方資料版：python -m streamlit run app.py。規格見README。"""
from __future__ import annotations
import hashlib, io, json, math, re, sqlite3, time, zipfile
from datetime import datetime, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo
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
MODEL_VERSION='V4-Strength-Position-Risk'
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
        self.observed=[]
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
                day=today().isoformat(); count=int(read('count:'+day,0))
                if count >= 200: raise Halt('本機今日200次請求預算用完，請明日再試。')
                time.sleep(max(0,15-(time.time()-float(read('last',0)))))
                headers={'User-Agent':'TaiwanWaveDashboard/1.0 (personal research; cached; rate-limited)'}
                write('last',time.time());write('count:'+day,count+1);db.commit()
                try:
                    r=requests.request(method,url,params=params if method=='GET' else None,
                                       data=params if method=='POST' else None,headers=headers,timeout=(10,30))
                except requests.RequestException:
                    raise SourceError('網路連線失敗；不自動重試，稍後手動再試。') from None
                if r.status_code in (401,402,403,429):
                    write('blocked_until',time.time()+86400);db.commit()
                    raise Halt(f'來源回覆HTTP {r.status_code}；全體請求停止24小時，請稍後再試。')
                if r.status_code >= 500:
                    write('blocked_until',time.time()+900);db.commit()
                    raise Halt('來源伺服器異常；停止請求15分鐘。')
                if not r.ok: raise SourceError(f'來源回覆HTTP {r.status_code}，不重試。')
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

def main():
    st.set_page_config(page_title='台股波段｜公開免費版',layout='wide')
    if st.session_state.get('model_version')!=MODEL_VERSION:
        st.session_state.pop('outputs',None);st.session_state['model_version']=MODEL_VERSION
    st.title('台股波段｜公開免費官方資料版')
    st.caption('V4｜股票強度、進場位置、風控分開。八面向權重保留；進場分級改依不含R的股票強度。未回測，分數不代表勝率。')
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
    one=st.button('計算這一檔')
    st.subheader('第二區｜首頁市值榜20筆中的非ETF')
    st.caption('直接取首頁原榜，保留名次與日期；ETF移到第三區，不補名額。')
    both=st.button('更新第二區與第三區')
    st.subheader('第三區｜定期定額前15 ETF＋首頁榜移入ETF')
    st.caption('按代碼合併去重。保留ETF綜合分，另列排除穩定分的ETF技術強度與ATR波動%；不同產品風險不可只按綜合分比較。')
    only_etf=st.button('僅更新第三區')
    if one or both or only_etf:
        notice=st.empty()
        try:
            with st.spinner('讀取公開資料與快取；首次建立歷史較久，可保留視窗等待。'):
                companies,funds=registry(c,include_funds=not one,refresh=refresh_recent)
                if one:
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
                    _,rankday=homepage(c)
                else:
                    stocks,etfs,rankday=lists(c,companies,funds)
                if cutoff<rankday and (both or only_etf):raise SourceError('首頁榜單僅最新快照，請選最新截止日，不能當歷史排名')
                now=datetime.now(ZoneInfo('Asia/Taipei'));end=min(cutoff,rankday)
                if end==today() and now.hour<21:end-=timedelta(days=1)
                raw,index=history(c,end,notice.info,refresh_recent=refresh_recent);prices=comparable(raw)
                dates=list(index.index[-3:])
                need_stock=both or (one and not is_etf)
                if need_stock:
                    notice.info('補齊營收、營益、估值與法人資料（已下載日期直接沿用）…')
                    codes=list(stocks.code) if both else list(selected.code)
                    rev,profit,val=fundamentals(c,companies,codes)
                    flow=chips(c,list(index.index[-22:]),refresh_recent=refresh_recent)
                else:rev=profit=val=flow=pd.DataFrame()
                context=(companies,rev,profit,val,flow)
                if 'outputs' not in st.session_state:st.session_state.outputs={}
                if one:st.session_state.outputs['第一區']=batch(selected,dates,prices,index,context,is_etf)
                if both:st.session_state.outputs['第二區']=batch(stocks,dates,prices,index,context)
                if both or only_etf:st.session_state.outputs['第三區']=batch(etfs,dates,prices,index,context,True)
                st.session_state['sources']=c.observed
                downloaded=sum(x['模式']=='新下載' for x in c.observed)
                reused=len(c.observed)-downloaded
                if downloaded:st.session_state.pop('cache_backup',None)
                st.session_state['speed_note']=f'本次實際下載{downloaded}次、快取命中{reused}次。歷史資料未強制複查時沿用原值；官方更正可勾選複查。'
                st.session_state['data_note']=f'行情與籌碼評分日：{dates[-1]}；三日：'+ '、'.join(map(str,dates))+f'；首頁排名日：{rankday}；基本面為本次可取得最新快照。'
            notice.success('計算完成；缺資料的標的會明確報錯，不自行填分。')
        except (SourceError,ValueError,KeyError,requests.RequestException) as exc:
            notice.error(str(exc))
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
