"""公開免費官方資料版：python -m streamlit run app.py。規格見README。"""
from __future__ import annotations
import hashlib, json, math, re, sqlite3, time
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
NAMES={'M':'大盤','I':'產業','F':'營運','V':'估值','T':'趨勢','L':'量能','H':'籌碼','R':'風報'}
class SourceError(RuntimeError):pass
class Halt(SourceError):pass
def today():return datetime.now(ZoneInfo('Asia/Taipei')).date()
class Client:
    """跨程序序列化；磁碟快取；15秒最小間隔；200次/日；限制即停止。"""
    def __init__(self, offline=False):
        self.offline = offline
        self.observed=[]
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

def number(x):
    return pd.to_numeric(str(x).replace(',','').replace('%','').replace('−','-').strip(),errors='coerce')
def normdate(v):
    s=re.sub(r'[^0-9]','',str(v))
    if len(s)==7:s=str(int(s[:3])+1911)+s[3:]
    return datetime.strptime(s,'%Y%m%d').date()
def score4(conditions):
    return 25*sum(bool(x) for x in conditions)
def js(client,url,params=None,ttl=86400):
    try:return json.loads(client.get(url,params,ttl=ttl))
    except json.JSONDecodeError:raise SourceError('來源未回傳有效JSON；停止本次操作，不自動重試。') from None

def snapshot(client,path):
    data=js(client,API+path)
    if not isinstance(data,list) or not data:raise SourceError('官方公開資料空白：'+path)
    return pd.DataFrame(data)

def registry(client):
    companies=snapshot(client,'/opendata/t187ap03_L')
    funds=snapshot(client,'/opendata/t187ap47_L')
    for d,k in [(companies,'公司代號'),(funds,'基金代號')]:
        if k not in d:raise SourceError('官方商品分類格式改變')
        d[k]=d[k].astype(str)
    return companies,funds

def lists(client,companies,funds):
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

def history(client,end,progress=None):
    frames=[];indices=[];cursor=end;checked=0
    # 最多160日，找85個市場交易日。週末略過，國定休市回應快取。
    while len(frames)<85 and checked<160:
        if cursor.weekday()<5:
            ttl=21600 if (today()-cursor).days<=7 else 30*86400
            payload=js(client,RWD+'/afterTrading/MI_INDEX',{'date':cursor.strftime('%Y%m%d'),'type':'ALLBUT0999','response':'json'},ttl)
            parsed=parse_market(payload)
            if parsed:
                p,idx=parsed
                if idx['date']!=cursor:raise SourceError('歷史行情回傳日期與查詢不符')
                frames.append(p);indices.append(idx)
                if progress:progress(f'建立全市場行情快取：{len(frames)}/85 個交易日；最新日期 {end}')
            elif '沒有符合' not in str(payload.get('stat','')):
                raise SourceError('行情來源回覆異常：'+str(payload.get('stat',''))[:100])
        cursor-=timedelta(days=1);checked+=1
    if len(frames)<83:raise SourceError('歷史市場資料不足83交易日')
    raw=pd.concat(frames,ignore_index=True).sort_values(['code','date'])
    raw=raw.drop_duplicates(['code','date'])
    index=pd.DataFrame(indices).sort_values('date').set_index('date')
    return raw,index

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
        g['atr']=tr.rolling(14).mean();g['money20']=g.money.rolling(20).mean();g['money5']=g.money.rolling(5).mean()
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

def chips(client,dates):
    frames=[]
    for dt in dates:
        j=js(client,RWD+'/fund/T86',{'date':dt.strftime('%Y%m%d'),'selectType':'ALLBUT0999','response':'json'},86400)
        frames.append(parse_t86(j,dt))
    return pd.concat(frames,ignore_index=True)

def fundamentals(client):
    rev=snapshot(client,'/opendata/t187ap05_L');profit=snapshot(client,'/opendata/t187ap17_L')
    finance=[]
    for suffix in ['fh','basi','ins','bd']:
        f=snapshot(client,'/opendata/t187ap06_L_'+suffix)
        eps=[k for k in f if '基本每股盈餘' in k]
        if len(eps)!=1:raise SourceError('金融財報EPS欄位改變：'+suffix)
        f['__financial_eps']=f[eps[0]].map(number)
        finance.append(f)
    profit=pd.concat([profit,*finance],ignore_index=True)
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

def advice(total,parts):
    if total<60:return '不進場','依退出規則評估'
    if total<70:return '不進場','減碼評估'
    if total<80:return '觀察','續抱檢查'
    if parts['M']<50 or min(parts[k] for k in ['F','T','R'])<75:
        return '高分但未通過必要條件','檢查風報及持有理由'
    return ('試單候選' if total<85 else '分批建倉候選' if total<90 else '完整建倉候選'),'續抱；加碼須另通過風控'

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
    parts['T']=score4([x.adj_close>x.ma60,x.ma20>x.ma60,x.ret60>r60,abs(x.adj_close-x.ma20)<=2*x.atr])
    last=hist.tail(20);up=last.loc[last.change>0,'volume'].mean();down=last.loc[last.change<0,'volume'].mean()
    parts['L']=score4([x.money20>=1e8,x.money5>=x.money20,up>down,(last.volume>0).all()])
    dates=list(index.loc[:dt].tail(20).index)
    f=flow[(flow.code==code)&flow.date.isin(dates)].sort_values('date')
    if len(f)!=20 or f[['foreign','trust']].isna().any().any():
        raise SourceError('該股法人近20日資料不齊；不把缺資料當0買賣超')
    net20=f.foreign.sum()+f.trust.sum();net5=f.tail(5).foreign.sum()+f.tail(5).trust.sum()
    denom=last.volume.sum()
    parts['H']=score4([f.foreign.sum()>0,f.trust.sum()>0,net5>0,denom>0 and net20/denom>=.01])
    # 比較價先換回評分日可交易價尺度，再算參考停損與目標。
    scale=x.close/x.adj_close;atr=x.atr*scale
    previous=hist.iloc[:-1]
    stop=float(previous.adj_low.tail(20).min()*scale-.5*atr)
    target=float(previous.adj_high.tail(60).max()*scale)
    entry=float(x.close);risk=entry-stop;cost=entry*(2*.001425+.003+.001)
    if target<=entry or stop<=0 or risk<=0:
        rr=None;parts['R']=0
    else:
        rr=(target-entry-cost)/(risk+cost)
        parts['R']=score4([rr>=2,rr>=3,risk<=2*atr,risk/entry<=.08])
    total=round(sum(WEIGHTS[k]*parts[k]/100 for k in WEIGHTS),2)
    buy,hold=advice(total,parts)
    meta.update({'市場廣度樣本':len(cross),'產業樣本':len(peer),'收盤':entry,'參考停損':round(stop,2),
                 '參考目標':round(target,2) if target>entry else None,'扣成本風報比':round(rr,2) if rr is not None else None,
                 'R說明':'前60日高點未高於現價，沒有可用上方目標，R=0' if target<=entry else '以前20日低點減0.5ATR為停損，前60日高點為目標',
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
    return total,parts,{'未持有':'ETF技術分，不沿用個股門檻','已持有':'另查淨值折溢價與產品風險'}

def batch(rows,dates,prices,index,context,is_etf=False):
    companies,rev,profit,val,flow=context
    results=[];detail=[]
    for item in rows.to_dict('records'):
        code=str(item['code']);record=dict(item);values=[]
        try:
            for dt in dates:
                total,parts,meta=etf_score(code,dt,prices) if is_etf else stock_score(code,dt,prices,index,companies,rev,profit,val,flow)
                values.append(total)
                detail.append({'代碼':code,'日期':str(dt),'總分':total,**{NAMES.get(k,k):v for k,v in parts.items()},**meta})
            record.update({'三日圖':values,'當日分數':values[-1],'近三日平均':round(sum(values)/3,2),'三日變化':round(values[-1]-values[0],2),
                           '狀態':temperature(values),'未持有':meta['未持有'],'已持有':meta['已持有'],'評分日':str(dates[-1]),
                           '日期序列':' → '.join(str(x) for x in dates),'分數序列':' → '.join(str(x) for x in values)})
        except (SourceError,KeyError,ValueError) as exc:
            record.update({'狀態':'無法評分','錯誤':str(exc)})
        results.append(record)
    return pd.DataFrame(results),pd.DataFrame(detail)

def display_results(table,detail,key):
    if table.empty:st.info('本區沒有標的');return
    st.dataframe(table,use_container_width=True,hide_index=True,column_config={
        '三日圖':st.column_config.LineChartColumn('近三日升降溫',y_min=0,y_max=100,width='medium'),
        '當日分數':st.column_config.NumberColumn(format='%.2f'),
        '近三日平均':st.column_config.NumberColumn(format='%.2f')})
    export=table.drop(columns=['三日圖'],errors='ignore')
    st.download_button('下載本區CSV',export.to_csv(index=False).encode('utf-8-sig'),file_name=key+'.csv',key=key+'_csv')
    if not detail.empty:
        with st.expander('八面向／ETF分項、價格與資料日期'):
            code=st.selectbox('選擇代碼',list(dict.fromkeys(detail['代碼'])),key=key+'_select')
            chosen=detail[detail['代碼']==code]
            chart=alt.Chart(chosen.assign(日期=pd.to_datetime(chosen['日期']))).mark_line(point=True).encode(
                x=alt.X('日期:T',title='評分日期'),y=alt.Y('總分:Q',scale=alt.Scale(domain=[0,100])),tooltip=['日期:T','總分:Q'])
            st.altair_chart(chart,use_container_width=True)
            st.dataframe(chosen,hide_index=True,use_container_width=True)
            st.download_button('下載分項CSV',detail.to_csv(index=False).encode('utf-8-sig'),file_name=key+'_detail.csv',key=key+'_detail')

def main():
    st.set_page_config(page_title='台股波段｜公開免費版',layout='wide')
    st.title('台股波段｜公開免費官方資料版')
    st.caption('免帳號、免token；事件面向已移除。個股八面向；ETF獨立公式。未回測，分數不代表勝率。')
    st.warning('三日個股分數為「以最新公開基本面與估值橫截面重估」：不是當時已知資訊的歷史回測。比較價依官方收盤價及參考價重建，不是供應商總報酬還原價。')
    with st.sidebar:
        st.header('查詢設定')
        cutoff=st.date_input('資料截止日',value=today(),max_value=today())
        offline=st.checkbox('只用本機快取',value=False)
        st.caption('目前支援上市普通股與上市ETF；不支援上櫃、興櫃。初次建立85個交易日全市場快取約需30–60分鐘；後續共用快取。')
        st.caption('所有請求至少間隔15秒、每日最多200次；遇限制立即停止。不能保證IP不被來源限制。')
        st.caption('資料來源：證交所官方OpenAPI與官網公開JSON；原始資料皆會快取，不需手動證據CSV。')
        st.link_button('官方OpenAPI文件','https://openapi.twse.com.tw/')
    c=Client(offline)
    st.subheader('第一區｜股票中文名稱／代碼')
    query=st.text_input('上市股票完整簡稱或代碼，例如台積電、2330')
    one=st.button('計算這一檔')
    st.subheader('第二區｜首頁市值榜20筆中的非ETF')
    st.caption('直接取首頁原榜，保留名次與日期；ETF移到第三區，不補名額。')
    both=st.button('更新第二區與第三區')
    st.subheader('第三區｜定期定額前15 ETF＋首頁榜移入ETF')
    st.caption('按代碼合併去重；ETF分數＝趨勢40%＋動能25%＋量能20%＋價格穩定15%。不使用個股買賣門檻。')
    only_etf=st.button('僅更新第三區')
    if one or both or only_etf:
        notice=st.empty()
        try:
            with st.spinner('讀取公開資料與快取；首次建立歷史較久，可保留視窗等待。'):
                companies,funds=registry(c)
                stocks,etfs,rankday=lists(c,companies,funds)
                if cutoff<rankday and (both or only_etf):raise SourceError('首頁榜單僅最新快照，請選最新截止日，不能當歷史排名')
                now=datetime.now(ZoneInfo('Asia/Taipei'));end=min(cutoff,rankday)
                if end==today() and now.hour<21:end-=timedelta(days=1)
                raw,index=history(c,end,notice.info);prices=comparable(raw)
                dates=list(index.index[-3:])
                if one:
                    q=query.strip()
                    a=companies[(companies['公司代號']==q)|(companies['公司簡稱']==q)]
                    b=funds[(funds['基金代號']==q)|(funds['基金簡稱']==q)]
                    if len(a)==1:
                        row=a.iloc[0];selected=pd.DataFrame([{'code':str(row['公司代號']),'name':row['公司簡稱']}]);is_etf=False
                    elif len(b)==1:
                        row=b.iloc[0];selected=pd.DataFrame([{'code':str(row['基金代號']),'name':row['基金簡稱']}]);is_etf=True
                    else:raise SourceError('查無唯一上市名稱／代碼，目前未支援上櫃與興櫃')
                need_stock=both or (one and not is_etf)
                if need_stock:
                    notice.info('取得營收、營益、估值與近22交易日法人資料…')
                    rev,profit,val=fundamentals(c);flow=chips(c,list(index.index[-22:]))
                else:rev=profit=val=flow=pd.DataFrame()
                context=(companies,rev,profit,val,flow)
                if 'outputs' not in st.session_state:st.session_state.outputs={}
                if one:st.session_state.outputs['第一區']=batch(selected,dates,prices,index,context,is_etf)
                if both:st.session_state.outputs['第二區']=batch(stocks,dates,prices,index,context)
                if both or only_etf:st.session_state.outputs['第三區']=batch(etfs,dates,prices,index,context,True)
                st.session_state['sources']=c.observed
                st.session_state['data_note']=f'行情與籌碼評分日：{dates[-1]}；三日：'+ '、'.join(map(str,dates))+f'；首頁排名日：{rankday}；基本面為本次可取得最新快照。'
            notice.success('計算完成；缺資料的標的會明確報錯，不自行填分。')
        except (SourceError,ValueError,KeyError,requests.RequestException) as exc:
            notice.error(str(exc))
    if 'outputs' in st.session_state:
        st.info(st.session_state.get('data_note',''))
        with st.expander('來源與快取擷取時間'):
            st.dataframe(pd.DataFrame(st.session_state.get('sources',[])),hide_index=True)
        st.caption('🔴升溫　🔵降溫　⚪持平　🟠回升　🟣回落；持平＝三日最高最低差≤2分。所有圖固定0–100分。')
        for key,(table,detail) in st.session_state.outputs.items():
            st.subheader(key+'結果（修改設定後須重新計算）')
            display_results(table,detail,key)

if __name__=='__main__':main()
