import json, unittest
from datetime import date
from pathlib import Path
import pandas as pd
from app import (WEIGHTS,temperature,score4,parse_market,comparable,etf_score,merge_etfs,
                 stock_score,stock_factors,SourceError,lists)

class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dates=list(pd.bdate_range('2025-01-01',periods=85).date)
        raw=[]
        for code in ['2330','0050']:
            for i,dt in enumerate(cls.dates):
                p=100+i*.5
                raw.append(dict(code=code,date=dt,name='測試',open=p,high=p+1,low=p-1,close=p,reference=p-.5,volume=10000000,money=2000000000))
        cls.prices=comparable(pd.DataFrame(raw));cls.index=pd.DataFrame({'close':[100+i*.2 for i in range(85)]},index=cls.dates)
        # 市場廣度樣本，非價格歷史，用來離線驗證截面計算。
        extra=[]
        for code in [str(3000+i) for i in range(310)]:
            x=cls.prices[cls.prices.code=='2330'].copy();x['code']=code;extra.append(x)
        cls.allprices=pd.concat([cls.prices,*extra],ignore_index=True)
        cls.companies=pd.DataFrame([{'公司代號':c,'產業別':'24'} for c in ['2330']+[str(3000+i) for i in range(310)]])
        cls.rev=pd.DataFrame([{'公司代號':'2330','營業收入-去年同月增減(%)':20,'累計營業收入-前期比較增減(%)':10,'資料年月':'11403'}])
        cls.profit=pd.DataFrame([{'公司代號':'2330','營業利益率(%)(營業利益)/(營業收入)':30,'年度':'114','季別':'1'}])
        cls.val=pd.DataFrame([{'Code':c,'PEratio':10 if c=='2330' else 20,'PBratio':2 if c=='2330' else 3} for c in cls.companies['公司代號']])
        cls.flow=pd.DataFrame([dict(code='2330',date=d,foreign=100000,trust=100000) for d in cls.dates[-22:]])
    def test_weights(self):self.assertEqual(sum(WEIGHTS.values()),100);self.assertNotIn('C',WEIGHTS)
    def test_temperatures(self):
        cases=[([70,75,80],'🔴 持續升溫'),([80,75,70],'🔵 持續降溫'),([80,81,80],'⚪ 持平'),([80,70,75],'🟠 回升'),([70,80,75],'🟣 回落'),([70,70,80],'🔴 持續升溫')]
        for scores,want in cases:self.assertEqual(temperature(scores),want)
    def test_etf_score_range(self):
        total,parts,_=etf_score('0050',self.dates[-1],self.prices)
        self.assertTrue(0<=total<=100);self.assertEqual(set(parts),{'趨勢','動能','量能','價格穩定'})
    def test_no_future_prices(self):
        dt=self.dates[-3];before=etf_score('0050',dt,self.prices)
        altered=self.prices.copy();altered.loc[altered.date>dt,'adj_close']=9999
        self.assertEqual(before,etf_score('0050',dt,altered))
    def test_merge(self):
        p=pd.DataFrame([dict(code='0050',name='ETF',來源='定期定額前15',定期定額名次=1)])
        m=pd.DataFrame([dict(code='0050',name='ETF',來源='首頁市值榜移入',首頁名次=6),dict(code='0056',name='ETF2',來源='首頁市值榜移入',首頁名次=20)])
        r=merge_etfs(p,m);self.assertEqual(len(r),2);self.assertIn('＋',r.iloc[0]['來源']);self.assertEqual(r.iloc[0]['首頁名次'],6)
    def test_complete_eight(self):
        total,parts,meta=stock_score('2330',self.dates[-1],self.allprices,self.index,self.companies,self.rev,self.profit,self.val,self.flow)
        self.assertEqual(set(parts),set(WEIGHTS));self.assertEqual(total,round(sum(WEIGHTS[k]*parts[k]/100 for k in parts),2))
        self.assertTrue(0<=parts['R']<=100);self.assertGreater(meta['參考停損'],0)
    def test_no_upside_target(self):
        p=self.allprices.copy();p['adj_high']=p.adj_close
        _,parts,meta=stock_score('2330',self.dates[-1],p,self.index,self.companies,self.rev,self.profit,self.val,self.flow)
        self.assertIsNone(meta['參考目標']);self.assertEqual(parts['R'],0)
    def test_missing_flow_fails(self):
        with self.assertRaises(SourceError):stock_score('2330',self.dates[-1],self.allprices,self.index,self.companies,self.rev,self.profit,self.val,self.flow.iloc[:-1])
    def test_missing_fundamentals_fails(self):
        with self.assertRaises(SourceError):stock_factors('2330',self.companies,self.rev.iloc[:0],self.profit,self.val)
    def test_dividend_reference_chain(self):
        raw=pd.DataFrame([dict(code='0050',date=self.dates[i],name='ETF',open=p,high=p,low=p,close=p,reference=ref,volume=1,money=1) for i,(p,ref) in enumerate([(100,100),(90,90),(91,90)])])
        d=comparable(raw)
        self.assertAlmostEqual(d.adj_close.iloc[0],d.adj_close.iloc[1]);self.assertAlmostEqual(d.adj_close.iloc[2]/d.adj_close.iloc[1],91/90)
    def test_twenty_split(self):
        class C:
            def get(self,url,*a,**kw):
                if 'values.json' in url:return json.dumps({'ranking':{'market':[dict(NO='0050' if i==5 else str(2300+i),NAME='測試',AMT=100-i,DATE='20261016') for i in range(20)]}})
                return json.dumps([dict(ETFsSecurityCode='0050' if i==0 else '009'+str(i).zfill(2),ETFsName='ETF',ETFsNumberofTradingAccounts=100-i) for i in range(15)])
        companies=pd.DataFrame([{'公司代號':str(2300+i)} for i in range(20) if i!=5]);funds=pd.DataFrame([{'基金代號':'0050','基金類型':'國內成分證券指數股票型基金'}])
        stocks,etfs,_=lists(C(),companies,funds)
        self.assertEqual(len(stocks),19);self.assertEqual(len(etfs),15);self.assertIn('＋',etfs.set_index('code').loc['0050','來源'])

if __name__=='__main__':unittest.main()
