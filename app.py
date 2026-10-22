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
