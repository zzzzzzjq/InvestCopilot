#个股行情数据同步

import datetime
import sys
import pandas as pd

sys.path.append("../../../..")
import InvestCopilot_App.models.toolsutils.dbutils as dbutls

def sync_stock_market_by_days ():
    #个股行情数据同步
    bt=datetime.datetime.now()
    ntradeDate = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")
    import pymssql
    # 定义数据库连接信息
    db_host = '120.78.94.82'
    db_host = '172.18.163.218'
    db_database = 'qai'
    db_user = 'IZTKD54Y1PP0Z4Z\msql'
    db_password = 'M@231014'
    connection = pymssql.connect(host=db_host,port="1433",user=db_user, password=db_password)  # SQL Server身份验证建立连接
    print("connection:", connection)
    # pool.close()
    # 创建数据库连接池
    # pool = PooledDB(pymssql, maxconnections=10, host=db_host, database=db_database,port=1433, user=db_user, password=db_password)
    # 创建游标对象
    cursor = connection.cursor()
    # 执行查询
    query = """ 
    select MX.id,MX.name,MarketDate,prc.Close_* adj.CumAdjFactor AdjClose, prc.Open_,prc.High,prc.Low,prc.Close_,prc.Volume,adj.CumAdjFactor
    from  DS2PRIMQTPRC prc 
    join  DS2ADJ adj 
    on prc.infocode=adj.infocode
    AND adj.ADJTYPE = 2 
    AND prc.MARKETDATE BETWEEN adj.ADJDATE AND isnull(adj.ENDADJDATE,'06/06/2095') 
    AND prc.MARKETDATE>'%s'
    join vw_Ds2Mapping MP

    on MP.VenCode=prc.InfoCode
    join vw_SecurityMasterX MX
    on MX.SecCode = MP.seccode
    and MX.Typ = MP.typ
    --where prc.infocode=72990 
    where MX.id='%s' --'AAPL'
    order by prc.marketdate asc 
        """
    con,cur = dbutls.getConnect()
    i_sql = "insert into newdata.reuters_us_stockprice (windcode,stockcode,ric,stockname,s_dq_close,s_dq_volume,s_dq_open,s_dq_high,s_dq_low,tradedate)" \
            " values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    # dsql="delete from newdata.reuters_financial_report"
    # cur.execute(dsql)
    qdestMaxDay="select stockcode,max(tradedate) as tradedate from  newdata.reuters_us_stockprice group by stockcode"
    cur.execute(qdestMaxDay)
    codeMaxDays = cur.fetchall()
    codeMaxDay_dt={}
    for cdt in codeMaxDays:
        codeMaxDay_dt[cdt[0]]=cdt[1]
    q_codes="select id,windcode,ric,ticker from  config.reuters_usstocks where windcode is not null"
    cur.execute(q_codes)
    codlist = cur.fetchall()
    for s in codlist:
        print(s)
        qid = s[0]
        windCode=s[1]
        ric=s[2]
        stockCode=s[3]
        tradedate="01/01/2010"
        if stockCode in codeMaxDay_dt:
            tradedate =  codeMaxDay_dt[stockCode]
        if tradedate==ntradeDate:
            print('exists ')
            continue
        # cur.execute("select max(tradedate) from newdata.reuters_us_stockprice where stockcode='%s'"% (s['symbol']))
        # tradedate = cur.fetchone()[0]
        # print("tradedate:",tradedate)
        # if tradedate is None:
        query1=query%(tradedate,qid)
        print("query1:",query1)
        bqt=datetime.datetime.now()
        cursor.execute(query1)
        eqt = datetime.datetime.now()
        print("query time:%s"%(eqt-bqt).total_seconds())
        ndts=[]
        frest = cursor.fetchall()
        if len(frest)==0:
            print("is empty:%s"%qid)
            continue
        print("%s need add num:%s"%(windCode,len(frest)))

        bat = datetime.datetime.now()
        for row in frest:
            # print("row:",row)
            ndt = [windCode, stockCode, ric]
            codeid=row[0]
            codeName=row[1]
            MarketDate=row[2]
            tradeDate=MarketDate.strftime("%Y%m%d")
            AdjClose=row[3]
            CumAdjFactor=row[9]#复权因子
            if pd.isnull(row[4]):
                continue
            if pd.isnull(row[5]):
                continue
            if pd.isnull(row[6]):
                continue
            if pd.isnull(row[7]):
                continue
            s_dq_open=float(row[4])*CumAdjFactor
            s_dq_high=float(row[5])*CumAdjFactor
            s_dq_low=float(row[6])*CumAdjFactor
            s_dq_close=float(row[7])*CumAdjFactor
            s_dq_volume=row[8]
            ndt.extend([codeName,s_dq_close,s_dq_volume,s_dq_open,s_dq_high,s_dq_low,tradeDate])
            ndts.append(ndt)
            # print("ndt:",ndt)
        cur.executemany(i_sql,ndts)
        con.commit()
        eat = datetime.datetime.now()
        print("add finish time:%s"%(eat-bat).total_seconds())
    cursor.close()
    connection.close()

    cur.close()
    con.close()
    print("end...")
    et = datetime.datetime.now()
    print(bt)
    print(et)

def sync_index_market_by_days ():
    #个股行情数据同步
    bt=datetime.datetime.now()
    ntradeDate = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")
    import pymssql
    # 定义数据库连接信息
    db_host = '120.78.94.82'
    db_host = '172.18.163.218'
    db_database = 'qai'
    db_user = 'IZTKD54Y1PP0Z4Z\msql'
    db_password = 'M@231014'
    connection = pymssql.connect(host=db_host,port="1433",user=db_user, password=db_password)  # SQL Server身份验证建立连接
    print("connection:", connection)
    # pool.close()
    # 创建数据库连接池
    # pool = PooledDB(pymssql, maxconnections=10, host=db_host, database=db_database,port=1433, user=db_user, password=db_password)
    # 创建游标对象
    cursor = connection.cursor()
    # 执行查询
    query = """   
        SELECT  D.DSINDEXCODE
                , DI.INDEXDESC
                , D.DATATYPEVALUE as s_dq_close   
                ,VALUEDATE as TRADEDATE
                --, CONVERT( varchar(8),D.VALUEDATE,112) AS TRADEDATE 
        FROM    DS2INDEXADDLDATA D
                JOIN DS2EQUITYINDEX DI
                     ON D.DSINDEXCODE = DI.DSINDEXCODE
        WHERE  D.DSINDEXCODE = '%s' --sp500
        AND D.DATATYPEMNEM IN ( 'PI')
		and d.VALUEDATE>CONVERT(DATE,'%s',112)
		order by valuedate desc
        """
    con,cur = dbutls.getConnect()
    i_sql = "insert into newdata.reuters_us_indexprice (indexcode,indexname,s_dq_close,tradedate)" \
            " values(%s,%s,%s,%s)"
    # dsql="delete from newdata.reuters_financial_report"
    # cur.execute(dsql)
    qdestMaxDay="select indexcode,max(tradedate) as tradedate from  newdata.reuters_us_indexprice group by indexcode"
    cur.execute(qdestMaxDay)
    codeMaxDays = cur.fetchall()
    codeMaxDay_dt={}
    for cdt in codeMaxDays:
        codeMaxDay_dt[cdt[0]]=cdt[1]
    indexCodes = ['40823','41620']#NDX,SP500
    for indexcode in indexCodes:
        print(indexcode)
        tradedate="20100101"
        if indexcode in codeMaxDay_dt:
            tradedate =  codeMaxDay_dt[indexcode]
        if tradedate==ntradeDate:
            print('exists ')
            continue
        # cur.execute("select max(tradedate) from newdata.reuters_us_stockprice where stockcode='%s'"% (s['symbol']))
        # tradedate = cur.fetchone()[0]
        # print("tradedate:",tradedate)
        # if tradedate is None:
        query1=query%(indexcode,tradedate)
        print("query1:",query1)
        bqt=datetime.datetime.now()
        cursor.execute(query1)
        eqt = datetime.datetime.now()
        print("query time:%s"%(eqt-bqt).total_seconds())
        ndts=[]
        frest = cursor.fetchall()
        if len(frest)==0:
            print("is empty:%s"%indexcode)
            continue
        print("%s need add num:%s"%(indexcode,len(frest)))

        bat = datetime.datetime.now()
        for row in frest:
            # print("row:",row)
            #indexcode,indexname,s_dq_close,tradedate
            s_dq_close=row[2]
            if pd.isnull(s_dq_close):
                continue
            MarketDate=row[3]
            tradeDate=MarketDate.strftime("%Y%m%d")
            ndts.append([row[0],row[1],s_dq_close,tradeDate])
        cur.executemany(i_sql,ndts)
        con.commit()
        eat = datetime.datetime.now()
        print("add finish time:%s"%(eat-bat).total_seconds())
    cursor.close()
    connection.close()

    cur.close()
    con.close()
    print("end...")
    et = datetime.datetime.now()
    print(bt)
    print(et)



if __name__ == '__main__':
    sync_index_market_by_days()#指数行情同步
    sync_stock_market_by_days()#个股价格同步
    pass