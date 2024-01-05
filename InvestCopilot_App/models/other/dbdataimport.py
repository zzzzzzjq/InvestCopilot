import datetime
import json

import numpy as np
import pandas as pd
import requests
import traceback
import sys
sys.path.append("../../..")
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
import  InvestCopilot_App.models.toolsutils.dbmongo as  dbmongo

alyurl = "https://www.daohequant.com/mobileApp/api/informationApi/"
def getAlySetData(webSite, webTag, addQuerys={}, rtColumns={}):
    rtdata = []
    try:
        data = {'doMethod': "getSetData", 'webSite': webSite, 'webTag': webTag, "userName": "mgName!@#",
                "passWord": "pwd@#A!@#", }
        headers = {"Accept": "application/json, text/plain, */*",
                   "Accept-Encoding": "gzip, deflate, br",
                   "Accept-Language": "zh-CN,zh;q=0.9",
                   "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36", }
        import pickle
        if len(addQuerys) > 0:
            data['addQuerys'] = json.dumps(addQuerys)
            # data['addQuerys']=pickle.dumps(addQuerys)
        if len(rtColumns) > 0:
            data['rtColumns'] = json.dumps(rtColumns)

        resp = requests.post(alyurl, data=data, headers=headers, timeout=(10, 30))
        if resp.status_code == 200:
            rtjs = resp.json()
            if rtjs['errorFlag']:
                return rtjs['data']
            return rtdata
        else:
            pass
        return rtdata
    except:
        print("getAlySetData error")
        print(traceback.format_exc())
        pass

    return rtdata
import InvestCopilot_App.models.toolsutils.ToolsUtils as Tuts
import InvestCopilot_App.models.toolsutils.dbutils as dbutls
def copyStockPoolData():

    impdbs = ['jpStocks','hkStocks','ksStocks','chStocks','twStocks']

    con, cur = dbutls.getConnect()
    for dbset in impdbs:
        rts = getAlySetData("StockPool", dbset, addQuerys={},
                                rtColumns={"windCode": 1, "symbol": 1, "ticker": 1, "bbgCode": 1,
                                           "ord": 1, "shortName":1,"fullName": 1, "englishName":1,   })  #
        sorted(rts, key=lambda x: x['ord'])
        addDatas=[]
        if len(rts) > 0:
            # print("spx500:", rts)
            for dt in rts:
                #id, windcode, symbol, ticker, bbgcode, shortname, fullname, englishname, ord, inserttime, status
                windcode=dt['windCode']
                symbol=dt['symbol']
                ticker=dt['ticker']
                bbgCode=dt['bbgCode']
                ord=dt['ord']
                shortName=dt['shortName']
                fullName=dt['fullName']
                englishName=dt['englishName']
                sid = Tuts.md5("%s"%(windcode))
                addDatas.append([sid,windcode,symbol, ticker, bbgCode, shortName, fullName, englishName, ord,datetime.datetime.now(),None])
        print (len(addDatas))
        ts = """   INSERT INTO config.{}(	id, windcode, symbol, ticker, bbgcode, shortname, fullname, englishname, ord, inserttime, status)	VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)   """.format(dbset)
        print("ts:",ts)
        cur.executemany(ts,addDatas)
        cur.execute("select  count(1) from config.%s group by id having count(1)>1"%dbset)
        sss = cur.fetchall()[0][0]
        print('ss:',sss)
        con.commit()
    cur.close()
    con.close()

import InvestCopilot_App.models.other.linuxPgdbpool as linux_dbutls
def  copyStockInfo():

    isql="INSERT INTO stockinfo " \
        "(stockcode, windcode, eastcode, stockname, stocktype, area, updateday, pinyin, relationcode, disabled)" \
        "VALUES(%s, %s, %s, %s, %s, %s, %s,%s, %s, %s)"

    acon,acur = linux_dbutls.getConnect()

    q_s="select * from stockinfo"
    alydt=pd.read_sql(q_s,acon)
    acur.close()
    acon.close()
    print("alydt:",len(alydt))
    lcon,lcur = dbutls.getConnect()

    lcur.execute("delete from stockinfo")
    print("delete:",lcur.rowcount)
    lcur.executemany(isql,alydt.values.tolist())
    print("copy:", lcur.rowcount)
    lcon.commit()
    lcur.close()
    lcon.close()

def  copyWorkDays():

    acon,acur = linux_dbutls.getConnect()
    lcon,lcur = dbutls.getConnect()

    # isql="INSERT INTO  config.workday  (seq,tradedate) VALUES(%s, %s)"
    # q_s="select * from newdata.workday"
    # alydt=pd.read_sql(q_s,acon)
    # print("alydt:",len(alydt))

    # lcur.execute("delete from   config.workday  ")
    # print("delete:",lcur.rowcount)
    # lcur.executemany(isql,alydt.values.tolist())
    # print("copy:", lcur.rowcount)
    # lcon.commit()
    #
    #
    # isql="INSERT INTO  config.hkworkday  (workday) VALUES(%s)"
    # q_s="select * from newdata.hkworkday"
    # alydt=pd.read_sql(q_s,acon)
    # acur.close()
    # acon.close()
    # print("alydt:",len(alydt))
    # lcur.execute("delete from   config.hkworkday  ")
    # print("delete:",lcur.rowcount)
    # lcur.executemany(isql,alydt.values.tolist())
    # print("copy:", lcur.rowcount)
    # lcon.commit()
    #

    # isql="INSERT INTO  config.usaworkday  (workday) VALUES(%s)"
    # q_s="select * from newdata.usaworkday"
    # alydt=pd.read_sql(q_s,acon)
    # acur.close()
    # acon.close()
    # print("alydt:",len(alydt))
    # lcur.execute("delete from   config.usaworkday  ")
    # print("delete:",lcur.rowcount)
    # lcur.executemany(isql,alydt.values.tolist())
    # print("copy:", lcur.rowcount)
    # lcon.commit()


    # isql="INSERT INTO  spider.usa_stockcode  (stockcode,eastcode,stockname,area) VALUES(%s,%s,%s,%s)"
    # q_s="select * from spider.usa_stockcode"
    # alydt=pd.read_sql(q_s,acon)
    # acur.close()
    # acon.close()
    # print("alydt:",len(alydt))
    # lcur.execute("delete from  spider.usa_stockcode  ")
    # print("delete:",lcur.rowcount)
    # lcur.executemany(isql,alydt.values.tolist())
    # print("copy:", lcur.rowcount)
    # lcon.commit()

    # isql="INSERT INTO newdata.asharedescription (object_id, s_info_windcode, s_info_code, s_info_name, s_info_compname, s_info_compnameeng, s_info_isincode, s_info_exchmarket, s_info_listboard, s_info_listdate, s_info_delistdate, s_info_sedolcode, crncy_code, s_info_pinyin, s_info_listboardname, is_shsc, opdate, opmode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # q_s="select * from newdata.asharedescription"
    # alydt=pd.read_sql(q_s,acon)
    # acur.close()
    # acon.close()
    # print("alydt:",len(alydt))
    # lcur.execute("delete from newdata.asharedescription  ")
    # print("delete:",lcur.rowcount)
    # lcur.executemany(isql,alydt.values.tolist())
    # print("copy:", lcur.rowcount)
    # lcon.commit()

    isql="INSERT INTO spider.eaststockcode (eastcode, enable, stocktype) VALUES(%s, %s, %s)"
    q_s="select * from spider.eaststockcode"
    alydt=pd.read_sql(q_s,acon)
    acur.close()
    acon.close()
    print("alydt:",len(alydt))
    lcur.execute("delete from spider.eaststockcode  ")
    print("delete:",lcur.rowcount)
    lcur.executemany(isql,alydt.values.tolist())
    print("copy:", lcur.rowcount)
    lcon.commit()


    acur.close()
    acon.close()
    lcur.close()
    lcon.close()

def fmtpinyin():
    # q_stockInfo = "select * from stockinfo t where  t.stocktype <> 'sh' and t.stocktype <> 'sz'"

    lcon,lcur = dbutls.getConnect()
    q_stockInfo = "select t.WINDCODE,t.STOCKNAME from config.stockinfo t where  t.stocktype   in ('usa')"
    u_stockInfo = "update config.stockinfo t set pinyin=%s where windcode=%s"
    stockInfoDF = pd.read_sql(q_stockInfo, lcon)
    stockInfoDF=tools_utils.dfColumUpper(stockInfoDF)
    # rs_stockInfo = cur.execute(q_stockInfo)
    upPy = []
    for row in stockInfoDF.itertuples():
        windcode = row.WINDCODE
        pinyin = tools_utils.getPinYinInitial(row.STOCKNAME)
        print("pinyin:",pinyin)
        upPy.append([str(pinyin).lower(), windcode])

    lcur.executemany(u_stockInfo, upPy)
    # 代码去重复
    # d_rf = "delete from stockinfo t where ctid <> (select min(x.ctid) from stockinfo x where t.stockcode=x.stockcode)"
    # lcur.execute(d_rf)
    lcon.commit()
    lcon.close()

def sqlserver():
    from dbutils.pooled_db import PooledDB
    import pymssql

    # 定义数据库连接信息
    db_host = '120.78.94.82'
    db_database = 'qai'
    db_user = 'query'
    db_password = 'Q@231106'

    # 定义数据库连接信息
    # db_host = '127.0.0.1'
    db_database = 'qai'
    db_user = 'IZTKD54Y1PP0Z4Z\msql'
    db_password = 'M@231014'

    # 创建数据库连接池
    # pool = PooledDB(pymssql, maxconnections=10, host=db_host, database=db_database,port=1433, user=db_user, password=db_password)

    connection = pymssql.connect(host=db_host, server="IZTKD54Y1PP0Z4Z\msql", port="1433",
                           user=db_user, password=db_password)  # SQL Server身份验证建立连接

    print("connection:",connection)
    # pool.close()
    # 创建数据库连接池
    # pool = PooledDB(pymssql, maxconnections=10, host=db_host, database=db_database,port=1433, user=db_user, password=db_password)

    # 从连接池获取连接

    # 创建游标对象
    cursor = connection.cursor()

    # 执行查询
    query = """ 
select MX.id,MX.name,MarketDate,prc.Close_* adj.CumAdjFactor AdjClose, prc.Open_,prc.High,prc.Low,prc.Close_,prc.Volume,adj.CumAdjFactor
from  DS2PRIMQTPRC prc 
join  DS2ADJ adj 
on prc.infocode=adj.infocode
AND adj.ADJTYPE = 2 
AND prc.MARKETDATE BETWEEN adj.ADJDATE AND isnull(adj.ENDADJDATE,'06/06/2025') 
AND prc.MARKETDATE>='01/01/2010'
join vw_Ds2Mapping MP

on MP.VenCode=prc.InfoCode
join vw_SecurityMasterX MX
on MX.SecCode = MP.seccode
and MX.Typ = MP.typ
--where prc.infocode=72990 
where MX.id='AAPL' --'AAPL'
order by prc.marketdate asc 
    """
    cursor.execute(query)

    # 获取查询结果

    for row in cursor.fetchall():
        print(row)
    i_sql="insert into newdata.reuters_us_stockprice (windcode,stockcode,ric,stockname,s_dq_close,s_dq_close,s_dq_volume,s_dq_open,s_dq_high,s_dq_low,tradedate) values()"
    # 关闭游标和连接
    cursor.close()
    connection.close()


from pyDes import *
def copyUserInfo():
    alycon,alycur = linux_dbutls.getConnect()
    lcon,lcur = dbutls.getConnect()
    #迁移公司
    isql="INSERT INTO company(companyid, companyname, shortcompanyname, updatetime) VALUES (%s, %s, %s,current_timestamp )"
    q_s="select * from company where companyid not in ('000','001','002','003','010')"
    # alydt=pd.read_sql(q_s,alycon)
    # print("company alydt:",len(alydt))
    # lcur.execute("delete from company ")
    # print("company delete:",lcur.rowcount)
    # cpts=alydt.values.tolist()
    # print("cpts:",cpts)
    # cpts=[x.append(datetime.datetime.now()) for x in cpts]
    # print("cpts:",cpts)
    # lcur.executemany(isql,cpts)
    # print("company copy:", lcur.rowcount)
    # lcon.commit()
    #用户关系迁移
    q_s = "select * from copilot_tusers where  companyid is null --companyid !='pinnacle'"
    alydt = pd.read_sql(q_s, alycon)
    print("company alydt:", len(alydt))

    from InvestCopilot_App.models.user import UserInfoUtil as user_util
    # lcur.execute("delete from company  ")
    alydt=tools_utils.dfColumUpper(alydt)
    import base64
    userdt=[]
    comprldt=[]
    researchdt=[]
    userroledt=[]
    for dt in alydt.itertuples():
        userid=str(dt.USERID)
        oldPwd=str(dt.USERPASSWORD)
        privilegeset=str(dt.PRIVILEGESET)
        print('明文 oldPwd:', oldPwd)
        #解码
        k = des("\5\21\34\12\11\12\24\0", CBC, "\0\0\0\0\0\0\0\0", padmode=PAD_PKCS5)
        password2 = k.decrypt(base64.decodebytes(oldPwd.encode('ascii'))).decode('ascii')
        print('明文 password2:', password2)
        #转码
        newPassWord = user_util.passWordEncode(password2)
        print(newPassWord)
        #用户信息
        userdt.append([userid,dt.USERNAME,newPassWord,None,privilegeset,dt.USERSTATUS,dt.USERREALNAME,dt.USERNICKNAME,None])
        #用户与公司关系
        companyId=dt.COMPANYID
        if not pd.isnull(companyId):
            comprldt.append([companyId,userid])
        else:
            #临时账户
            comprldt.append(["temp_account",userid])
        #用户研报权限
        if not pd.isnull(dt.SEARCHROLE):
            # researchdt.append([userid,str(dt.SEARCHROLE)])
            userroledt.append([userid, "69"])#研报权限
        else:
            #userrole
            userroledt.append([userid, "19"])

    for us in userdt:
        print("us",us)
    for compr in comprldt:
        print("compr",compr)

    # for us in researchdt:
    #     print("researchdt",us)
    iuser="INSERT INTO tusers (userid, username, userpassword, roleid, privilegeset, userstatus, userrealname, usernickname, department)" \
          " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    icompanyrl = "insert into companyuser (companyid,userid)  values(%s,%s)"
    # iresearchl = "insert into tresearch (userid,need)  values(%s,%s)"
    iuserrolel = "insert into userrole (userid,roleid)  values(%s,%s)"
    # lcur.execute("delete from tusers where to_number (userid,'fm99999') > 1 ")
    # print("tusers delete:", lcur.rowcount)
    lcur.executemany(iuser, userdt)
    print("tusers copy:", lcur.rowcount)

    # lcur.execute("delete from companyuser where to_number (userid,'fm99999') > 1")
    # print("companyuser delete:", lcur.rowcount)
    lcur.executemany(icompanyrl, comprldt)
    # print("companyuser copy:", lcur.rowcount)
    # lcur.execute("delete from tresearch where to_number (userid,'fm99999') > 1")
    # print("tresearch delete:", lcur.rowcount)
    # lcur.executemany(iresearchl, researchdt)
    # print("tresearch copy:", lcur.rowcount)

    # lcur.execute("delete from userrole where to_number (userid,'fm99999') > 1")
    # print("userrole delete:", lcur.rowcount)

    lcur.executemany(iuserrolel, userroledt)
    print("userrole copy:", lcur.rowcount)
    lcon.commit()
    alycur.close()
    alycon.close()

    lcur.close()
    lcon.close()


def copyPortfolio():
    alycon,alycur = linux_dbutls.getConnect()
    lcon,lcur = dbutls.getConnect()
    #自选股组合
    isql="INSERT INTO portfolio.user_portfolio (userid, portfolioid, portfoliotype, portfolioname, seqno, createtime, averageyield, stocknum) " \
         "VALUES(%s, %s, %s, %s, %s, %s, %s, 0)"
    q_s="select * from copilot_portfolio where userid in (select userid from copilot_tusers where (companyid !='pinnacle') or (companyid is null) )"
    alydt=pd.read_sql(q_s,alycon)
    print("user_portfolio alydt:",len(alydt))
    lcur.execute("delete from  portfolio.user_portfolio where userid  in ( select userid from companyuser c where  c.companyid!='pinnacle'  ) ")
    print("user_portfolio delete:",lcur.rowcount)
    cpts=alydt.values.tolist()
    # cpts=[x.append(datetime.datetime.now()) for x in cpts]
    # print("cpts:",cpts)
    lcur.executemany(isql,cpts)
    print("user_portfolio copy:", lcur.rowcount)
    # lcon.commit()

    #股票池持仓
    q_s = "select * from copilot_portfoliolist where userid in (select userid from copilot_tusers where (companyid !='pinnacle' ) or (companyid is null) )"
    alydt = pd.read_sql(q_s, alycon)
    print("copilot_portfoliolist alydt:", len(alydt))

    iuser="INSERT INTO portfolio.user_portfolio_list (userid, portfolioid, windcode, seqno, createtime) VALUES(%s, %s, %s, %s,%s)"

    lcur.execute("delete from portfolio.user_portfolio_list  where userid  in ( select userid from companyuser c where c.companyid!='pinnacle' ) -- to_number (userid,'fm99999') > 1")
    print("portfolio.user_portfolio_list delete:", lcur.rowcount)
    stockdt=alydt.values.tolist()
    lcur.executemany(iuser, stockdt)
    print("portfolio.user_portfolio_list copy:", lcur.rowcount)

    #更新数量
    q_sn="select  count(1) ,portfolioid from  portfolio.user_portfolio_list  group by portfolioid"
    lddt = pd.read_sql(q_sn, lcon)
    uddt=lddt.values.tolist()
    u_n="update   portfolio.user_portfolio  set stocknum=%s where portfolioid=%s"
    lcur.executemany(u_n, uddt)
    lcon.commit()
    alycur.close()
    alycon.close()
    lcur.close()
    lcon.close()

def copyTemplate():
    alycon,alycur = linux_dbutls.getConnect()
    lcon,lcur = dbutls.getConnect()

    #查询用户自选股组合 只迁移公司账号
    q_alytemp="select userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate from factorrluser f" \
              " where templatetype='100' and userid in (select userid from copilot_tusers where companyid='pinnacle') order by templateno"

    tempDF=pd.read_sql(q_alytemp,alycon)
    tempDF=tools_utils.dfColumUpper(tempDF)
    local_in_template="INSERT INTO public.factorrluser " \
                      "(userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate, portfolioid)" \
                      "VALUES(%s, %s, %s, %s, %s,%s, %s, %s, %s)"

    local_in_factorn="INSERT INTO public.factortemplate" \
                      "(templateno, factorno, orderno) VALUES(%s, %s, %s)"

    add_tempdts = []
    add_factors = []
    for _i,tempdt in tempDF.iterrows():
        #获取用户的所有组合
        userId=str(tempdt.USERID)
        q_localcomb = "select * from portfolio.user_portfolio up where userid=%s"
        combDF=pd.read_sql(q_localcomb,lcon,params=[userId])
        combDF = tools_utils.dfColumUpper(combDF)
        portfolioids = combDF['PORTFOLIOID'].values.tolist()
        tempdt = tempdt.values.tolist()#模板接口 关联所有组合
        templateNo = tempdt[1]
        for _tid,portfolioid in enumerate(portfolioids):
            new_templateNo=str(_i)+"_"+str(_tid+1)
            tempdt[1]=new_templateNo
            tempdt[6]=_tid+1
            add_tempdts.append(tempdt+[portfolioid])#模版配置表
            q_alyfactor="select '{}' as templatno,factorno,orderno from factortemplate f where templateno =%s order by orderno".format(new_templateNo)
            factorDF=pd.read_sql(q_alyfactor,alycon,params=[templateNo])
            factorDF = tools_utils.dfColumUpper(factorDF)
            add_factors.extend(factorDF.values.tolist())

    for x in add_tempdts:
        print("template:",x)
    print("*"*100)
    for x in add_factors:
        print("factors:",x)
    #添加  模版配
    lcur.execute("delete from  public.factorrluser where templateno not in ('8') ")
    lcur.executemany(local_in_template,add_tempdts)
    print("user_portfolio delete:",lcur.rowcount)

    #为模版添加指标
    lcur.execute("delete from  public.factortemplate where templateno not in ('8') ")
    lcur.executemany(local_in_factorn,add_factors)
    print("user_portfolio delete:",lcur.rowcount)
    lcon.commit()

    alycur.close()
    alycon.close()

    lcur.close()
    lcon.close()



def copyAmStocks():
    impdbs = ['stocklist']
    con, cur = dbutls.getConnect()
    for dbset in impdbs:
        rts = getAlySetData("StockPool", dbset, addQuerys={},
                                rtColumns={"windCode": 1, "symbol": 1, "ticker": 1, "bbgCode": 1,"bbgName": 1,
                                           "ord": 1, "shortName":1,"fullName": 1, "englishName":1,   })  #
        sorted(rts, key=lambda x: x['ord'])
        addDatas=[]
        if len(rts) > 0:
            # print("spx500:", rts)
            for dt in rts:
                #id, windcode, symbol, ticker, bbgcode, shortname, fullname, englishname, ord, inserttime, status
                windcode=dt['windCode']
                symbol=dt['symbol']
                if "bbgCode" not in dt:
                    print("dt:",dt)
                    continue

                ticker=str(dt['bbgCode']).replace("EQUITY","").strip()
                bbgCode=dt['bbgCode']
                ord=dt['ord']
                shortName=dt['bbgName']
                fullName=dt['bbgName']
                englishName=dt['bbgName']
                sid = Tuts.md5("%s"%(windcode))
                addDatas.append([sid,windcode,symbol, ticker, bbgCode, shortName, fullName, englishName, ord,datetime.datetime.now(),None])
        print (len(addDatas))
        ts = """   INSERT INTO config.{}(	id, windcode, symbol, ticker, bbgcode, shortname, fullname, englishname, ord, inserttime, status)	VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)   """.format("usstocks")
        print("ts:",ts)
        cur.executemany(ts,addDatas)
        cur.execute("select  count(1) from config.%s group by id having count(1)>1"%"usstocks")
        # sss = cur.fetchall()[0][0]
        # print('ss:',sss)
        con.commit()
    cur.close()
    con.close()

def fillEastWindCode():
    edf = pd.read_excel("c:/Users/env/Documents/全部美股.xlsx")
    uds=[]
    for ed in edf.itertuples():
        #windCode	stockName	mkt	symbol
        windCode=ed.windCode
        stockName=ed.stockName
        symbol=str(ed.symbol)
        uds.append([windCode,symbol])
    usql="update spider.usa_stockcode set windCode=%s where eastcode=%s "

    con, cur = dbutls.getConnect()
    cur.executemany(usql,uds)
    con.commit()
    cur.close()
    con.close()


def addAmOTCStocks():
    con, cur = dbutls.getConnect()
    edf = pd.read_excel("c:/Users/env/Documents/美股OTC.xlsx")
    uds=[]
    addDatas=[]
    for ord,ed in enumerate(edf.itertuples()):
        #证券代码	证券简称	股票代码	公司英文名称	公司中文名称	"总市值1 [交易日期] 最新收盘日 [币种] 原始币种 [单位] 元↓"	exchange	bbgcode	ticker
        windCode=ed.证券代码
        stockName=ed.证券简称
        if pd.isnull(stockName):
            continue
        symbol=ed.股票代码
        fullName=ed.公司中文名称
        englishName=ed.公司英文名称
        exchange=ed.exchange
        bbgCode=ed.bbgcode

        ticker = str(bbgCode).replace("Equity", "").strip()
        sid = Tuts.md5("%s" % (windCode))
        addDatas.append(
            [sid, windCode, symbol, ticker, bbgCode, stockName, fullName, englishName, ord, datetime.datetime.now(),
             None,exchange])
        uds.append([windCode,symbol])

    print(len(addDatas))
    ts = """   INSERT INTO config.{}(	id, windcode, symbol, ticker, bbgcode, shortname, fullname, englishname, ord, inserttime, status,exchange)
    	VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)   """.format(
        "otcstocks")
    print("ts:", ts)
    cur.execute("delete from config.otcstocks")
    cur.executemany(ts, addDatas)
    con.commit()
    cur.close()
    con.close()


def addAmExtStocks():
    #美股退市
    con, cur = dbutls.getConnect()
    edf = pd.read_excel("c:/Users/env/Documents/美股退市.xlsx")
    uds=[]
    addDatas=[]
    for ord,ed in enumerate(edf.itertuples()):
        #证券代码	证券简称	股票代码	公司英文名称	公司中文名称	"总市值1 [交易日期] 最新收盘日 [币种] 原始币种 [单位] 元↓"	exchange	bbgcode	ticker
        windCode=ed.证券代码
        stockName=ed.证券简称
        if pd.isnull(stockName):
            continue
        symbol=ed.股票代码
        fullName=ed.公司中文名称
        englishName=ed.公司英文名称
        exchange=ed.exchange
        bbgCode=symbol

        ticker = str(bbgCode).replace("Equity", "").strip()
        sid = Tuts.md5("%s" % (windCode))
        addDatas.append(
            [sid, windCode, symbol, ticker, bbgCode, stockName, fullName, englishName, ord, datetime.datetime.now(),
             None,exchange])
        uds.append([windCode,symbol])

    print(len(addDatas))
    ts = """   INSERT INTO config.exitusstocks(	id, windcode, symbol, ticker, bbgcode, shortname, fullname, englishname, ord, inserttime, status,exchange)
    	VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)   """.format(
        "otcstocks")
    print("ts:", ts)
    cur.execute("delete from config.exitusstocks")
    cur.executemany(ts, addDatas)
    con.commit()
    cur.close()
    con.close()


def addAmETFStocks():
    #美股退市
    con, cur = dbutls.getConnect()
    edf = pd.read_excel("c:/Users/env/Documents/美国ETF.xlsx")
    uds=[]
    addDatas=[]
    for ord,ed in enumerate(edf.itertuples()):
        #证券代码	证券简称	股票代码	公司英文名称	公司中文名称	"总市值1 [交易日期] 最新收盘日 [币种] 原始币种 [单位] 元↓"	exchange	bbgcode	ticker
        windCode=ed.证券代码
        symbol=str(windCode).split(".")[0]
        stockName=ed.证券名称
        addDatas.append(
            [windCode ,symbol,stockName])
    print(len(addDatas))
    ts = """   INSERT INTO spider.east_usetfstocks(	windcode, 	symbol, stockname) values( %s, %s, %s)   """
    print("ts:", ts)
    cur.execute("delete from spider.east_usetfstocks")
    cur.executemany(ts, addDatas)
    con.commit()
    cur.close()
    con.close()


def addEastAmETFStocks():
    #美股退市
    con, cur = dbutls.getConnect()
    edf = pd.read_excel("c:/Users/env/Documents/已退市美股.xlsx")
    uds=[]
    addDatas=[]
    for ord,ed in enumerate(edf.itertuples()):
        #证券代码	证券简称	股票代码	公司英文名称	公司中文名称	"总市值1 [交易日期] 最新收盘日 [币种] 原始币种 [单位] 元↓"	exchange	bbgcode	ticker
        windCode=ed.证券代码
        symbol=str(windCode).split(".")[0]
        stockName=ed.证券名称
        addDatas.append(
            [windCode ,symbol,stockName])
    print(len(addDatas))
    ts = """   INSERT INTO spider.east_exitusetfstocks(	windcode, 	symbol, stockname) values( %s, %s, %s)   """
    print("ts:", ts)
    cur.execute("delete from spider.east_exitusetfstocks")
    cur.executemany(ts, addDatas)
    con.commit()
    cur.close()
    con.close()


def addEastAmStocks():
    con, cur = dbutls.getConnect()
    edf = pd.read_excel("c:/Users/env/Documents/全部美股X.xlsx")
    uds=[]
    addDatas=[]
    for ord,ed in enumerate(edf.itertuples()):
        #windCode	stockName	mkt	symbol	上市地	ISIN代码	exchange
        windCode=ed.windCode
        stockName=ed.stockName
        if pd.isnull(stockName):
            continue
        symbol=ed.symbol
        exchange=ed.exchange

        addDatas.append(
            [ windCode, stockName,ed.mkt, symbol, ed.ISIN代码, exchange])
        uds.append([windCode,symbol])

    print(len(addDatas))
    ts = """   INSERT INTO spider.east_ussttocks(	windcode,stockName,mkt, symbol, ISIN ,exchange)
    	VALUES (%s, %s, %s, %s, %s,%s )   """
    print("ts:", ts)
    cur.execute("delete from spider.east_ussttocks")
    cur.executemany(ts, addDatas)
    con.commit()
    cur.close()
    con.close()


def clearLabled():
    con, cur = dbutls.getConnect()
    cur.execute("delete  from business.summary_comments where flag is null ")
    cur.execute("delete  from business.summary_like where flag is null  ")
    cur.execute("alter sequence business.seq_comment restart with 1;")
    con.commit()
    cur.close()
    con.close()



def findLabledUsers():
    alycon,alycur = linux_dbutls.getConnect()
    con, cur = dbutls.getConnect()
    q_lbP="select userid,content,operadate from userlog where ( content like '%:P' or  content like '%:N') order by operadate asc " #点赞数据
    pDF=pd.read_sql(q_lbP,alycon)
    alycur.close()
    alycon.close()
    up_labled_users=[]
    pDF=tools_utils.dfColumUpper(pDF)
    for pdt in pDF.itertuples():
        userid=pdt.USERID
        if pd.isnull(userid):
            continue
        content=pdt.CONTENT
        contents = content.split(":")
        rid=contents[0]
        Labled=contents[1]
        operadate=pdt.OPERADATE
        up_labled_users.append([userid,operadate,rid])
    print("up_labled_users:",len(up_labled_users))
    cur.executemany("update    business.summary_like set userid=%s,updatetime=%s where sid=%s",up_labled_users)
    print("summary_like",cur.rowcount)
    cur.executemany("update    business.summary_comments set userid=%s,updatetime=%s where sid=%s",up_labled_users)
    print("summary_comments",cur.rowcount)
    con.commit()
    cur.close()
    con.close()


def copyLabled(website):
    from InvestCopilot_App.models.business.likeMode import likeMode
    with dbmongo.Mongo(website) as mdb:
        qdb = mdb.db
        qsets=qdb.list_collection_names()
        dblist = []
        for dbsets in qsets:
            # print("dataBase:",dataBase)
            dblist.append({"$unionWith": dbsets})
        querys = {
            # "tickers": {"$in":windCodes},
        }
        projection = {
            "_id": 0,"id": 1,"label": 1,"labelText": 1,"publishOn":1,
        }
        sort = {"publishOn": -1}  # 1表示升序，-1表示降序
        # dblist.append({"$match": querys})  # 过滤符合条件的文档
        # dblist.append({"$sort": sort})  # 根据排序条件排序
        # dblist.append({"$skip": skip})  # 跳过文档
        # dblist.append({"$limit": pageSize})  # 限制结果数量
        dblist.append({"$project": projection})  # 筛选要显示的字段
        print("dblist:", dblist)
        contentsSets = list(qdb.collection_name.aggregate(dblist))
        sups=[]
        for s in contentsSets:
            sup={}
            if "label" in s:
                sup["id"] = s["id"]
                sup['label']=s['label']
            if "labelText" in s:
                sup["id"] = s["id"]
                sup['labelText']=s['labelText']
            if len(sup)>0:
                sups.append(sup)

        if len(sups)>0:
            for sup in sups:
                if "labelText" not in sup:
                    labelText=""
                else:
                    labelText = sup['labelText']
                if "label" not in sup:
                    label=""
                else:
                    label = sup['label']
                if label=="":
                    continue
                #业绩会议 与翻译统一编号处理问题。
                likeMode().addLike(sup["id"],"1967",label)#点赞
                if labelText is None or str(labelText).strip()=="":
                    continue
                likeMode().addComments(sup["id"],"1967",labelText)#点赞
def impReutersCodes():
    con, cur = dbutls.getConnect()
    edf = pd.read_excel(r"D:\demand\mys\starmine\stocks_gics_multiple_market_121206.xlsx",sheet_name='us',index_col=None)
    uds=[]
    edf=edf.replace({"NULL":None,np.NAN:None})
    print("edf:",edf.columns)
    addDatas=edf.values.tolist()
    print(addDatas[0])
    #Id	SecCode	Cusip	ISIN	Name_	issuename	RIC	Ticker	Sedol	ExchCode	CntryCode	gicssectorname	gicsindustryname	gicssubindustrynamme
    ts="INSERT INTO config.reuters_usstocks2 " \
    "(id,  seccode, cusip, isin, stockname, issuename, ric, ticker, sedol, exchcode, cntrycode, gicssectorname, gicsindustryname, gicssubindustrynamme) " \
    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # for ord,ed in enumerate(edf.itertuples()):
    #     #windCode	stockName	mkt	symbol	上市地	ISIN代码	exchange
    #     windCode=ed.windCode
    #     stockName=ed.stockName
    #     if pd.isnull(stockName):
    #         continue
    #     symbol=ed.symbol
    #     exchange=ed.exchange
    #     addDatas.append(
    #         [ windCode, stockName,ed.mkt, symbol, ed.ISIN代码, exchange])
    #     uds.append([windCode,symbol])
    #
    # print(len(addDatas))
    # ts = """   INSERT INTO spider.east_ussttocks(	windcode,stockName,mkt, symbol, ISIN ,exchange)
    # 	VALUES (%s, %s, %s, %s, %s,%s )   """
    # print("ts:", ts)
    # cur.execute("delete from spider.east_ussttocks")
    cur.executemany(ts, addDatas)
    con.commit()
    cur.close()
    con.close()
"""
delete from  tusers t where userid ='2s000' ;
delete from  userrole t where userid ='200s0' ;
delete from  portfolio.user_portfolio  where userid ='200s0' ;
delete from  portfolio.user_portfolio_list upl  where userid ='2s000' ;
delete from  factortemplate f where templateno  in ( select templateno  from  factorrluser f   where userid ='2s000' )
delete from  factorrluser f   where userid ='20s00' ;
delete from  business.summary_like sl where userid ='20s00' ;
delete from  business.summary_comments sc  where userid ='200s0' ;
"""
if __name__ == '__main__':
    # copyStockPoolData()#将mongodb表转 config scheme下表
    # impReutersCodes()#路透社代码 明博提供
    # copyAmStocks()
    # copyStockInfo()#股票代码 stockinfo
    # copyWorkDays()#工作日
    # fmtpinyin()#美股公司 拼音搜索
    # sqlserver()
    # copyUserInfo() #复制用户信息
    # copyPortfolio() #复制用户组合
    #组合序列重置
    # copyTemplate() ##指标模板迁移 先迁移组合 组合关联模板

    # fillEastWindCode()
    # addEastAmStocks()
    # addAmOTCStocks()
    # addAmExtStocks()
    # addAmETFStocks()
    # addEastAmETFStocks()
    # clearLabled()#需要清除数据 重置序列
    # copyLabled("website") #点评 点赞数据迁移
    # copyLabled("news")#点评 点赞 数据迁移
    # findLabledUsers()#关联点评所属用户
    pass
    #https://pypi.tuna.tsinghua.edu.cn/simple
    #AI重要性
    #AI
    #修改 cao 和 实习生 的用户id
    """
    
    update tusers set userid='034'  where userid ='1982';
    update companyuser  set userid ='034'  where userid ='1982';
    update userrole set userid ='034'  where userid ='1982';
    update business.callmanger set cuserid ='034'  where cuserid ='1982';

    update tusers set userid='2046'  where userid ='2047';
    update companyuser  set userid ='2046'  where userid ='2047';
    update userrole set userid ='2046'  where userid ='2047';
    update business.callmanger set cuserid ='2046'  where cuserid ='2047';

    update business.summary_like set userid='2046'  where userid ='2047';
    update business.summary_like  set userid ='034'  where userid ='1982';
    update business.summary_comments set userid='2046'  where userid ='2047';
    update business.summary_comments  set userid ='034'  where userid ='1982';
    """
    #update business.callmanger set cuserid='2046'  where cuserid ='2047';
    #update business.callmanger  set cuserid ='034'  where cuserid ='1982';  --051 1967

