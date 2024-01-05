
import datetime
import pandas as pd

from InvestCopilot_App.models.toolsutils import dbutils as dbutils

MARKET_LIST=['.BJ','.SH','.SZ','.HK']
MARKET_CH_LIST=['.BJ','.SH','.SZ']

def getstock_windcode(stockcode):
    # A股，上证和深圳地域判断
    if stockcode[:1] == '6':
        s_con_windcode = stockcode + '.SH'
    else:
        # 深交所
        if stockcode[:1] in ['0', '3']:
            s_con_windcode = stockcode + '.SZ'
        else:  # 北交所
            s_con_windcode = stockcode + '.BJ'

    return s_con_windcode


def getWorkDate(tradeDay='', num=0):
    """
    获取系统工作日
    """
    # sqlQuery = "select max(tradedate) from workday t where t.tradedate<=:newDay"
    if tradeDay == '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    sql_tradeDate = "select tradedate  from config.workday d where d.seq = (select max(seq) + " + str(
        num) + " from config.workday where tradedate <=%s)"

    con,cur=dbutils.getConnect()
    cur.execute(sql_tradeDate, [tradeDay])
    rs=cur.fetchall()
    if len(rs)==0:
        return ""
    workdate = rs[0][0]
    cur.close()
    con.close()
    return workdate



def getHkWorkDay(tradeDay=''):
    """
    获取港股交易日
    """
    if tradeDay == '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    con, cur = dbutils.getConnect()
    q_hkworkday = "select workday from config.hkworkday t" \
                  " where t.workday=(select max(workday) from config.hkworkday where workday<=%s)"
    cur.execute(q_hkworkday,[tradeDay])
    rs = cur.fetchall()
    tradeDate = rs[0][0]
    cur.close()
    con.close()
    return tradeDate



def getUSAWorkDay(tradeDay=''):
    """
    获美股交易日
    """
    if tradeDay == '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    con, cur = dbutils.getConnect()
    q_hkworkday = "select workday from config.usaworkday t" \
                  " where t.workday=(select max(workday) from config.usaworkday where workday<=%s)"
    cur.execute(q_hkworkday,[tradeDay])
    rs = cur.fetchall()
    tradeDate = rs[0][0]
    cur.close()
    con.close()
    return tradeDate

def isHkWorkDay(tradeDay=''):
    """
    是否为港股交易日
    """
    if tradeDay == '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    con,cur=dbutils.getConnect()
    q_hkworkday="select * from config.hkworkday t where t.workday='%s'" % tradeDay
    cur.execute(q_hkworkday)
    rs=cur.fetchall()
    cur.close()
    con.close()
    if len(rs)==0:
        return False
    else:
        return True


def getAMWorkDayByTrade(tradeDay='',num=0):
    """
    是否为美股交易日
    """
    if tradeDay == '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    ascending = False
    if num <0:
        num = abs(num)
        q_amworkday = "select workday from config.usaworkday t where t.workday< %s order by workday desc limit %s"
        ascending = True
    elif num>=1:
        q_amworkday = "select workday from config.usaworkday t where t.workday >%s order by workday asc limit %s"
    else:
        num=1
        q_amworkday = "select min(workday) as workday from config.usaworkday t where t.workday>=%s limit %s"
    con, cur = dbutils.getConnect()
    dataDF=pd.read_sql(q_amworkday,con,params=[tradeDay,num])
    cur.close()
    con.close()
    dataDF=dataDF.sort_values(['WORKDAY'],ascending=ascending)
    if not dataDF.empty:
        data=dataDF.iloc[0]
        tradeDay=data.WORKDAY
    return tradeDay

def isUSAWorkDay(tradeDay=''):
    """
    是否为美股交易日
    """
    if tradeDay == '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    con, cur = dbutils.getConnect()
    #usaworkday 美股交易日记录日期
    q_hkworkday = "select * from config.usaworkday t where t.workday='%s'" % tradeDay
    cur.execute(q_hkworkday)
    rs = cur.fetchall()
    cur.close()
    con.close()
    if len(rs) >0 :
        return True
    else:
        return False


def isWorkDay():
    """
    是否为A股交易日
    """
    nowDate = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
    workDay = getWorkDate()
    if nowDate == workDay:
        return True
    else:
        return False
