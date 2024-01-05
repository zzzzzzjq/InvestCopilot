import datetime

import InvestCopilot_App.models.cache.dataSynTools.connect as db


def getWorkDate(tradeDay='',num=0):
    # 获取A股交易日：非工作日时，获取的是前一工作日
    # tradeDay：匹配交易日
    # num:匹配交易日往前或往后推算N个交易日
    if tradeDay is '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")

    sql_tradeDate = "select tradedate  from workday d where d.seq = (select max(seq) + " + str(num) + " from workday where tradedate <=:tradeDay)"
    con=db.connect()
    cur=con.cursor()
    cur.execute(sql_tradeDate, [tradeDay])
    rs=cur.fetchall()
    cur.close()
    con.close()
    if len(rs)==0:
        return ""
    workdate = rs[0][0]
    return workdate

def isTradeTime():
    nowDate=datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")
    tradeDate = getWorkDate()
    if nowDate !=tradeDate:
        return False
    nowTime = datetime.datetime.strftime(datetime.datetime.now(),"%H%M")

    if ((nowTime >= '0930' and nowTime <= '1205' ) or (nowTime >= '1300' and nowTime <= '1615')):
        return True
    return False

if __name__ == '__main__':
    rs =getWorkDate(tradeDay='00001011')
    print(rs)