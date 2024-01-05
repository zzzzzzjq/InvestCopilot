import traceback
import datetime
import pandas as pd
import collections
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils import dbmongo
Logger = logger_utils.LoggerUtils()

news_class_dt =collections.OrderedDict({"Industry trends": "行业趋势", "Earnings": "公司业绩公告", "Company event": "公司事件",
                 "Analyst opinion": "分析师报告","Policy news": "政策新闻","Market fluctuations":"市场波动","Company performance": "公司业绩",}) #"Company performance": "公司业绩",

news_titleTag_zh= [
    {"titleTag": "ai_software", "titleName": "AI软件"},
                   {"titleTag": "ai_hardware", "titleName": "AI硬件"},
                   {"titleTag": "bitcoin", "titleName": "比特币"},
                   {"titleTag": "saas", "titleName": "SAAS"},
                   # {"titleTag": "us_consumer", "titleName": "美国消费"},
                   {"titleTag": "us_consumer_discretionary", "titleName": "美国可选消费"},
                   {"titleTag": "us_internet", "titleName": "美国互联网"},
                   {"titleTag": "global_ev", "titleName": "全球电动车"},
                   {"titleTag": "solar_energy", "titleName": "光伏新能源"},

                   {"titleTag": "chinese_consumer", "titleName": "中国消费"},
                   {"titleTag": "chinese_cost_effective_consumer", "titleName": "中国性价比消费"},
                   {"titleTag": "chinese_internet", "titleName": "中国互联网"},
                   {"titleTag": "chinese_travel", "titleName": "中国出行"},
                   {"titleTag": "weight_loss_drugs", "titleName": "减肥药"},
]
news_titleTag_en = [{"titleTag": "ai_software", "titleName": "AI software"},
                  {"titleTag": "ai_hardware", "titleName": "AI hardware"},
                  {"titleTag": "bitcoin", "titleName": "bitcoin"},
                  {"titleTag": "saas", "titleName": "SAAS"},
                  # {"titleTag": "us_consumer", "titleName": "us consumer"},
                  {"titleTag": "us_consumer_discretionary", "titleName": "US Consumer Discretionary"},
                  {"titleTag": "us_internet", "titleName": "US Internet"},
                   {"titleTag": "global_ev", "titleName": "global_ev"},
                   {"titleTag": "solar_energy", "titleName": "solar_energy"},

                  {"titleTag": "chinese_consumer", "titleName": "CH consumer"},
                   {"titleTag": "chinese_cost_effective_consumer", "titleName": "CH CostEffectiveConsumer"},
                  {"titleTag": "chinese_internet", "titleName": "CH Internet"},
                  {"titleTag": "chinese_travel", "titleName": "US Travel"},
                  {"titleTag": "weight_loss_drugs", "titleName": "weight LossRrugInfo"},

                    ]
#那几个主题的阈值帮我调整到 ai hardware 0.743 / bitcoin 0.756 / saas 0.748 / ai software 0.745@Robby 
news_total_title_gt_dt={"ai_software": 0.755,
                     "us_consumer": 0.716,
                     "ai_hardware": 0.763,
                     "bitcoin": 0.756,
                     "saas": 0.752}
def getInfomationDataBase(sumaryFlag="0"):
    rest = ResultData()
    rest.information_datas = []
    try:
        information_datas = []
        with dbmongo.Mongo("StockPool") as md:
            mydb = md.db
            summarySourceSet = mydb["summarySource"]
            summarySource = summarySourceSet.find_one({"id": "information_datas"}, {"_id": 0})
            if summarySource is not None:
                summarySourceVs = summarySource['value']
                for sv in summarySourceVs:
                    if str(sumaryFlag) == "-1":
                        information_datas.append(sv)
                    else:
                        if str(sv['summary']) == str(sumaryFlag):
                            information_datas.append(sv)
            rest.information_datas = information_datas
    except:
        Logger.errLineNo(msg="getInfomationDataBase error")
        rest.errorData(errorCode="")
        print(traceback.format_exc())
    return rest


def getStockInfo():
    dbNames={"stocklist":"USD","hkStocks":"HKD","chStocks":"CNY","ksStocks":"KRW","twStocks":"TWD"}
    allStockDict={}
    with dbmongo.Mongo("StockPool") as md:
        mydb = md.db
        for dbName,unit in dbNames.items():
            dbset = mydb[dbName]
            dbdata=dbset.find({},{"_id":0})
            for d in dbdata:
                d['source']=dbName
                d['unit']=unit
                allStockDict[d['windCode']]=d
    return allStockDict


def getStockInfoByWindCodes(windCodes=[]):
        rst = ResultData()
        if isinstance(windCodes,str):
            windCodes=[windCodes]
        allStockDict=getStockInfo()
        rtdata=[]
        for wc in windCodes:
            # if wc in stockCodes:
            # ncode.add(wc)
            if wc in allStockDict:
                winfo = allStockDict[wc]
                if winfo['source'] =='stocklist':
                    fata={"windCode": wc, "symbol": winfo['symbol'],"unit": winfo['unit']}
                    if "zhName" not in winfo:
                        fata['zhName']=winfo["bbgName"]
                    else:
                        fata['zhName'] = wc
                    if "enName" not in winfo:
                        fata['enName']=winfo["bbgName"]
                    else:
                        fata['enName'] = wc
                    rtdata.append(fata)
                else:
                    enName=""
                    if not pd.isnull(winfo['englishName']):
                        enName=winfo['englishName']
                    rtdata.append({"windCode":wc,"symbol":winfo['symbol'],"zhName":winfo['shortName'],"enName":enName, "unit": winfo['unit']})
        rst.data=rtdata
        return rst

def getWindCodeInfo(tickerId):
    tickerInfo=[]
    with dbmongo.Mongo("StockPool") as md:
        mydb = md.db
        # 需要获取最新的
        Reviewset = mydb["stocklist"]
        tickerIds=[tickerId]
        if isinstance(tickerId,list):
            tickerIds=tickerId
        ReviewDBIds = Reviewset.find({'symbol': {"$in":tickerIds}}, {"_id": 0})  # 0不显示，1：显示； 查询数据
        for rd in ReviewDBIds:
            tickerInfo.append(dict(rd))
        if len(tickerInfo)==1:
            return tickerInfo[0]
    return tickerInfo

class mgdbdata_utils():

    def getSycStockPool(self, userIds=[],filterUsers=[]):
        rest = ResultData()
        rest.fsymbols = []
        try:
            fsymbols = []
            if len(userIds) == 0:
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    userstocksSet = mydb["userstocks"]
                    userStocks = userstocksSet.find({"disabled": {"$exists": False}}, {"_id": 0})
                    for u in userStocks:
                        if u['userId'] in filterUsers:
                            continue
                        symbols = u['stocks']
                        fsymbols.extend(symbols)  # 用户订阅的股票池列表
                # 系统默认设定的股票
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    stocksSet = mydb["stocks"]
                    defaultStocks = stocksSet.find({"stockTypes": "Default"}, {"_id": 0})
                    for u in defaultStocks:
                        symbols = u['stocks']
                        fsymbols.extend(symbols)  # 用户订阅的股票池列表
                # 板块分类股票
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    plateCodesSet = mydb["plateCodes"]
                    plateCodesStocks = plateCodesSet.find({"disabled": {"$exists": False}}, {"_id": 0})
                    for u in plateCodesStocks:
                        symbols = u['windCodes']
                        fsymbols.extend(symbols)
                fsymbols = list(set(fsymbols))
                rest.fsymbols = fsymbols
            else:
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    userstocksSet = mydb["userstocks"]
                    querys = {}
                    querys["userId"] = {"$in": userIds}
                    userStocks = userstocksSet.find(querys, {"_id": 0})
                    for u in userStocks:
                        symbols = u['stocks']
                        fsymbols.extend(symbols)  # 用户订阅的股票池列表
                    fsymbols = list(set(fsymbols))
                    rest.fsymbols = fsymbols

            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                stocklistset = mydb["stocklist"]
                stocks = stocklistset.find({}, {"_id": 0, 'windCode': 1, 'ord': 1})
                ordCodes = [x for x in stocks if x['windCode'] in rest.fsymbols]
                # order 排序
                # ordCodes =sorted(ordCodes,key=lambda x:x['ord'])
            ordCodes = sorted(ordCodes, key=lambda x: x['ord'])
            hisfsymbols=rest.fsymbols
            rest.fsymbols=[s['windCode'] for s in ordCodes]
            ordOutherCodes = list(set(hisfsymbols) - set(rest.fsymbols))
            if len(ordOutherCodes)>0:
                rest.fsymbols.extend(ordOutherCodes)
            #股票代码按市值排序
        except:
            Logger.errLineNo(msg="getStockPool error")
            rest.errorData(errorCode="")
            print(traceback.format_exc())
        return rest

    def getDBSetInfo(self,dataType):
        # 通过前缀识别数据源
        dbRst = getInfomationDataBase(sumaryFlag="1")
        if not dbRst.errorFlag:
            return dbRst
        infomationDBs = dbRst.information_datas
        for idb in infomationDBs:
            idprefix = idb['idprefix']
            if  idprefix == dataType:
                database = idb['website']
                dbset = idb['dbset']
                return database,dbset
        return None,None


    def getMaxDataGroupWindCode(self,website,dbSet):
        codeTranscriptsMaxDay_dt={}
        try:
            if website is None or dbSet is None:
                return codeTranscriptsMaxDay_dt
            if website =="" or dbSet =="":
                return codeTranscriptsMaxDay_dt
            with dbmongo.Mongo(website) as mdb:
                mydb=mdb.db
                collection=mydb[dbSet]
                pipeline = [
                    {
                        "$group": {
                            "_id": "$windCode",
                            "maxPublishOn": {"$max": "$publishOn"}
                        }
                    }
                ]
                result = list(collection.aggregate(pipeline))
                for entry in result:
                    wind_code = entry["_id"]
                    max_publish_on = entry["maxPublishOn"]
                    codeTranscriptsMaxDay_dt[wind_code]={'windCode': wind_code, 'publishOn':max_publish_on,}
        except Exception as e:
            Logger.errLineNo(msg="getMaxDataGroupWindCode error")

        return codeTranscriptsMaxDay_dt

    def getStockPool(self,userIds=[],trackFlag="1",preDays=-2,nextDays=0,filterUsers=[],dataType=None):
        rest = ResultData()
        rest.fsymbols = [] #系统默认股票
        rest.trackCodes=[] #跟踪股票
        rest.allCodes=[] #
        try:
            #系统股票池 用户自定义股票+系统默认股票+板块
            rtsp=self.getSycStockPool(userIds=userIds,filterUsers=filterUsers)
            sysCodes = rtsp.fsymbols
            rest.fsymbols=sysCodes
            allCodes=sysCodes.copy()
            #SPX500
            spx500 = []
            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                # 需要获取最新的
                SPXSet = mydb["stocks"]
                SPXStocks = SPXSet.find({"stockTypes": {"$in": ['SPX']}}, {"_id": 0})
                for u in SPXStocks:
                    symbols = u['stocks']
                    spx500.extend(symbols)  # 用户订阅的股票池列表
            #跟踪股票 跟踪股票需要包含系统股票池
            if trackFlag=="1":
                qdrts = self.getQuarterlyReport(preDays=preDays, nextDays=nextDays)
                # 跟踪出业绩股票  股票池+标普500
                ckAllCodes = list(set(spx500 + sysCodes))
                codeDataMaxDay_dt={}
                if dataType in ["sAts_"]:
                    # 过滤已经抓到的出了业绩的代码
                    database,dbset=self.getDBSetInfo(dataType)
                    codeDataMaxDay_dt=self.getMaxDataGroupWindCode(database,dbset)
                trackCodes = []
                preDtime=datetime.datetime.now() + datetime.timedelta(days=-2)
                itrackCodes=[]
                for qd in qdrts:
                    ckqd=qd['windCode']
                    if ckqd in ckAllCodes:#在标普500 + 自选股内
                        itrackCodes.append(ckqd)
                #过滤已经抓取了报告
                for ckqd in itrackCodes:
                    if ckqd in codeDataMaxDay_dt:
                        codeDataMax = codeDataMaxDay_dt[ckqd]
                        if "publishOn" in codeDataMax:
                            publishOn = codeDataMax['publishOn']
                            pdtime = datetime.datetime.strptime(publishOn, '%Y-%m-%d %H:%M:%S')
                            if pdtime > preDtime:
                                continue
                    trackCodes.append(ckqd)
                #按市值排序
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    stocklistset = mydb["stocklist"]
                    stocks = stocklistset.find({}, {"_id": 0, 'windCode': 1, 'ord': 1})
                    ordCodes = [x for x in stocks if x['windCode'] in trackCodes]
                    # order 排序
                    # ordCodes =sorted(ordCodes,key=lambda x:x['ord'])
                ordCodes = sorted(ordCodes, key=lambda x: x['ord'])
                trackfsymbols = [s['windCode'] for s in ordCodes]
                ordOutherCodes = list(set(trackCodes) - set(trackfsymbols))
                if len(ordOutherCodes) > 0:
                    trackfsymbols.extend(ordOutherCodes)
                allCodes.extend(trackfsymbols)
                rest.trackCodes = trackfsymbols
            allCodes = list(set(allCodes))
            rest.allCodes = allCodes
        except:
            Logger.errLineNo(msg="getStockPoolNews error")
            rest.errorData(errorMsg="getStockPoolNews error")
        return rest


    def getPressReleases(self,preDays=-3,nextDays=0):
        # 跟踪标普500出业绩的公司
        rest=self.getStockPool(trackFlag="1",preDays=preDays,nextDays=nextDays)
        needCodes=rest.allCodes
        return needCodes

    def getQuarterlyReport(self,preDays=-3,nextDays=1):
        nt = datetime.datetime.now()
        preDay = (nt + datetime.timedelta(days=preDays)).strftime("%Y-%m-%d")
        nextDay = (nt + datetime.timedelta(days=nextDays)).strftime("%Y-%m-%d")
        qdrts = []
        with dbmongo.Mongo("common") as md:
            mydb = md.db
            # 需要获取最新的
            qReportSet = mydb["quarterlyReport"]
            SPXStocks = qReportSet.find({"qReport": {"$gte": preDay, "$lte": nextDay}}, {"_id": 0})
            for u in SPXStocks:
                qdrts.append(u)
        return qdrts

    def getCodeQuarterDay(self,windCodes):
        qdrts={}
        with dbmongo.Mongo("common") as md:
            mydb = md.db
            # 需要获取最新的
            qReportSet = mydb["quarterlyReport"]
            SPXStocks = qReportSet.find({"windCode": {"$in": windCodes}}, {"_id": 0})
            for u in SPXStocks:
                qdrts[u['windCode']] =u  # 用户订阅的股票池列表
        return qdrts

    def getEarningsForecastData(self,preDays=-1,nextDays=7):
        #展示一周出业绩的公司
        rst=ResultData()
        try:
            efWindCodes = self.getPressReleases(preDays=preDays,nextDays=nextDays)
            quarterlyReports =self.getCodeQuarterDay(efWindCodes)
            fdatas=[]
            qReports=[]
            for r,v in quarterlyReports.items():
                # print("r:",r,v)
                v['bbgName']=str(v['bbgName']).title()
                wdname = str(v['windName'])
                if wdname.isalpha():
                    v['windName'] =wdname
                else:
                    cks=str(v['windName'])
                    # cks = cks[0:cks.find("(")]
                    v['windName']=cks.title()
                v['qReport']=v['qReport'] + " 00:00:00"
                qReports.append(v['qReport'])
                fdatas.append(v)
            fdatas=sorted(fdatas,key=lambda x: x['qReport'])
            qReports=sorted(set(qReports))
            redatas={}
            for rd in qReports:
                rds=[]
                for fd in fdatas:
                    if rd ==fd['qReport']:
                        rds.append(fd)
                redatas[rd]=rds
            # for r,v in redatas.items():
            #     print(r,v)
            #按日期排序分组展示
            rst.data= {"data":redatas,"periodId":qReports}
        except:
            errorMsg = "抱歉，获取数据异常，请稍后处理！"
            Logger.errLineNo(msg=errorMsg)
            rst.errorData(errorMsg=errorMsg)
        return rst


if __name__ == '__main__':
    mut=mgdbdata_utils()
    s =mut.getEarningsForecastData(-7,14)
    print(s.toDict())