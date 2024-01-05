__author__ = 'Robby'

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

import pandas as pd
import sys
import datetime
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache
from django.conf import settings as settings


# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 *10

class BaseCacheUtils():
    # 是否启用缓存

    # 缓存时长

    # 缓存对象类型

    # 缓存key规则

    def __init__(self, idFix='', cacheSeconds=30 * 60):
        self.idFix = idFix
        self.cacheSeconds = cacheSeconds
        pass

    def cacheIdFix(idFix):
        pass

    def setValue(self, key, value, seconds=(30 * 60)):
        print(self.idFix)
        print(self.cacheSeconds)

        if self.cacheSeconds is None:
            cache.set(key, value, seconds)
        else:
            cache.set(key, value, self.cacheSeconds)

        print(cache.get(key))

    def getValue(self, key):
        return cache.get(key)

    def delKey(self, key):
        cache.delete(key)

    def getTableaMaxDataDF(self, schemaTableName):
        """
        查询需要缓存的表数据,最新日期数据。
        """
        tableDF = pd.DataFrame()
        try:
            q_tableName = 'SELECT * FROM {} T  WHERE T.TRADEDATE =(SELECT MAX(TRADEDATE) FROM {})'.format(
                schemaTableName, schemaTableName)
            tableDF = dbutils.getPDQuery(q_tableName)
            # cache.set(schemaTableName, tableDF)
            # cacheSize = round(sys.getsizeof(tableDF) / (1024 * 1024), 2)
        except Exception as ex:
            Logger.errLineNo(msg="getTableaMaxDataDF 缓存表[%s]异常" % schemaTableName)

        return tableDF

    def getTableALLDataDF(self, schemaTableName):
        """
        查询需要缓存的表数据，基础数据
        """
        tableDF = pd.DataFrame()
        try:
            q_tableName = 'SELECT * FROM {} T '.format(schemaTableName)
            tableDF=dbutils.getPDQuery(q_tableName)
        except Exception as ex:
            Logger.errLineNo(msg="getTableALLDataDF 缓存表[%s]异常" % schemaTableName)
        return tableDF

    def flushCacheData(self):
        """
        刷新缓存数据
        """
        from InvestCopilot_App.models.toolsutils.ResultData import ResultData
        resultData = ResultData()
        try:
            begin=datetime.datetime.now()
            #清空表缓存
            # delTableCache()
            # 基础数据
            baseResult =self.cacheBaseTables()
            # 指标数据
            factorResult = self.cacheFactorTables()

            con=dbutils.getDBConnect()
            q_cacheTable="select count(1) as count from cachetable t where t.cachetype ='1' or t.cachetype='2' "
            allDF = pd.read_sql(q_cacheTable,con)
            con.close()
            allCount =allDF['COUNT'].values[0]
            #成功
            success=baseResult.success+factorResult.success
            #失败
            fail=baseResult.fail+factorResult.fail
            #success.append([tableName,size,CACHEUTILS_UPDATE_SECENDS,datetime.datetime.now(),'成功'])
            columns=['缓存编号',"大小",'有效期',"创建时间","处理状态"]
            tableFail = pd.DataFrame(fail, columns=columns)
            tableSuccess = pd.DataFrame(success, columns=columns)
            failSize=len(fail)
            successSize=len(success)
            from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
            import InvestCopilot_App.sendemail as sendemail
            tableFail = tools_utils.dataFrameToTableHtml(tableFail)
            tableSuccess = tools_utils.dataFrameToTableHtml(tableSuccess)

            if allCount==(failSize+successSize):
                isTrue=True
            else:
                isTrue=False

            title = "刷新缓存数据[%s]:总数[%d]失败[%d]成功[%d],总耗时[%d](秒)" % \
                    (isTrue,allCount,failSize, successSize,((datetime.datetime.now()) - begin).seconds)

            content = "<hr>执行失败记录(" + str(failSize) + ")：<br>" + tableFail + "<hr>执行成功记录(" + str(
                successSize) + ")：<br>" + tableSuccess
            #sendemail.sendSimpleEmail('robby.xia@baichuaninv.com', 'robby.xia', title, content)

        except Exception as ex:
            resultData.errorData(errorMsg="刷新缓存数据异常")
            Logger.errLineNo(msg='刷新缓存数据异常')

        return resultData

    def getCacheTableNewData(self, schemaTableName,reload=False):
        """
        从缓存中获取表数据，缓存中为None，从数据库中获取
        """
        tableData = None
        try:
            # 缓存异常处理
            tableData = cache.get("NEW_TABLE_"+schemaTableName)
        except Exception as ex:
            Logger.errLineNo()
        try:
            if tableData is None or  tableData.empty:
                reload=True

            if reload:
                # 缓存数据为空，从数据库获取
                tableDF = self.getTableaMaxDataDF(schemaTableName)
                tableData=tableDF
                # size = round(sys.getsizeof(tableDF) / (1024 * 1024), 2)
                # size = str(size) + "M"
                cache.set("NEW_TABLE_"+schemaTableName, tableDF, CACHEUTILS_UPDATE_SECENDS)  # 48小时
                # self.syncCacheTableLog("TABLE_"+schemaTableName, size, CACHEUTILS_UPDATE_SECENDS, datetime.datetime.now())
        except Exception as ex:
            Logger.errLineNo()

        return tableData

    def getCacheTableAllData(self, schemaTableName,reload=False):
        """
        从缓存中获取表数据，缓存中为None，从数据库中获取
        """
        tableData = None
        try:
            # 缓存异常处理
            tableData = cache.get("ALL_TABLE_" + schemaTableName)

        except Exception as ex:
            Logger.errLineNo()
        try:
            if tableData is None or tableData.empty:
                reload=True

            if reload:
                # 缓存数据为空，从数据库获取
                tableDF = self.getTableALLDataDF(schemaTableName)
                tableData = tableDF
                # size = round(sys.getsizeof(tableDF) / (1024 * 1024), 2)
                # size = str(size) + "M"
                cache.set("ALL_TABLE_" + schemaTableName, tableDF, CACHEUTILS_UPDATE_SECENDS)  # 48小时
                # self.syncCacheTableLog("TABLE_" + schemaTableName, size, CACHEUTILS_UPDATE_SECENDS, datetime.datetime.now())
        except Exception as ex:
            Logger.errLineNo()

        return tableData

    def cacheBaseTables(self):
        """
        缓存基础表表数据
        """
        try:
            from InvestCopilot_App.models.toolsutils.ResultData import ResultData
            resultData = ResultData()
            success=[]
            fail=[]
            # 指标数据缓存
            q_cacheTable = "select * from cacheTable t where t.cacheType=1"
            factorTableDF = dbutils.getPDQuery(q_cacheTable)
            for idx, tableInfo in factorTableDF.iterrows():
                tableName = tableInfo.TABLENAME
                queryType = tableInfo.QUERYTYPE
                try:
                    Logger.info('准备装载[%s]缓存数据' % tableName)
                    print('准备装载[%s]缓存数据' % tableName)
                    begin = datetime.datetime.now()
                    if 'MAX' == queryType:
                        tableDF = self.getCacheTableNewData(tableName,reload=True)
                    elif 'ALL' == queryType :
                        tableDF = self.getCacheTableAllData(tableName,reload=True)
                    else:
                        continue

                    sizeByte =sys.getsizeof(tableDF)
                    sizeKb = round(sizeByte / (1024), 2) #kb
                    if sizeKb==0:
                        #缓存数据为空，判失败
                        fail.append([tableName,sizeKb,CACHEUTILS_UPDATE_SECENDS,datetime.datetime.now(),'大小为0kb'])
                        continue

                    size=getSizeOf(tableDF)
                    # cache.set("TABLE_"+tableName, tableDF, CACHEUTILS_UPDATE_SECENDS)  # 48小时
                    now=datetime.datetime.now()
                    validTime=now + datetime.timedelta(seconds=CACHEUTILS_UPDATE_SECENDS)
                    self.syncCacheTableLog("TABLE_"+tableName, size, validTime, now)
                    end = datetime.datetime.now()
                    Logger.info('装载[%s]缓存数据完毕,大小[%s]，耗时[%d]MS' % (tableName, size, (end - begin).seconds * 1000))
                    print('装载[%s]缓存数据完毕,大小[%s]，耗时[%d]MS' % (tableName, str(size), (end - begin).seconds * 1000))
                    success.append([tableName,size,CACHEUTILS_UPDATE_SECENDS,datetime.datetime.now(),'成功'])
                except Exception as ex1:
                    Logger.error('加载缓存基础表表数据异常')
                    Logger.errLineNo()
                    fail.append([tableName,0,CACHEUTILS_UPDATE_SECENDS,datetime.datetime.now(),ex1])

        except Exception as ex2:
            resultData.errorData(errorMsg='加载缓存基础表表数据异常')
            Logger.error('加载缓存基础表表数据异常')
            Logger.errLineNo()

        resultData.success=success
        resultData.fail=fail
        return resultData

    def cacheFactorTables(self):
        """
        缓存指标表数据
        """
        try:
            from InvestCopilot_App.models.toolsutils.ResultData import ResultData
            resultData = ResultData()
            success=[]
            fail=[]
            # 指标数据缓存
            q_cacheTable = "select * from cacheTable t where t.cacheType=2"
            factorTableDF = dbutils.getPDQuery(q_cacheTable)
            for idx, tableInfo in factorTableDF.iterrows():
                tableName = tableInfo.TABLENAME
                queryType = tableInfo.QUERYTYPE
                try:
                    Logger.info('准备装载[%s]缓存数据' % tableName)
                    print('准备装载[%s]缓存数据' % tableName)
                    begin = datetime.datetime.now()
                    if 'MAX' == queryType:
                        tableDF = self.getCacheTableNewData(tableName,reload=True)
                    elif 'ALL' == queryType :
                        tableDF = self.getCacheTableAllData(tableName,reload=True)
                    else:
                        continue

                    sizeByte =sys.getsizeof(tableDF)
                    sizeKb = round(sizeByte / (1024), 2) #kb
                    if sizeKb==0:
                        #缓存数据为空，判失败
                        fail.append([tableName,sizeKb,CACHEUTILS_UPDATE_SECENDS,datetime.datetime.now(),'大小为0kb'])
                        continue

                    size=getSizeOf(tableDF)
                    # cache.set("TABLE_"+tableName, tableDF, CACHEUTILS_UPDATE_SECENDS)  # 48小时
                    now=datetime.datetime.now()
                    validTime=now + datetime.timedelta(seconds=CACHEUTILS_UPDATE_SECENDS)
                    self.syncCacheTableLog("TABLE_"+tableName, size, validTime, now)
                    end = datetime.datetime.now()
                    Logger.info('装载[%s]缓存数据完毕,大小[%s]，耗时[%d]MS' % (tableName, size, (end - begin).seconds * 1000))
                    print('装载[%s]缓存数据完毕,大小[%s]，耗时[%d]MS' % (tableName, str(size), (end - begin).seconds * 1000))
                    #key,size
                    success.append([tableName,size,CACHEUTILS_UPDATE_SECENDS,datetime.datetime.now(),'成功'])
                except Exception as ex1:
                    fail.append([tableName,0,CACHEUTILS_UPDATE_SECENDS,datetime.datetime.now(),ex1])
                    Logger.error('缓存指标表数据异常')
                    Logger.errLineNo()

        except Exception as ex2:
            resultData.errorData(errorMsg='缓存指标表数据异常')
            Logger.error('缓存指标表数据异常')
            Logger.errLineNo()

        resultData.success=success
        resultData.fail=fail
        return resultData

    def syncCacheTableLog(self, cachekey, cachesize, validTime, createtime):
        # 记录数据库缓存日志
        if settings.DBTYPE =='Oracle':
            i_cachelog = """
                insert into cachelog ( cachekey, cachesize, validTime, createtime) values (:v_cachekey, :v_cachesize, :v_validTime, :v_createtime)
            """
            d_cachelog = """
                delete from  cachelog where cachekey=:cachekey
            """
        elif settings.DBTYPE =='postgresql':
            i_cachelog = """
                insert into cachelog ( cachekey, cachesize, validTime, createtime) values (%s, %s, %s, %s)
            """
            d_cachelog = """
                delete from  cachelog where cachekey=%s
            """
        from InvestCopilot_App.models.toolsutils.ResultData import ResultData
        resultData = ResultData()
        try:
            cacheLog = [cachekey, cachesize, validTime, createtime]
            con, cur = dbutils.getConnect()
            cur.execute(d_cachelog, [cachekey])
            cur.execute(i_cachelog, cacheLog)
            con.commit()
            cur.close()
            con.close()
        except Exception as ex:
            Logger.error("记录缓存日志异常")
            Logger.errLineNo()
            resultData.errorData(errorMsg='记录缓存日志异常')
        finally:
            try:
                cur.close()
                con.close()
            except:
                pass

        return resultData


    def clearKeyByTime(self):
        rs =cache.keys("*")
        now = datetime.datetime.now()
        for key in rs :
            valiTime = cache.ttl(key)
            if valiTime is None:
                #永久有效
                continue
            else:
                #当前时间加有效期时间，单位秒
                nowT =now + datetime.timedelta(seconds=valiTime)

            afterT=now+datetime.timedelta(seconds=CACHEUTILS_UPDATE_SECENDS)
            nowD=nowT.strftime("%Y%m%d")
            afterD=afterT.strftime("%Y%m%d")
            if nowD < afterD:
                msg = "key[%s]已过期，失效时间[%s]，key失效时间[%s]" % (key,afterT,nowT)
                Logger.info(msg)
                cache.delete(key)

def getSizeOf(data):
    try:
        kb=round(float(sys.getsizeof(data)/1024),2)
        if int(kb/1024)==0:
            size = str(kb)+"KB"
        else:
            size = str(round(kb/1024, 2))+"M"
        return size
    except:
        pass
    return "0kb"
def flushCacheData():
    "刷新缓存数据"
    c = BaseCacheUtils()
    resultData = c.flushCacheData()
    return resultData


def syncCacheLog():
    """
    将缓存数据日志同步到数据库方便监控
    获取缓存日志：缓存key，缓存大小，缓存有效期
    """
    # 同步缓存数据
    i_cachelog = """
        insert into cachelog ( cachekey, cachesize, validTime, createtime) values (:v_cachekey, :v_cachesize, :v_validTime, :v_createtime)
    """
    d_cachelog = """
        delete from  cachelog
    """
    from InvestCopilot_App.models.toolsutils.ResultData import ResultData
    resultData = ResultData()
    try:
        #sql查询缓存
        querySqlKey = cache.keys("SQL_*")
        #基础表缓存
        # q_cachetable="SELECT TABLENAME FROM CACHETABLE"
        # con=dbutils.getDBConnect()
        # cachetableDF = pd.read_sql(q_cachetable,con)
        #["TABLE_"+tableName for tableName in cachetableDF['TABLENAME'].values.tolist()]
        tableKey = cache.keys("TABLE_*")
        keys = querySqlKey + tableKey
        # print('keys:',keys)
        cacheLog = []
        con, cur = dbutils.getConnect()
        for key in keys:
            # if key.find('django.') > -1:
            #     continue
            d=cache.get(key)
            if d is None:
                continue
            size=getSizeOf(d)
            valiTime = cache.ttl(key)
            now = datetime.datetime.now()
            if valiTime is None:
                #永久有效
                nowT =now + datetime.timedelta(seconds=100000000)
            else:
                #当前时间加有效期时间，单位秒
                nowT =now + datetime.timedelta(seconds=valiTime)
            cacheLog.append([key, size, nowT, datetime.datetime.now()])

        cur.execute(d_cachelog)
        cur.executemany(i_cachelog, cacheLog)
        con.commit()
        cur.close()
        con.close()
    except Exception as ex:
        try:
            con.rollback()
        except:
            pass
        Logger.errLineNo(msg="获取缓存日志异常")
        resultData.errorData(errorMsg='获取缓存日志异常')
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass

    return resultData



def delTableCache():
    #清空表缓存
    count = cache.delete_pattern("TABLE_*")
    return count


if __name__ == '__main__':
    # cacheUtis = BaseCacheUtils()
    # flushCacheData()
    # syncCacheLog()
    # import os
    # os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")

    # data= cacheUtis.getTableDataDF('NEWDATA.QUALITYFACTOR_FINAL')
    # cache.set('NEWDATA.QUALITYFACTOR_FINAL',data,60*10)
    # cache.set('NEWDATA.QUALITYFACTOR_FINAL1',data,60*20)
    # dataDF = cacheUtis.getCacheBaseTables('NEWDATA.ANALYSTDATA')
    # print(dataDF)
    # cacheUtis.cacheFactorTables()
    # print(cache.get('NEWDATA.ANALYSTDATA'))
    # print(cache.__dict__)
    # print(dir(cache))
    # pds = pd.DataFrame()
    # print(pds)
    # print(pds.empty)
    # flushCacheData()
    c = BaseCacheUtils()
    c.clearKeyByTime()
    pass
