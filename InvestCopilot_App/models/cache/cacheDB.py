# coding=utf-8
__author__ = 'Rocky'
"""
    数据库表数据缓存
"""

import sys

sys.path.append("../../")
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import InvestCopilot_App.models.cache.BaseCacheUtils as base_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
import pandas as pd
import sys
import datetime
import traceback

Logger = logger_utils.LoggerUtils()
base_cache = base_utils.BaseCacheUtils()

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10

KEYFIX = 'SQL_'


##
##数据库表【单表】缓存 开始
##
# 个股信息数据
def getStockInfoDF(reload=False):
    """
    tableName:NEWDATA.ASHAREDESCRIPTION
    股票基本信息(个股信息)缓存获取，所有数据
    """
    #return getCacheTableAllDataDF(schemaTableName="NEWDATA.ASHAREDESCRIPTION", reload=reload)
    return getCacheTableAllDataDF(schemaTableName="CONFIG.STOCKINFO", reload=reload)


# 个股信息数据
def getStockIntroductionDF(reload=False):
    """
    tableName:NEWDATA.ASHAREDESCRIPTION
    股票信息介绍（和公司、股东、注册地相关）缓存获取，所有数据
    """
    return getCacheTableAllDataDF(schemaTableName="NEWDATA.ASHAREINTRODUCTION", reload=reload)


def getStockRankDF(reload=False):
    """
    tableName:stockrank
    个股行业，总排名,最新数据
    """
    return getCacheTableNewData(schemaTableName="STOCKRANK", reload=reload)


def getStockInduDF(reload=False):
    """
    行业缓存
    """
    return getCacheTableNewData(schemaTableName="NEWDATA.MSINDUS", reload=reload)


def getCitics12DF(reload=False):
    """
    tableName:citics12
    数据库表[citics12]，一二级行业映射关系
    """
    return getCacheTableAllDataDF(schemaTableName="CITICS12", reload=reload)


def getSysDictDF(reload=False):
    """
    tableName:sysdictionary
    数据库表[sysdictionary]，数据字典
    """
    return getCacheTableAllDataDF(schemaTableName="SYSDICTIONARY", reload=reload)


##
##数据库表【单表】缓存 结束
##


##
##数据库表【联查】缓存 开始
##

def SQL_A_stockInfoDF(reload=False):
    return getStockInfoCache(flage='A', reload=reload)

def SQL_ALL_stockInfoDF(reload=False):
    return getStockInfoCache(flage='ALL', reload=reload)

def SQL_ZZETF_stockInfoDF(reload=False):
    return getStockInfoCache(flage='ZZETF', reload=reload)


def getStockInfoCache(flage='ALL', reload=False):
        "flage=sh  etf  hk  gzqh  usa lof kzz sz"
        # 股票选择器缓存处理
        qKey = KEYFIX + flage + '_stockInfoDF'
        stockInfoDF = cache.get(qKey)
        isLock = False
        if stockInfoDF is None:
            #_lock=threading.RLock()
            #_lock.acquire()
            isLock = True
            reload = True
            stockInfoDF = cache.get(qKey)
            if stockInfoDF is not None:
                reload = False
        if reload:
            if True:
                # 添加股票选择列表至缓存中
                if 'A' == flage:  # A股
                    # sh+sz
                    sql_query = "select t.stockcode,t.windcode ,eastcode,t.stockname as STOCKNAME ,t.area,stocktype,T.pinyin " \
                                "from  config.stockinfo t where t.stocktype='sh' or t.stocktype='sz' or t.stocktype='bj' and disabled is null "
                elif 'H' == flage:  # 港股
                    sql_query = "select t.stockcode,t.windcode ,eastcode,t.stockname as STOCKNAME ,t.area,stocktype,T.pinyin " \
                                "from  config.stockinfo t where t.area='HK'  and disabled is null "
                elif 'U' == flage:  # 美股
                    sql_query = "select t.stockcode,t.windcode ,eastcode,t.stockname as STOCKNAME ,t.area,stocktype,T.pinyin " \
                                "from  config.stockinfo t where t.area='AM'  and disabled is null "
                elif 'IDX' == flage:  # 指数期货
                    sql_query = "select t.stockcode,t.windcode ,eastcode,t.stockname as STOCKNAME ,t.area,stocktype,T.pinyin " \
                                "from  config.stockinfo t where t.stocktype='idx'  and disabled is null "
                elif 'ALL' == flage:
                    sql_query = "select t.stockcode,t.windcode ,eastcode,t.stockname as STOCKNAME ,t.area,stocktype,T.pinyin from  config.stockinfo t  where  disabled is null and stocktype not in ('kzz','etf','lof')"
                #alter table stockinfo add disabled char(1);
                elif 'ZZETF' == flage:
                    sql_query = "select t.stockcode,t.windcode ,eastcode,t.stockname as STOCKNAME ,t.area,stocktype,T.pinyin " \
                                "from  config.stockinfo t where t.stocktype='kzz' or t.stocktype='etf' or t.stocktype='lof'  and disabled is null  "
                # elif 'KCB' == flage:#科创板
                #     sql_query = "select s_info_code as stockcode,s_info_windcode as  WINDCODE,s_info_windcode as eastcode,STOCKNAME,'CH' as area,STOCKNAME||' '||s_info_pinyin as name,'sh' as stocktype,s_info_pinyin pinyin from newdata.asharedescription t" \
                #                 " where s_info_listboardname='科创板' and substr(s_info_code,1,1) <> 'A' and s_info_compname is not null and s_info_listdate is not null  "
                else:
                    sql_query = "select t.stockcode,t.windcode ,eastcode,t.stockname as STOCKNAME ,t.area,stocktype,T.pinyin " \
                                "from  config.stockinfo t where t.stocktype='%s'  and disabled is null  " % (flage)
                try:
                    # Logger.debug("sql_query:%s"%sql_query)
                    stockInfoDF=dbutils.getPDQuery(sql_query)
                    #分红状态个股
                    # div_stock_dict=getStockDividendDate(reload=reload)
                    # def _fun_divdendCode(r):
                    #     if r.WINDCODE in div_stock_dict:
                    #        return div_stock_dict[r.WINDCODE]+" "+str(r.PINYIN)
                    #     else:
                    #         return r.STOCKNAME + " " +str( r.PINYIN)
                    # stockInfoDF['NAME']=stockInfoDF[['WINDCODE','STOCKNAME','PINYIN']].apply(lambda r: _fun_divdendCode(r),axis=1 )
                    #'STOCKCODE', 'WINDCODE', 'EASTCODE', 'STOCKNAME', 'AREA', 'NAME', 'STOCKTYPE', 'SEARCH'
                    stockInfoDF = stockInfoDF.fillna('')
                    stockInfoDF=tools_utils.dfColumUpper(stockInfoDF)
                    # stockInfoDF = stockInfoDF.drop(['PINYIN'],axis=1)
                    ##分红状态个股
                    # stockInfoDF['STOCKNAME'] = stockInfoDF[['WINDCODE','STOCKNAME']].apply(lambda x: div_stock_dict[x.WINDCODE] if x.WINDCODE in div_stock_dict else x.STOCKNAME,axis=1 )
                    stockInfoDF.columns=stockInfoDF.columns.map(lambda x:x.upper())
                    stockInfoDF['SEARCH'] = stockInfoDF['STOCKCODE'] + stockInfoDF['STOCKNAME'].apply(lambda x: x.replace(' ', '')) + stockInfoDF['PINYIN']
                    stockInfoDF['SEARCH'] =  stockInfoDF['SEARCH'].apply(lambda x: str(x).lower())
                    cache.set(qKey, stockInfoDF, CACHEUTILS_UPDATE_SECENDS)
                except Exception as ex:
                    Logger.error(traceback.format_exc())
                    raise ex
        if isLock:
            pass#_lock.release()
        return stockInfoDF
#
# def SQL_ALLSTOCK_stockInfoDF(reload=False):
#     return getStockInfoAllCache(reload=reload)
#
#
# def getStockInfoAllCache(reload=False):
#     # 获得所有股票的缓存信息
#     key = "SQL_ALLSTOCK_stockInfoDF"
#     stockInfoDF = cache.get(key)
#     if stockInfoDF is None or stockInfoDF.empty:
#         reload = True
#         # select s_info_code as stockcode ,s_info_windcode as stockwindcode,s_info_name, s_info_name||' '||s_info_pinyin as name
#         # from NEWDATA.AShareDescription  where  substr(s_info_code,1,1)<>'A'
#         # union all select substr(stockcode,0,instr(stockcode,'.')-1),stockcode as stockwindcode, stockname as s_info_name,stockname as name from Overseasstock t
#         # where t.tradedate =(select max(tradedate) from overseasstock)
#         # union all select substr(s.equitycode,0,6) as stockcode , s.equitycode as stockwindcode,s.equityname as s_info_name ,equityname as name from newdata.bond_etf s
#     if reload:
#         # 添加股票选择列表至缓存中,stockcode代码都去后缀(AShareDescription：取A股代码；bond_etf取转债代码，overseasstock表取美股代码，hkstockcode表取港股代码(东财5位代码转万德4位代码）
#         sql_query = """
#         select t.stockcode,t.windcode as stockwindcode,t.stockname as s_info_name ,t.stocktype,t.area,t.pinyin as name from stockinfo t
#         """
#
#         con = dbutils.getDBConnect()
#         stockInfoDF = pd.read_sql(sql_query, con)
#         con.close()
#
#         cache.set(key, stockInfoDF, CACHEUTILS_UPDATE_SECENDS)
#     return stockInfoDF
#

# print(getStockInfoAllCache(reload=True))
# def SQL_HK_stockInfoDF(reload=False):
#     return getHKStockInfoCache(reload=reload)
#
#
# def getHKStockInfoCache(reload=False):
#     # 股票选择器缓存处理
#     stockInfoDF = cache.get("SQL_HKStockInfoDF")
#     if stockInfoDF is None or stockInfoDF.empty:
#         reload = True
#     if reload:
#         # 添加股票选择列表至缓存中
#         sql_query = "SELECT WINDSTOCKCODE as STOCKWINDCODE,EASTSTOCKCODE,STOCKNAME FROM HKSTOCKCODEDICT"
#         con = dbutils.getDBConnect()
#         stockInfoDF = pd.read_sql(sql_query, con)
#         con.close()
#
#         cache.set("SQL_HKStockInfoDF", stockInfoDF, CACHEUTILS_UPDATE_SECENDS)
#
#     return stockInfoDF


def SQL_industryType(reload=False):
    return getIndustryTypeCache(reload=reload)


def getIndustryTypeCache(reload=False):
    """
    获取中信一级、二级、三级行业信息
    """
    allIndusDF = cache.get('SQL_industryType')
    if allIndusDF is None or allIndusDF.empty:
        reload = True
    if reload:
        sql_query = """
        select distinct t.s_info_windcode,substr(s_info_name,1, length(s_info_name)-1)||'一级)' as s_info_name  from newdata.aindexmemberscitics t ,windcustomcode b  where t.s_info_windcode=b.s_info_windcode and  t.cur_sign='1'
        union all
        select distinct t.s_info_windcode, substr(s_info_name,1, length(s_info_name)-1)||'二级)' as s_info_name  from newdata.aindexmemberscitics2 t ,windcustomcode b  where t.s_info_windcode=b.s_info_windcode and  t.cur_sign='1'
        union all
        select distinct t.s_info_windcode, substr(s_info_name,1, length(s_info_name)-1)||'三级)' as s_info_name  from newdata.aindexmemberscitics3 t ,windcustomcode b  where t.s_info_windcode=b.s_info_windcode and  t.cur_sign='1'
       """
        # union all
        # select distinct t.msinduscode as s_info_windcode,t.msindusname||'(多极)' as s_info_name from newdata.MsIndus t where t.tradedate = (select max(tradedate) from newdata.MsIndus)

        con = dbutils.getDBConnect()
        allIndusDF = pd.read_sql(sql_query, con)
        con.close()
        # 替换中信二字 @季博需求
        allIndusDF['S_INFO_NAME'] = allIndusDF['S_INFO_NAME'].apply(lambda x: str(x).replace("中信", ""))
        cache.set('SQL_industryType', allIndusDF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return allIndusDF


def SQL_industry1(reload=False):
    return getIndustry1(reload=reload)


def getIndustry1(reload=False):
    """
    获取中信一级行业与股票映射关系
    """
    indus1DF = cache.get('SQL_industry1')
    if indus1DF is None or indus1DF.empty:
        reload = True
    if reload:
        # s_info_windcode 行业代码，s_con_windcode：wind股票代码 ，s_con_indate：调入时间
        sql_query = "select t.s_info_windcode,b.s_info_name, t.s_con_windcode as stockwindcode,t.s_con_indate  from newdata.aindexmemberscitics t ,windcustomcode b  where t.s_info_windcode=b.s_info_windcode and  t.cur_sign='1'"
        con = dbutils.getDBConnect()
        indus1DF = pd.read_sql(sql_query, con)
        con.close()
        # 替换中信二字 @季博需求
        indus1DF['S_INFO_NAME'] = indus1DF['S_INFO_NAME'].apply(lambda x: str(x).replace("(中信)", ""))
        cache.set('SQL_industry1', indus1DF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return indus1DF


def SQL_industry2(reload=False):
    return getIndustry2(reload=reload)


def getIndustry2(reload=False):
    """
    获取中信二级行业与股票映射关系
    """
    indus2DF = cache.get('SQL_industry2')
    if indus2DF is None or indus2DF.empty:
        reload = True
    if reload:
        # s_info_windcode 行业代码，s_con_windcode：wind股票代码 ，s_con_indate：调入时间
        sql_query = "select t.s_info_windcode,b.s_info_name, t.s_con_windcode as stockwindcode,t.s_con_indate  from newdata.aindexmemberscitics2 t ,windcustomcode b  where t.s_info_windcode=b.s_info_windcode and  t.cur_sign='1'"
        con = dbutils.getDBConnect()
        indus2DF = pd.read_sql(sql_query, con)
        con.close()
        # 替换中信二字 @季博需求
        indus2DF['S_INFO_NAME'] = indus2DF['S_INFO_NAME'].apply(lambda x: str(x).replace("(中信)", ""))
        cache.set('SQL_industry2', indus2DF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return indus2DF


def SQL_industry3(reload=False):
    return getIndustry3(reload=reload)


def getIndustry3(reload=False):
    """
    获取中信三级行业与股票映射关系
    """
    indus3DF = cache.get('SQL_industry3')
    if indus3DF is None or indus3DF.empty:
        reload = True
    if reload:
        # s_info_windcode 行业代码，s_con_windcode：wind股票代码 ，s_con_indate：调入时间
        sql_query = "select t.s_info_windcode,b.s_info_name, t.s_con_windcode as stockwindcode,t.s_con_indate  from newdata.aindexmemberscitics3 t ,windcustomcode b  where t.s_info_windcode=b.s_info_windcode and  t.cur_sign='1'"
        con = dbutils.getDBConnect()
        indus3DF = pd.read_sql(sql_query, con)
        con.close()
        # 替换中信二字 @季博需求
        indus3DF['S_INFO_NAME'] = indus3DF['S_INFO_NAME'].apply(lambda x: str(x).replace("(中信)", ""))
        cache.set('SQL_industry3', indus3DF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return indus3DF


def SQL_industryAll(reload=False):
    return getIndustryAll(reload=reload)


def getIndustryAll(reload=False):
    industryAllDF = cache.get('SQL_industryAll')
    if industryAllDF is None or industryAllDF.empty:
        reload = True
    if reload:
        industryAllDF = getIndustry1()
        industry2 = getIndustry2()
        industry3 = getIndustry3()
        industryAllDF = industryAllDF.append(industry2)
        industryAllDF = industryAllDF.append(industry3)
        cache.set('SQL_industryAll', industryAllDF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒

    return industryAllDF


def SQL_concept(reload=False):
    return getWindSecName(reload=reload)


def getWindSecName(reload=False):
    """
    最新概念，股票与概念关系
    """
    conceptDF = cache.get('SQL_concept')
    if conceptDF is None or conceptDF.empty:
        reload = True
    if reload:
        sql_query = "SELECT T.WIND_SEC_CODE, T.S_INFO_WINDCODE AS S_CON_WINDCODE, T.WIND_SEC_NAME ,T.ENTRY_DT FROM NEWDATA.ASHARECONSEPTION T " \
                    "WHERE ENTRY_DT <= (SELECT TO_CHAR(SYSDATE, 'YYYYMMDD') FROM DUAL)" \
                    "   AND (REMOVE_DT >= (SELECT TO_CHAR(SYSDATE, 'YYYYMMDD') FROM DUAL) OR  REMOVE_DT IS NULL)" \
                    "   ORDER BY ENTRY_DT DESC"

        con = dbutils.getDBConnect()
        conceptDF = pd.read_sql(sql_query, con)
        con.close()
        cache.set('SQL_concept', conceptDF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return conceptDF


def getWindConseption(reload=False):
    """
    最新概念，股票与概念关系
    """
    conceptDF = cache.get('SQL_conceptNew')
    if conceptDF is None or conceptDF.empty:
        reload = True
    if reload:
        sql_query = """
        select T.WIND_SEC_CODE, T.WIND_SEC_NAME  from NEWDATA.ASHARECONSEPTION T where t.cur_sign = '1' group by T.WIND_SEC_CODE, T.WIND_SEC_NAME
        """
        con = dbutils.getDBConnect()
        conceptDF = pd.read_sql(sql_query, con)
        con.close()
        cache.set('SQL_conceptNew', conceptDF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return conceptDF


def SQL_factorCellDF(reload=False):
    return getFactorCellDF(reload=reload)


def getFactorCellDF(reload=False):
    """
    指标信息缓存(factorcell)
    """
    factorDF = cache.get('FACTOR_factorCellDF')
    if factorDF is None or factorDF.empty:
        reload = True
    if reload:
        sqlStr = """
                SELECT UPPER(C.FACTORTYPE || C.FACTORNO) AS FIXFACTORTNAME,
                        to_char(C.FACTORNO,'FM999999999999') AS FACTORNO,
                        c.FACTORCLASS,c.FACTORTYPE,c.FACTORDESC,c.FACTORTABLE,
                        c.FACTORTNAME,c.FACTORORDER,c.REGXNUMBER,c.REGXTYPE,
                        c.FLOATSIZE,c.ENABLE,c.QMODE,'' as statementtype,
                        '' as reportperiod,
                        c.fdesc,
                        c.searchkey,
                        c.fview
                   from FACTORCELL C
                  WHERE C.ENABLE IS NULL
                  ORDER BY C.FACTORNO, C.FACTORORDER
        """
        factorDF=dbutils.getPDQuery(sqlStr)
        cache.set('FACTOR_factorCellDF', factorDF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return factorDF


def SQL_indexMenuDF(reload=False):
    return getIndexMenuDF(reload=reload)


def getIndexMenuDF(reload=False):
    """
    指标和目录的缓存
    """
    indexMenuDF = cache.get('FACTOR_indexMenuDF')
    if indexMenuDF is None or indexMenuDF.empty:
        reload = True
    if reload:
        sqlStr ="SELECT  DISTINCT t.menuid,t.menuname,t.menutype,tt.factorno,tt.ord FROM menufactor t INNER JOIN menufactorsconfig tt ON t.menuid = tt.menuid"
        indexMenuDF =dbutils.getPDQuery(sqlStr)
        cache.set('FACTOR_indexMenuDF', indexMenuDF, CACHEUTILS_UPDATE_SECENDS)
    return indexMenuDF

def SQL_historyTableConfigDF(reload=False):
    return getHistoryTableConfig(reload=reload)

def getHistoryTableConfig(reload=False):
    """
    历史表配置
    """
    hisPdData = cache.get('SQL_historyTableConfigDF')
    if hisPdData is None or hisPdData.empty:
        reload = True
    if reload:
        sqlStr = '''
        SELECT t.* FROM histableconfig t
        '''
        con = dbutils.getDBConnect()
        hisPdData = pd.read_sql(sqlStr,con=con)
        con.close()
        cache.set('SQL_historyTableConfigDF',hisPdData,CACHEUTILS_UPDATE_SECENDS)
    return hisPdData


def SQL_userStatusDF(reload=False):
    return getUserStatusDF(reload=reload)


def getUserStatusDF(reload=False):
    """
    用户状态缓存
    """
    userStatusDF = cache.get('SQL_userStatusDF')
    if userStatusDF is None or userStatusDF.empty:
        reload = True
    if reload:
        con = dbutils.getDBConnect()
        sqlStr = '''
        SELECT t.userid,t.userstatus
        FROM tusers t
        '''
        userStatusDF = pd.read_sql(sqlStr, con=con)
        userStatusDF.columns = userStatusDF.columns.str.upper()
        con.close()
        cache.set('SQL_userStatusDF', userStatusDF, CACHEUTILS_UPDATE_SECENDS)
    return userStatusDF


def SQL_userMenuIdDF(reload=False):
    return getUserMenuIdDF(reload=reload)


def getUserMenuIdDF(reload=False):
    """
    用户菜单id缓存
    """
    userMenuIdDF = cache.get('SQL_userMenuIdDF')
    if userMenuIdDF is None or userMenuIdDF.empty:
        reload = True
    if reload:
        con = dbutils.getDBConnect()
        sqlStr = '''
        SELECT b.USERID,a.MENUIDLIST
        FROM PRIVROLE a
        INNER JOIN USERROLE b ON a.ROLEID = b.ROLEID
        '''
        userMenuIdDF = pd.read_sql(sqlStr, con=con)
        userMenuIdDF.columns = userMenuIdDF.columns.str.upper()
        con.close()
        cache.set('SQL_userMenuIdDF', userMenuIdDF, CACHEUTILS_UPDATE_SECENDS)
    return userMenuIdDF


def SQL_userConfig_dt(reload=False):
    return getUserConfig_dt(reload=reload)


def getUserConfig_dt(reload=False):
    """
    用户基本参数
    """
    userCfg_dt = cache.get('SQL_userConfig_dt')
    if userCfg_dt is None or len(userCfg_dt)==0:
        reload = True
    if reload:
        con = dbutils.getDBConnect()
        sqlStr = '''
        select x.*  from (
        select u.userid,u.userrealname,u.usernickname,c.shortcompanyname,username,c.companyid,u.privilegeset
        from tusers u, companyuser cw, company c where u.userid =cw.userid  and cw.companyid =c.companyid  ) as x 
        '''
        userConfigDF = pd.read_sql(sqlStr, con=con)
        userConfigDF.columns = userConfigDF.columns.str.upper()
        con.close()
        userCfg_dt={}
        for i,d in userConfigDF.iterrows():
            userCfg_dt[d['USERID']]=dict(d)
        cache.set('SQL_userConfig_dt', userCfg_dt, CACHEUTILS_UPDATE_SECENDS)
    return userCfg_dt


def SQL_menuDF(reload=False):
    return getMenuDF(reload=reload)


def getMenuDF(reload=False):
    """
    所有的菜单缓存
    """
    menuDF = cache.get('SQL_menuDF')
    if menuDF is None or menuDF.empty:
        reload = True
    if reload:
        con = dbutils.getDBConnect()
        sqlStr = '''
        SELECT t.*
        FROM MENU t
        '''
        menuDF = pd.read_sql(sqlStr, con=con)
        menuDF.columns = menuDF.columns.str.upper()
        con.close()
        cache.set('SQL_menuDF', menuDF, CACHEUTILS_UPDATE_SECENDS)
    return menuDF


def SQL_apiDF(reload=False):
    return getApiDF(reload=reload)


def getApiDF(reload=False):
    """
    获取api配置的缓存
    """
    apiDF = cache.get('SQL_apiDF')
    if apiDF is None or apiDF.empty:
        reload = True
    if reload:
        con = dbutils.getDBConnect()
        sqlStr = '''
        SELECT t.*
        FROM APILIMIT t
        '''
        apiDF = pd.read_sql(sqlStr, con=con)
        con.close()
        cache.set('SQL_apiDF', apiDF, CACHEUTILS_UPDATE_SECENDS)
    return apiDF


# 指数
def SQL_IndexDF(reload=False):
    return getIndexCodeDF(reload=reload)


def getIndexCodeDF(reload=False):
    """
    获取指定指数代码数据
    000001.SH	上证综指，399001.SZ	深证成指，000010.SH	上证180，000016.SH	上证50，000300.SH	沪深300，
    000985.SH	中证全指(SH)，000985.CSI	中证全指，399005.SZ	中小板指，
    399006.SZ	创业板指，399101.SZ	中小板综，399102.SZ	创业板综，
    399317.SZ	国证A指，399905.SZ	中证500
    """
    apiDF = cache.get('SQL_IndexCodeDF')
    if apiDF is None or apiDF.empty:
        reload = True
    if reload:
        con = dbutils.getDBConnect()
        # '399905','000001','399006','399101','399317','000300','000010','399005','399102','000985','000016'
        sqlStr = '''select t.s_info_windcode,t.s_info_name,t.s_info_code from windcustomcode t where T.S_INFO_SECURITIESTYPES = 'S' AND
             t.s_info_code in ('399001','399905','000001','399006','399101','000300','000010','399005','399102','000016')
             order by t.s_info_code
        '''
        apiDF = pd.read_sql(sqlStr, con=con)
        con.close()
        cache.set('SQL_IndexCodeDF', apiDF, CACHEUTILS_UPDATE_SECENDS)
    return apiDF

def SQL_StockBaseInfoDataDF(reload=False):
    return getStockBaseInfoData(reload=reload)

def getStockBaseInfoData(reload=False):
    """
    获取个股基本信息的缓存
    """
    basePdData = cache.get('SQL_StockBaseInfoDataDF')
    if basePdData is None or basePdData.empty:
        reload = True
    if reload:
        con = dbutils.getDBConnect()
        sqlStr1 = '''
        select t.stockcode as 股票代码,t.column2 as 总收入,t.column5 as 总收入同比
        from newdata.factortable3 t
        '''
        data1 = pd.read_sql(sqlStr1,con=con)

        sqlStr2 = '''
        select t.stockcode as 股票代码,t.column12 as 净利润,t.column15 as 净利润同比
        from newdata.FactorTable1 t
        '''
        data2 = pd.read_sql(sqlStr2,con=con)

        sqlStr3 = '''
        select t.stockcode as 股票代码,t.column3 as 毛利率
        from newdata.FactorTable14 t
        '''
        data3 = pd.read_sql(sqlStr3,con=con)

        sqlStr4 = '''
        select t.stockcode as 股票代码,t.column10 as ROE
        from newdata.FactorTable22 t
        '''
        data4 = pd.read_sql(sqlStr4,con=con)

        sqlStr5 = '''
        select t.stockcode as 股票代码,t.column8 as 净利率
        from newdata.FactorTable17 t
        '''
        data5 = pd.read_sql(sqlStr5,con=con)

        sqlStr6 = '''
        select t.stockcode as 股票代码,t.column13 as 负债率
        from newdata.FactorTable10 t
        '''
        data6 = pd.read_sql(sqlStr6,con=con)

        pdData1 = pd.merge(data1,data2,how='inner',on='股票代码')
        pdData2 = pd.merge(pdData1,data3,how='inner',on='股票代码')
        pdData3 = pd.merge(pdData2,data4,how='inner',on='股票代码')
        pdData4 = pd.merge(pdData3,data5,how='inner',on='股票代码')
        pdData5 = pd.merge(pdData4,data6,how='inner',on='股票代码')
        basePdData = pdData5
        con.close()
        cache.set('SQL_StockBaseInfoDataDF',pdData5,CACHEUTILS_UPDATE_SECENDS)
    return basePdData

def getRealStockMarketDF(reload=False):
    """
    获取实时行情数据的缓存
    """
    stockMarketDF = cache.get('SQL_StockMarketDF')
    if stockMarketDF is None or stockMarketDF.empty:
        reload = True

    if reload:
        con = dbutils.getDBConnect()
        sqlStr = '''
        SELECT a.stockcode,
        a.stockcode as stockwindcode,
        a.stockcode as indexCode,
        to_char(a.nowprice, 'FM99990.00') AS nowprice,
        to_char(a.pctchange * 100, 'FM99990.00') AS pctchange
        FROM emminhq2 a
        '''
        stockMarketDF = pd.read_sql(sqlStr, con=con, index_col="INDEXCODE")
        con.close()
        cache.set('SQL_StockMarketDF', stockMarketDF, CACHEUTILS_UPDATE_SECENDS)
    return stockMarketDF


def getRealStockMarketByWindCode(stockWindCodes, reload=False):
    """
    获取实时行情数据的缓存
    """
    stockMarketDF = cache.get('SQL_StockMarketDF')
    if stockMarketDF is None or stockMarketDF.empty:
        reload = True

    if reload:
        stockMarketDF = getRealStockMarketDF(reload)

    if type(stockWindCodes) is list:
        stockWindCodes = [str(x).upper() for x in stockWindCodes]
    else:
        stockWindCodes = [str(stockWindCodes).upper()]

    stockMarketDF = stockMarketDF[stockMarketDF['STOCKWINDCODE'].isin(stockWindCodes)]
    if stockMarketDF.empty:
        sData = []
        for x in stockWindCodes:
            sData.append([x, "-", "-"])
            # {"STOCKWINDCODE":[x],"NOWPRICE":["-"],"PCTCHANGE":["-"]}
        rs = pd.DataFrame(sData, columns=["STOCKWINDCODE", "NOWPRICE", "PCTCHANGE"])
        return rs
    # 保持代码顺序
    stockMarketDF = stockMarketDF.sort_values(["STOCKWINDCODE"])
    rs = stockMarketDF
    return rs


def getRealStockMarketAllTimeDF(reload=False):
    """
    获取最新行情日期个股分时涨跌幅数据
    """
    stockMarketDF = cache.get('SQL_StockMarketAllTimeDF')
    if stockMarketDF is None or stockMarketDF.empty:
        reload = True

    if reload:
        con = dbutils.getDBConnect()
        sqlStr = '''SELECT T.STOCKCODE AS STOCKWINDCODE,T.TRADEDATE,SUBSTR(T.TRADETIME,0,4)||'00' AS TRADETIME,T.NOWPRICE,T.PCTCHANGE FROM EMMINHQ T WHERE T.TRADEDATE =(SELECT MAX(TRADEDATE) FROM EMMINHQ)'''
        stockMarketDF = pd.read_sql(sqlStr, con=con, index_col='STOCKWINDCODE')
        con.close()
        cache.set('SQL_StockMarketAllTimeDF', stockMarketDF, CACHEUTILS_UPDATE_SECENDS)

    return stockMarketDF


def updateRealStockMarket():
    try:
        getRealStockMarketDF(reload=True)
    except:
        Logger.errLineNo(msg="刷新实时行情缓存异常")
        return "error"
    return 'ok'


# 股价
def getStockPriceDF(reload=False):
    """
    股价
    """
    apiDF = cache.get('SQL_stockPriceDF')
    if apiDF is None or apiDF.empty:
        reload = True
    if reload:
        # 重新加载股价
        con, cur = dbutils.getConnect()
        # 当前日期往前推500个交易日
        newDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
        num = 10
        minDay = "select tradedate  from workday d" \
                 " where d.seq = (select max(seq) - " + str(
            num) + " from workday where tradedate <=(select max(tradedate) from workday t where t.tradedate<='" + newDay + "'))"
        rs = cur.execute(minDay)
        minDay = rs.fetchone()
        if minDay == None:
            minDay = '20160101'
        else:
            minDay = minDay[0]

        sqlStr = '''
        select stockcode,TRADEDATE, S_DQ_OPEN as OPEN1, S_DQ_CLOSE as CLOSE1, S_DQ_LOW as MIN1, S_DQ_HIGH as MAX1,  S_DQ_VOLUME/10000 as VOLUME
        from newdata.ashareeodprices_his where  tradedate>=:minDay
        '''
        apiDF = pd.read_sql(sqlStr, con=con, params={"minDay": minDay})
        cur.close()
        con.close()
        cache.set('SQL_stockPriceDF', apiDF, CACHEUTILS_UPDATE_SECENDS)
    return apiDF


##
##数据库表【联查】缓存 结束
##


def getCacheTableNewData(schemaTableName, reload=False):
    """
    从缓存中获取表数据，缓存中为None，从数据库中获取
    schemaTableName=schema.tableName
    """
    schemaTableName = str(schemaTableName).upper()
    return base_cache.getCacheTableNewData(schemaTableName=schemaTableName, reload=reload)


def getCacheTableAllDataDF(schemaTableName, reload=False):
    """
    查询需要缓存的表数据，基础数据
    获得缓存表数据，schemaTableName=schema.tableName
    """
    schemaTableName = str(schemaTableName).upper()
    return base_cache.getCacheTableAllData(schemaTableName=schemaTableName, reload=reload)


def delSQLCache():
    # 清空表sql缓存
    count = cache.delete_pattern(KEYFIX + "*")
    return count


def flushDBCache():
    """
    刷新sql查询缓存
    """
    # 添加依赖模块
    try:
        import datetime
        begin = datetime.datetime.now()
        keyDef = __import__("InvestCopilot_App.models.cache.cacheDB")
        py1 = "keyDef.models.cache.cacheDB"
        # 遍历sql查询方法
        syncMethod = [method for method in dir(eval(py1)) if method.find("SQL_") > -1]
        # print(syncMethod)
        allCount = len(syncMethod)
        sucess = []
        fail = []
        # 执行调用方法
        isTrue = True
        # 清理SQL_*缓存
        # keys = cache.keys("SQL_*")
        # keys =[key for key in keys if key.find('SQL_')>-1]
        # print('keys:',keys)
        for method in syncMethod:
            try:
                beginT = datetime.datetime.now()
                methodT = py1 + "." + method + "(reload=True)"
                # print("ttl1:",method,cache.ttl(method))
                # cache.delete(method)
                eval(methodT)
                # print("ttl2:",method,cache.ttl(method))
                sucess.append([method, "缓存成功", ((datetime.datetime.now()) - beginT).microseconds / 1000])
            except:
                fail.append([method, "缓存失败", ((datetime.datetime.now()) - beginT).microseconds / 1000])
                isTrue = False
                pass

        # 日志输出
        sucessCount = len(sucess)
        failCount = len(fail)
        import InvestCopilot_App.sendemail as sendemail
        from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils

        # 拼装邮件格式
        columns = ["缓存调用方法", "处理状态", "耗时(毫秒)"]
        tableFail = pd.DataFrame(fail, columns=columns)
        tableSuccess = pd.DataFrame(sucess, columns=columns)

        tableFail = tools_utils.dataFrameToTableHtml(tableFail)
        tableSuccess = tools_utils.dataFrameToTableHtml(tableSuccess)

        title = "刷新sql查询缓存报告[%s]:总数[%d]失败[%d]成功[%d],总耗时[%d](秒)" % (
            isTrue, allCount, failCount, sucessCount, ((datetime.datetime.now()) - begin).seconds)

        content = "<hr>执行失败记录(" + str(failCount) + ")：<br>" \
                  + tableFail + "<hr>执行成功记录(" + str(sucessCount) + ")：<br>" + tableSuccess
        #sendemail.sendSimpleEmail('robby.xia@baichuaninv.com', 'robby.xia', title, content)
    except Exception as ex:
        Logger.error("刷新sql查询缓存异常")
        Logger.errLineNo()


def flushOneDbCache(key):
    """
    刷新单个sql查询缓存
    """
    isSuccess = True
    try:
        # 执行调用方法
        keyDef = __import__("InvestCopilot_App.models.cache.cacheDB")
        py1 = "keyDef.models.cache.cacheDB"
        # 执行调用方法
        method = key
        methodT = py1 + "." + method + "(reload=True)"
        # cache.delete(method)
        pdData = eval(methodT)
        # 打log
        size = base_utils.getSizeOf(pdData)
        now = datetime.datetime.now()
        validTime = now + datetime.timedelta(seconds=CACHEUTILS_UPDATE_SECENDS)
        res = base_cache.syncCacheTableLog(method, size, validTime, now)
        if res.errorFlag is False:
            isSuccess = False
    except:
        Logger.errLineNo()
        isSuccess = False
    return isSuccess


def flushOneTableCache(key):
    """
    刷新单个表缓存
    """
    isSuccess = True
    try:
        tableName = str(key).replace('TABLE_', '')
        tableType = getCacheTableType(tableName)
        if tableType == 'MAX':  # 缓存最新数据的类型
            # cache.delete(key)
            getCacheTableNewData(schemaTableName=tableName, reload=True)
        elif tableType == 'ALL':  # 缓存最新数据的类型
            # cache.delete(key)
            getCacheTableAllDataDF(schemaTableName=tableName, reload=True)
    except Exception as ex:
        Logger.errLineNo()
        isSuccess = False
    return isSuccess


def getCacheTableType(tableName):
    """
    获取缓存表的类型
    """
    con = dbutils.getDBConnect()
    selectSql = '''
    SELECT * FROM cachetable t
    WHERE t.tablename = '%s'
    ''' % tableName
    data = pd.read_sql(selectSql, con=con)
    tableType = data['QUERYTYPE'].values.tolist()[0]
    con.close()
    return tableType


def getCacheAWorkDate(tradeDay='', num=0):
    """
    获取A股交易日：
    tradeDay：匹配交易日
    num:匹配交易日往前或往后推算N个交易日
    """
    # sqlQuery = "select max(tradedate) from workday t where t.tradedate<=:newDay"
    workDateAllDF = getCacheTableAllDataDF(schemaTableName='WORKDAY')
    # print(sys.getsizeof(workDateAllDF) / (1024 * 1024))
    if tradeDay == '':
        tradeDay = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")

    # sql_tradeDate = "select tradedate  from workday d where d.seq = (select max(seq) + " + str(
    #     num) + " from workday where tradedate <=:tradeDay)"
    # 通过交易日或自然日匹配大于等于 最小工作日
    workDateDF = workDateAllDF[workDateAllDF['TRADEDATE'] <= str(tradeDay)]
    workDateDF = workDateDF.sort_values(['TRADEDATE'], ascending=False)
    if workDateDF.empty: return ""
    tradeDayDFT = workDateDF.iloc[0]
    # 获取匹配最新工作日 序列
    seq = tradeDayDFT['SEQ']
    # 设置所有日期序列index
    seqIndex = workDateAllDF['SEQ'].values.tolist()
    workDateAllDF.index = seqIndex
    # 匹配工作日序列
    seq = seq + int(num)
    if not seq in seqIndex:
        return ''
    workdate = workDateAllDF.loc[seq]['TRADEDATE']
    # con = dbutils.getDBConnect()
    # cur = con.cursor()
    # con,cur=dbutils.getConnect()
    # cur.execute(sql_tradeDate, [tradeDay])
    # rs=cur.fetchall()
    # if len(rs)==0:
    #     return ""
    # workdate = rs[0][0]
    # cur.close()
    # con.close()
    return workdate


def getSizeOf(data):
    try:
        kb = round(float(sys.getsizeof(data) / 1024), 2)
        if int(kb / 1024) == 0:
            size = str(kb) + "KB"
        else:
            size = str(round(kb / 1024, 2)) + "M"
        return size
    except:
        pass
    return "0kb"

def SQL_FundTagsDF(reload=False):
    return getFundTagsDF(reload=reload)


def getFundTagsDF(reload=False):
    """
    所有的菜单缓存
    """
    FundTagsDF = cache.get('SQL_FundTagsDF')
    if FundTagsDF is None or FundTagsDF.empty:
        reload = True
    if reload:
        print("Run Reload Cache!")
        con = dbutils.getDBConnect()
        sqlStr = '''
        SELECT t.*
        FROM tfundtypedic t where t.FTYPE_DESCRIBE is not null
        '''
        FundTagsDF = pd.read_sql(sqlStr, con=con)
        con.close()
        cache.set('SQL_FundTagsDF', FundTagsDF, CACHEUTILS_UPDATE_SECENDS)
    return FundTagsDF

def getSelfTemplateColumnsDict(reload=False):
    # 获取自定义表字段映射匹配关系
    cacheKey="SQL_selfTemplatesDict"
    apiDF = cache.get(cacheKey)
    if apiDF is None or len(apiDF)==0:
        reload=True

    if reload:
        q_selfTemplate = "select * from fileselftemplates"
        con = dbutils.getDBConnect()
        selfDF = pd.read_sql(q_selfTemplate, con)
        con.close()
        apiDF = {}
        for idx, row in selfDF.iterrows():
            apiDF[row.EXCELCOLUMNS] = {"templateId": row.TEMPLATEID, 'templateName': row.TEMPLATENAME,
                                                 'tableName': row.TABLENAME, 'excelColumns': row.EXCELCOLUMNS,
                                                 'tableColumns': row.TABLECOLUMNS}

    cache.set(cacheKey,apiDF,CACHEUTILS_UPDATE_SECENDS)
    return apiDF


def initCacheData():
    getStockInfoDF()

if __name__ == '__main__':
    # ds = getCitics12DF()
    # ds = getUserConfig_dt()
    # userCfg_dt = cache.delete('SQL_userConfig_dt')
    userCfg_dt = cache.get('SQL_userConfig_dt')
    pass
