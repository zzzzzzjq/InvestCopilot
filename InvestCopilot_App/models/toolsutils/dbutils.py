__author__ = 'Robby'
"""
edit by Rocky 20020528
Add for postgresql Connect
"""

from django.conf import settings
import pandas as pd
from InvestCopilot_App.models.DBconnect.oraclepool import OraclePool
from InvestCopilot_App.models.DBconnect.pgdbpool import pgsqlPool
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils

Logger = logger_utils.LoggerUtils()


def dfColumUpper(fdf):
    fdf.columns = fdf.columns.map(lambda x: x.upper())
    return fdf

# 获得数据源和游标
def getConnect():
    try:
        if settings.DBTYPE =="postgresql":
            connection= pgsqlPool()
        else:
            connection=OraclePool()
        cursor=connection.cursor()
    except Exception as err:
        Logger.errLineNo()
        Logger.error('DBConnect Error!', err)
        return (['-2', '|', 'DBConnect Error!'])
    return connection, cursor


# 获得数据源
def getDBConnect():
    try:
        if settings.DBTYPE =="postgresql":
            connection= pgsqlPool()
        else:
            connection=OraclePool()
    except Exception as err:
        Logger.errLineNo()
        Logger.error('DBConnect Error!', err)
        return (['-2', '|', 'DBConnect Error!'])
    return connection


# 获得数据源
def read_sql(selectSql, con,params={}):
    try:
        if len(params)>0:
            data = pd.read_sql(selectSql, con=con,params=params)
            data=dfColumUpper(data)
        else:
            data = pd.read_sql(selectSql, con=con)
            data=dfColumUpper(data)
    except Exception as err:
        Logger.errLineNo()
        Logger.error('read_sql Error!', err)
        return (['-2', '|', 'read_sql Error!'])
    return data

 # 获得游标
def getDBCursor():
    try:
        if settings.DBTYPE =="postgresql":
            connection= pgsqlPool()
        else:
            connection=OraclePool()
        cursor=connection.cursor()
    except Exception as err:
        Logger.errLineNo()
        Logger.error('DBConnect Error!', err)
        return (['-2', '|', 'DBConnect Error!'])
    return cursor

def getPDQuery(sql_Query,upperColumn=True,fillNaN=None):
    """
    pandas 查询数据
    """
    try:
        con=getDBConnect()
        data =pd.read_sql(sql_Query,con=con)
        if fillNaN is not None:
            data=data.fillna(fillNaN)
        if upperColumn:
            data=dfColumUpper(data)
            return data
    except:
        Logger.errLineNo()
    finally:
        try:con.close()
        except:pass
    return pd.DataFrame()

def getPDQueryByParams(sql_Query,params,upperColumn=True,fillNaN=None):
    """
    pandas 查询数据
    """
    try:
        con=getDBConnect()
        data =pd.read_sql(sql_Query,con=con,params=params)
        if fillNaN is not None:
            data=data.fillna(fillNaN)
        if upperColumn:
            data=dfColumUpper(data)
        return data
    except:
        Logger.errLineNo()
    finally:
        try:con.close()
        except:pass
    return pd.DataFrame()

def getSeqNo():
    """
    获取序列编号
    """
    seq_no = '''
      SELECT SEQ_BATCHID.nextval FROM dual
    '''
    con ,cur = getConnect()
    res = cur.execute(seq_no)
    seqNo = res.fetchall()[0][0]
    cur.close()
    con.close()
    return seqNo

# tableName:需查询表名
# columnsList:需查询表字段名列表[column1,column2]
# whereDataDict:查询条件字典{column:value}
# return 返回DataFrame对象
def queryTableReturnDF(tableName, columnsList=[], whereDataDict={}):
    # 需要插入的columns
    if bool(columnsList):
        search_columns = columnsList
        search_columns_key = ",".join(search_columns)

    # 判断where是否有数据
    if len(whereDataDict.keys()) > 0:
        where_columns = list(whereDataDict.keys())
        where_value = [whereDataDict[k] for k in where_columns]
        where_columns_key = [k + "=:" + k for k in where_columns]
        where_columns_key = " and ".join(where_columns_key)
        fill_params = where_value

    try:
        # 构建修改sql语句 search TABLE SET CLO1=:COL1,COL2=:COL2 WHERE WHERE1=:WHERE1 AND WHERE2=:WHERE2
        if bool(columnsList) and len(whereDataDict.keys()) > 0:
            sql_search = "select {} from {} where {} ".format(search_columns_key, tableName, where_columns_key)
        elif bool(columnsList):
            sql_search = "select {} from {} ".format(search_columns_key, tableName)
        else:
            sql_search = "select {} from {} ".format('*', tableName)

        sql_search = sql_search.upper()

        if len(whereDataDict.keys()) > 0:
            Logger.debug("sql_search:%s ; where(%s)" % (sql_search, fill_params))
            # print("sql_search:%s ; where(%s)" % (sql_search, fill_params))
            return pd.read_sql(sql_search, con=getDBConnect(), params=fill_params)
        else:
            Logger.debug("sql_search:%s" % sql_search)
            # print("sql_search:%s" % (sql_search))
            return pd.read_sql(sql_search, con=getDBConnect())

    except Exception as ex:
        Logger.errLineNo()
        Logger.error(ex)


# tableName:插入数据表名
# insertDataDict:插入数据字典{column:value}
# return 返回DataFrame对象
def insertDB(tableName, insertDataDict):
    # 需要插入的columns
    insert_columns = list(insertDataDict.keys())
    # columns的value
    columns_value = [insertDataDict[k] for k in insert_columns]
    insert_columns_key = ",".join(insert_columns)

    if settings.DBTYPE=='Oracle':
        insert_columns_value = [":" + k for k in insert_columns]
        insert_columns_value = ",".join(insert_columns_value)
    elif settings.DBTYPE=='postgresql':
        insert_columns_value = ["%"+"s" for k in insert_columns]
        insert_columns_value = ",".join(insert_columns_value)

    connection = None
    cursor = None
    try:
        # 构建插入sql语句
        sql_insert = "INSERT INTO {} ({}) VALUES ({})".format(tableName, insert_columns_key, insert_columns_value)
        if settings.DBTYPE=='Oracle':
            sql_insert = sql_insert.upper()
        # Logger.debug("sql_insert:%s，columns_value:%s" % (sql_insert, columns_value))
        # print("sql_insert:%s，columns_value:%s" % (sql_insert,columns_value))
        connection = getDBConnect()
        cursor = connection.cursor()
        if settings.DBTYPE=='Oracle':
            cursor.prepare(sql_insert)
            cursor.execute(None, columns_value)
        elif settings.DBTYPE=='postgresql': 
            Logger.debug(sql_insert)
            cursor.execute(sql_insert, columns_value)

        num = cursor.rowcount  # 返回受影响行数
        connection.commit()
        return num
    except Exception as ex:
        Logger.errLineNo()
        Logger.error(ex)
        if not connection is None:
            connection.rollback()
        raise
    finally:
        if not cursor is None:
            cursor.close()
        if not connection is None:
            connection.close()


# tableName:修改数据表名
# updateDataDict:修改数据字段值字典{column:value,column:value}
# whereDataDict:修改数据字段条件字典{column:value,column:value}
def updateDB(tableName, updateDataDict, whereDataDict={}):
    # 需要插入的columns
    update_columns = list(updateDataDict.keys())
    # columns的value
    update_value = [updateDataDict[k] for k in update_columns]  # list(insertDataDict.values())
    # update_columns=",".join(update_columns)

    if settings.DBTYPE=='Oracle':
        update_columns_key = [k + "=:" + k for k in update_columns]
        update_columns_key = ",".join(update_columns_key)
    elif settings.DBTYPE=='postgresql':
        update_columns_key = [k+ "=%"+"s" for k in update_columns]
        update_columns_key = ",".join(update_columns_key)

    

    # 判断where是否有数据
    if len(whereDataDict.keys()) > 0:
        where_columns = list(whereDataDict.keys())
        where_value = [whereDataDict[k] for k in where_columns]
        where_columns_key = [k + "=:" + k for k in where_columns]
        where_columns_key = " and ".join(where_columns_key)
        fill_params = update_value + where_value
    else:
        fill_params = update_value

    connection = None
    cursor = None
    try:
        # 构建修改sql语句 UPDATE TABLE SET CLO1=:COL1,COL2=:COL2 WHERE WHERE1=:WHERE1 AND WHERE2=:WHERE2
        sql_update = "UPDATE {} SET {} ".format(tableName, update_columns_key)
        if len(whereDataDict.keys()) > 0:
            sql_update = "UPDATE {} SET {} WHERE {} ".format(tableName, update_columns_key, where_columns_key)

        if settings.DBTYPE=='Oracle':
            sql_update = sql_update.upper()
        Logger.debug("sql_update:%s ; values(%s)" % (sql_update, fill_params))
        # print("sql_update:%s ; values(%s)" % (sql_update, fill_params))
        connection = getDBConnect()
        cursor = connection.cursor()
        if settings.DBTYPE=='Oracle':
            cursor.prepare(sql_update)
            cursor.execute(None, fill_params)
        elif settings.DBTYPE=='postgresql': 
            cursor.execute(sql_update, fill_params)
        num = cursor.rowcount  # 返回受影响行数
        connection.commit()
        return num
    except Exception as ex:
        Logger.errLineNo()
        Logger.error(ex)
        if not connection is None:
            connection.rollback()
        raise
    finally:
        if not cursor is None:
            cursor.close()
        if not connection is None:
            connection.close()


# tableName:删除数据表名
# whereDataDict:删除数据字段条件字典{column:value,column:value}
def deleteDB(tableName, whereDataDict):
    # 需要插入的columns
    where_columns = list(whereDataDict.keys())
    # columns的value
    where_value = [whereDataDict[k] for k in where_columns]
    
    if settings.DBTYPE=='Oracle':
        where_columns_key = [k + "=:" + k for k in where_columns]
        where_columns_key = " and ".join(where_columns_key)

    elif settings.DBTYPE=='postgresql':
        where_columns_key = [k+ "=%"+"s" for k in where_columns]
        where_columns_key = ",".join(where_columns_key)


    fill_params = where_value

    connection = None
    cursor = None
    try:
        # 构建删除sql语句 DELETE FROM  TABLE  WHERE WHERE1=:WHERE1 AND WHERE2=:WHERE2
        sql_delete = "delete from  {} where {} ".format(tableName, where_columns_key)
        if settings.DBTYPE=='Oracle':
            sql_delete = sql_delete.upper()
        Logger.debug("sql_delete:%s ; values(%s)" % (sql_delete, fill_params))
        # print("sql_delete:%s ; values(%s)" % (sql_delete, fill_params))
        connection = getDBConnect()
        cursor = connection.cursor()
        if settings.DBTYPE=='Oracle':
            cursor.prepare(sql_delete)
            cursor.execute(None, fill_params)
        elif settings.DBTYPE=='postgresql': 
            cursor.execute(sql_delete, fill_params)
        num = cursor.rowcount  # 返回受影响行数
        connection.commit()
        return num
    except Exception as ex:
        Logger.errLineNo()
        Logger.error(ex)
        if not connection is None:
            connection.rollback()
        raise
    finally:
        if not cursor is None:
            cursor.close()
        if not connection is None:
            connection.close()


def getQueryInParam(param):
    """
    将单个或多个查询值格式化成sql in查询字符串
    """
    if type(param) is list:
        paramList = [str(x) for x in param]
    else:
        paramList=[str(param)]

    paramList = "','".join(paramList)
    paramList = "'" + str(paramList) + "'"
    return paramList


if __name__ == '__main__':
    on=OraclePool()

    sqlQuery = 'select * from userportfolio_new t where t.batchId=:batchId'
    data = pd.read_sql(sqlQuery, con=on._con, params={'batchId': 'xxx'})
    print(data)
