# coding:utf-8
__author__ = 'Rocky'

import pandas as pd
import json
import cx_Oracle
import InvestCopilot_App.models.toolsutils.dbutils as dbutils


def loadjsonfromdb(api,params):
    print("Load DF to DB....")
    print(api)
    print(params)
    str_params =''
    for p in params:
        str_params = str_params+p +'|'
    str_params = str_params[:-1]

    con,cur = dbutils.getConnect()
    jsonStr =''
    returncode =1

    #处理df类对象
    q_sql = "select df  from tdatastatic where api=:api and params = :params"    
    cur.execute(q_sql,[api,str_params])

    pram=[]
    for i in cur._cursor:
        if not i[0] is None:
            text = i[0].read()
            pram.append(text)        
    jsonStr = "".join(pram) 

    if len(jsonStr)==0 :
        returncode = -1    

    cur.close()    
    con.close()            
    return returncode ,jsonStr
    

def savejsontodb(api,params,jsonstr):
    print("Save DF to DB....")
    print(api)
    print(params)

    str_params = ''
    for p in params:
        str_params= str_params+ p +'|'
    str_params=  str_params[:-1]
    
    
    str_result = jsonstr
    str_result2 = ' '
    str_list =' '
    connection,cursor = dbutils.getConnect()

    sql = 'delete from tdatastatic where api=:1 and params=:2'
    cursor.execute(sql,[api,str_params])

    clob_data = cursor.var(cx_Oracle.CLOB)
    clob_data.setvalue(0,str_result)
    clob_data2 = cursor.var(cx_Oracle.CLOB)
    clob_data2.setvalue(0,str_result2)
    clob_data3 = cursor.var(cx_Oracle.CLOB)
    clob_data3.setvalue(0,str_list)
    
    sql = 'insert into tdatastatic values(:1,:2,:3,:4,:5)'
    cursor.execute(sql,(api,str_params,clob_data,clob_data2,clob_data3))  
    connection.commit()    
    cursor.close()
    connection.close()    

def loaddffromdb(api,params):
    print("Load DF to DB....")
    print(api)
    print(params)
    str_params =''
    for p in params:
        str_params = str_params+p +'|'
    str_params = str_params[:-1]

    con,cur = dbutils.getConnect()

    #处理df类对象
    q_sql = "select df  from tdatastatic where api=:api and params = :params"    
    cur.execute(q_sql,[api,str_params])
    pram=[]
    for i in cur._cursor:
        if not i[0] is None:
            text = i[0].read()
            pram.append(text)        
    jsonStr = "".join(pram) 
    rsDFs = []       
    if len(pram)>0 and not jsonStr.find('||||||'):        
        try:
            rsDF = pd.read_json(jsonStr, orient='table')
            rsDFs.append(rsDF)
        except Exception as err:
            rsDFs.append(pd.DataFrame())
            
    elif len(pram)>0:        
        jdfs= jsonStr.split('||||||')        
        for t_jdf  in jdfs :            
            jsonStr1 = "".join(t_jdf)            
            try:
                tDF = pd.read_json(jsonStr1, orient='table')
                rsDFs.append(tDF)
            except Exception as err:
                rsDFs.append(pd.DataFrame())

    #处理str类对象
    q_sql = "select str  from tdatastatic where api=:api and params = :params"    
    cur.execute(q_sql,[api,str_params])
    pram=[]
    for i in cur._cursor:
        if not i[0] is None:
            text = i[0].read()
            pram.append(text)    
    jsonStr = "".join(pram) 
    rsStrs = []   
    if len(pram)>0 and not jsonStr.find('||||||'):        
        rsStrs.append(pram)
    elif len(pram)>0:        
        jstrs= jsonStr.split('||||||')        
        rsStrs = jstrs
        
    cur.close()    
    con.close()    
    return rsDFs ,rsStrs


#20201117 加入了一个list类型的结果参数，为了保持和之前数据逻辑的兼容性，分离函数为两个，loaddffromdb 返回一个df数组，一个str数据，而loaddffromdb返回一个df数组，一个str数组，再加一个list数组
def loaddffromdb2(api,params):
    print("Load DF to DB....")
    print(api)
    print(params)
    str_params =''
    for p in params:
        str_params = str_params+p +'|'
    str_params = str_params[:-1]

    con,cur = dbutils.getConnect()

    #处理df类对象
    q_sql = "select df  from tdatastatic where api=:api and params = :params"    
    cur.execute(q_sql,[api,str_params])
    pram=[]
    for i in cur._cursor:
        if not i[0] is None:
            text = i[0].read()
            pram.append(text)        
    jsonStr = "".join(pram) 
    rsDFs = []       
    if len(pram)>0 and not jsonStr.find('||||||'):        
        try:
            rsDF = pd.read_json(jsonStr, orient='table')
            rsDFs.append(rsDF)
        except Exception as err:
            rsDFs.append(pd.DataFrame())
            
    elif len(pram)>0:        
        jdfs= jsonStr.split('||||||')        
        for t_jdf  in jdfs :            
            jsonStr1 = "".join(t_jdf)            
            try:
                tDF = pd.read_json(jsonStr1, orient='table')
                rsDFs.append(tDF)
            except Exception as err:
                rsDFs.append(pd.DataFrame())

    #处理str类对象
    q_sql = "select str  from tdatastatic where api=:api and params = :params"    
    cur.execute(q_sql,[api,str_params])
    pram=[]
    for i in cur._cursor:
        if not i[0] is None:
            text = i[0].read()
            pram.append(text)    
    jsonStr = "".join(pram) 
    rsStrs = []   
    if len(pram)>0 and not jsonStr.find('||||||'):        
        rsStrs.append(pram)
    elif len(pram)>0:        
        jstrs= jsonStr.split('||||||')        
        rsStrs = jstrs
    
    #处理list类对象
    q_sql = "select list  from tdatastatic where api=:api and params = :params"    
    cur.execute(q_sql,[api,str_params])
    pram=[]
    for i in cur._cursor:
        if not i[0] is None:
            text = i[0].read()
            pram.append(text)    
    jsonStr = "".join(pram) 
    rsLists = []   
    if len(pram)>0 and not jsonStr.find('||||||'):        
        rsLists.append(json.loads(pram))
    elif len(pram)>0:        
        jLists= jsonStr.split('||||||')        
        for t_jlist  in jLists :            
            jsonStr1 = "".join(t_jlist)            
            try:
                tList = json.loads(jsonStr1)
                rsLists.append(tList)
            except Exception as err:
                rsLists.append([])
    cur.close()    
    con.close()    
    return rsDFs ,rsStrs , rsLists

def savedftodb(api,params,resultlist):
    print("Save DF to DB....")
    print(api)
    print(params)

    str_params = ''
    for p in params:
        str_params= str_params+ p +'|'
    str_params=  str_params[:-1]
    
    str_result =''
    str_result2 =''    
    str_result3 =''
    for result in resultlist:    
        for result_df in result:
            if isinstance(result_df,str):
                str_result2 = str_result2+ result_df +'||||||' 
            elif isinstance(result_df, list):
                str_result3 = str_result3+ json.dumps(result_df) +'||||||'
            elif isinstance(result_df,pd.core.frame.DataFrame):
                str_result = str_result + result_df.to_json(orient='table',index=False)+'||||||'            

    str_result = str_result [:-6]
    str_result2 = str_result2 [:-6]        
    
    connection,cursor = dbutils.getConnect()

    sql = 'delete from tdatastatic where api=:1 and params=:2'
    cursor.execute(sql,[api,str_params])

    clob_data = cursor.var(cx_Oracle.CLOB)
    clob_data.setvalue(0,str_result)
    clob_data2 = cursor.var(cx_Oracle.CLOB)
    clob_data2.setvalue(0,str_result2)
    clob_data3 = cursor.var(cx_Oracle.CLOB)
    clob_data3.setvalue(0,str_result3)
    sql = 'insert into tdatastatic values(:1,:2,:3,:4,:5)'
    cursor.execute(sql,(api,str_params,clob_data,clob_data2,clob_data3))  
    connection.commit()    
    cursor.close()
    connection.close()
    

if __name__ == '__main__':    
    pass
