import pandas as pd
import json
import traceback

import InvestCopilot_App.models.toolsutils.dbutils as dbutils
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as Logger_utils




Logger = Logger_utils.LoggerUtils()




#获取openai任务列表
def getOpenAITaskInfo():
    returnArray =[]
    returnArray2 =[]
    result =[]
    
    
    #api= os.path.basename(sys._getframe().f_code.co_filename)
    #api= api+':'+ sys._getframe().f_code.co_name 
    #result, result_str = dftodb.loaddffromdb(api,[indexName,enddate]) 
    
    if len(result) !=0:        
        for i in range(len(result)):            
            result[i].fillna('',inplace=True)
            returnArray.append(result[i])
            returnArray2.append(result_str[i])
    else:
        con,cur = dbutils.getConnect()

        try:
            sql = "select * from topenaitask order by task_id desc"
            rsDF0 = pd.read_sql(sql, con=con)
            print(rsDF0)
            
        except Exception as ex:
            Logger.errLineNo(traceback.format_exc())
            rsDF0 = pd.DataFrame()
        finally:
            cur.close()
            con.close()
                
        resultlength =1
        for i in range(resultlength):            
            exec("rsDF{}.fillna('',inplace=True)".format(i))        
            exec("returnArray.append(rsDF{})".format(i))            
        
        #dftodb.savedftodb(api,[indexName,enddate],[returnArray,returnArray2])

    return returnArray


#获取openai队列列表
def getOpenAIQueueInfo():
    returnArray =[]
    returnArray2 =[]
    result =[]
    
    
    #api= os.path.basename(sys._getframe().f_code.co_filename)
    #api= api+':'+ sys._getframe().f_code.co_name 
    #result, result_str = dftodb.loaddffromdb(api,[indexName,enddate]) 
    
    if len(result) !=0:        
        for i in range(len(result)):            
            result[i].fillna('',inplace=True)
            returnArray.append(result[i])
            returnArray2.append(result_str[i])
    else:
        con,cur = dbutils.getConnect()

        try:
            sql = "select t.*,'' as operation from topenaiqueue t order by queue_order"
            rsDF0 = pd.read_sql(sql, con=con)
            
            
        except Exception as ex:
            Logger.errLineNo(traceback.format_exc())
            rsDF0 = pd.DataFrame()
        finally:
            cur.close()
            con.close()
                
        resultlength =1
        for i in range(resultlength):            
            exec("rsDF{}.fillna('',inplace=True)".format(i))        
            exec("returnArray.append(rsDF{})".format(i))            
        
        #dftodb.savedftodb(api,[indexName,enddate],[returnArray,returnArray2])

    return returnArray


#添加自选股组合
def add_new_userportfolio(userid, new_userportfolio):

    resultCode ,resultMsg = -1,'执行失败！'   

    #向数据库中添加自选股组合

    con,cur = dbutils.getConnect()

    try:
        sql = "insert into userportfolio(portfolioid,userid, portfolioname,stocknum) values (nextval('SEQ_PORTFOLIOID'),'{}','{}',0)".format(userid, new_userportfolio)
        cur.execute(sql)
        con.commit()
        resultCode ,resultMsg = 100,'添加成功！'
    except Exception as ex:
        Logger.errLineNo(traceback.format_exc())
        con.rollback()
        resultCode ,resultMsg = -100,'添加失败！'
    finally:
        cur.close()
        con.close()    

    return resultCode ,resultMsg

#删除自选股组合
def del_userportfolio(userid, userportfolioid ,userportfolio):
    
    resultCode ,resultMsg = -1,'执行失败！'   

    #向数据库中添加自选股组合

    con,cur = dbutils.getConnect()

    try:
        sql = "delete from  userportfolio where portfolioid ={} and userid ='{}' and  userportfolio ='{}' ".format(userportfolioid, userid, userportfolio)
        cur.execute(sql)
        con.commit()
        resultCode ,resultMsg = 100,'删除成功！'
    except Exception as ex:
        Logger.errLineNo(traceback.format_exc())
        con.rollback()
        resultCode ,resultMsg = -100,'删除失败！'
    finally:
        cur.close()
        con.close()    

    return resultCode ,resultMsg

#获取自选股组合明细

if __name__ == '__main__':
    None

