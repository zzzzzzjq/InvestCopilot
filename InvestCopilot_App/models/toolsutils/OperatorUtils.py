from InvestCopilot_App.models.toolsutils import dbutils as dbutils

import InvestCopilot_App.models.toolsutils.LoggerUtils as Logger_utils

Logger = Logger_utils.LoggerUtils()

STATETYPEKEY=['JIXIAO','ZUHE','REAL','STRATEGY']

def saveOrUpdateState(userId, stateType,paramsDict):
    """
    保存或更新用户操作状态
    userId：用户编号
    stateType：操作类型
    paramsDict：,字典
    """
    print(paramsDict)
    try:
        q_operatorState = "select count(1) from operatorstate t where t.userId='%s' and t.statetype='%s'" % (
        userId, stateType)
        con, cur = dbutils.getConnect()
        cur.execute(q_operatorState)

        rs = cur.fetchone()
        if rs[0] > 0:
            # update
            u_operatorState = "update operatorstate t set t.params=:params where t.userId=:userId and t.statetype=:stateType"
            cur.execute(u_operatorState, [json.dumps(paramsDict), userId, stateType])
            con.commit()
        else:
            # add
            i_operatorState = "insert into operatorstate (userId,statetype,params ) values (:userId,:statetype,:params)"
            cur.execute(i_operatorState, [userId, stateType, json.dumps(paramsDict)])
            con.commit()

        cur.close()
        con.close()
    except Exception as ex:
        Logger.errLineNo(msg="保存或更新用户操作状态异常")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass

def getOperatorState(userId, stateType):
    """
    获取用户操作状态参数列表
    return dict
    """
    con, cur = dbutils.getConnect()
    q_operatorState = "select params from operatorstate t where t.userId='%s' and t.statetype='%s'" % (
    userId, stateType)
    cur.execute(q_operatorState)
    rs = cur.fetchone()
    cur.close()
    con.close()
    if rs is None:
        return None
    else:
        return rs[0]

def delOperatorState(userId, stateType):
    """
    删除用户操作状态记录
    return dict
    """
    con, cur = dbutils.getConnect()
    try:
        q_operatorState = "delete from operatorstate t where t.userId=:userId and t.statetype=:statetype"
        cur.execute(q_operatorState,[userId,stateType])
        con.commit()
    except Exception as ex:
        Logger.errLineNo(msg="删除用户操作状态异常[%s][%s]" % (userId,stateType))
    finally:
        try:
            cur.close()
            con.close()
        except:pass

import json
def getStateValue(key, stateDictStr):
    """
    获取保存状态值，防止参数为名称为空的情况
    key不存在返回None
    """
    if stateDictStr is None:
        return None
    stateDict = json.loads(stateDictStr)
    if key in dict(stateDict):
        #保存的数据是数组
        if len(stateDict[key])>1:
            return stateDict[key]
        return stateDict[key][0]
    else:
        return None


if __name__ == '__main__':
    saveOrUpdateState('xxx1', 'xx1', {'a':1, 'b':2, 'c':'2'})
    rs = getOperatorState(userId='051', stateType='JIXIAO')
    print(rs)
    #{"opsers": ["xx"], "stateType": ["JIXIAO"], "sdfsd": ["23"]}
    ps = getStateValue('opsers', rs)
    ps = getStateValue('sfdsxxx', rs)
    print(ps)
