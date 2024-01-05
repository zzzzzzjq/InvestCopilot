__author__ = 'Robby'

import datetime
import numpy as np
import json
import logging
import time
import math
import pandas as pd
from pyDes import des,CBC,PAD_PKCS5
import base64
import socket

import pinyin.cedict

from collections import defaultdict

DES_KEY = "ALC!@#EF"
DES_IV = '\0\0\0\0\0\0\0\0'

from difflib import SequenceMatcher
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

globa_default_template_userId="80"
globa_default_template_no="9"#暂时无用 ，测试需要
globa_companyId="pinnacle"

# 消息通知
if socket.gethostname() == 'iZebwatodogxv8Z':
    globa_vtouser = 'robby.xia|caomy'#
else:
    globa_vtouser = 'robby.xia'


def floatFormat(value, size=0):
    try:
        if value is None or pd.isnull(value):
            return value
        if 0 == size:
            return round(float(value))  # 取整，四舍五入
        return round(float(value), int(size))
    except Exception as ex:
        print(value,size,ex)
        return value


def __multi_percent(value):
    value = float(value)
    value = round(value * 100, 2)
    return value


def __dive_percent(value):
    value = float(value)
    value = round(value / 10000, 2)
    return value


def __round_2(value):
    return round(value, 2)


def __moving_average(a, n):
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    ret[n - 1:] = ret[n - 1:] / n
    ret[n - 1:] = np.round(ret[n - 1:], 3)
    result = []
    for i in range(n - 1):
        result.append('-')
    result.extend(ret[n - 1:].tolist())
    return result


def beanToDict(bean):
    return bean.__dict__


def beanToJson(bean):
    return json.dumps(beanToDict(bean))


def jsonToBean(className, jsonStr):
    '''
    将json字符串转换为对象
    from InvestCopilot_App.models.toolsutils import ResultData as input_params
    params = requestToBean(input_params.InputParams, request)
    '''
    tjson = json.loads(jsonStr)
    newClass = className()
    newClass.__dict__ = tjson
    return newClass


def requestToDict(request):
    '''
    将request对象转换为字典
    处理完毕后返回的数据结构为 key1:list1,key2:list2
    '''
    if request.method == 'GET':
        # do_something()
        return dict(request.GET)
    elif request.method == 'POST':
        # do_something_else()
        return dict(request.POST)


def requestToBean(className, request):
    '''
    from InvestCopilot_App.models.toolsutils import ResultData as input_params
    import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
    params = tools_utils.requestToBean(input_params.InputParams, request)
    print(params)
    '''
    paramsDict = requestToDict(request)
    jsonStr = json.dumps(paramsDict)
    return jsonToBean(className, jsonStr)


def dfToListInnerDict(pd):
    # in DataFrame
    # out  list[dict..]
    listData = []
    for idx, row in pd.iterrows():
        listData.append(dict(row))

    return listData

def dfToJsonGroupby(pd,groubycolumn,datacolumn):
    # in groubycolumn:list
    # in datacolumn :list
    # out  Json
    # 20190711 liuy
    # 把dataframe转成下面参考格式的json
    # [
    #     {
    #         "CUSTOMER_ID": 10078229,
    #         "DATA": [
    #             {
    #                 "PRODUCT_ID": 508136536,
    #                 "VENDOR_ID": 450,
    #                 "DAT": "2018-11-23",
    #                 "ORDER_ID": 20183200576771,
    #                 "COLOR_ID": 1000
    #             }
    # ]
    df2 = pd.groupby(groubycolumn)[datacolumn].apply(lambda x: x.to_dict(orient="records")).reset_index(name="DATA").to_json(orient="records")

    return df2


def portfolioFileUpLoad(file_obj, userid):
    resultData = ResultData()
    if file_obj == None:
        resultData.errorData(errorCode='-110', errorMsg='文件上传失败')

    # factorname = request.POST.getlist('factorname') #因子名称
    # factorname = factorname[0]
    Logger.info('上传文件路径名：' + file_obj.name)
    # logger.info('因子名称：'+factorname)

    # file_name = 'tempfile-%d' % random.randint(0,1000000) # 不能使用文件名称，因为存在中文，会引起内部错误
    file_name = 'sdef_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')  # 不能使用文件名称，因为存在中文，会引起内部错误
    file_name = userid + file_name
    # 注意正式上传时要修改文件绝对路径，否则会上传到apache/bin目录下。
    import os
    
    file_abs_dir = os.path.dirname(os.path.abspath('.'))
    file_full_path = file_abs_dir +'/fileupload/sdefupload/' + file_name

    dest = open(file_full_path, 'wb+')
    dest.write(file_obj.read())
    dest.close()
    Logger.info('保存的文件为' + file_full_path)
    return file_full_path


def charLength(String, maxLength=8, checkObj="输入内容"):
    result = ResultData()
    if not isNull(String):
        dataL = str(String).strip().encode('gbk', 'ignore')
        if len(dataL) > maxLength:
            result.errorData(errorMsg='%s不能超过%d个汉字%s个字符' % (checkObj, maxLength / 2, maxLength))
            return result
        else:
            return result
    return result.errorData(errorMsg="%s不能为空" % checkObj)


def isNumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
    except Exception:
        return False



def strSimilar(baseStr, compStr):
    """
    计算字符串的相似度
    """
    return SequenceMatcher(None, baseStr, compStr).ratio()
    # return SequenceMatcher(None, baseStr, compStr).real_quick_ratio()
    # return SequenceMatcher(None, baseStr, compStr).quick_ratio()

def strEncode(content, encode='GBK'):
    """
    格式字符串，避免字符串转码错误,主要是数据库保存报错，python为UTF-8编码，oracle数据库为GBK
    UnicodeEncodeError: 'gbk' codec can't encode character '\u200b'
    """
    try:
        content=content.encode(encode,errors="ignore")
        content=content.decode(encode,errors="ignore")
    except Exception as ex:
        Logger.error(ex)
        return content

    return content


def dataFrameToTableHtml(df):
    """
    将DataFrame对象转换成tableHtml。
    """
    tableHtml = "<table><tr><td>&nbsp;<td><td>&nbsp;<td><tr></table>"
    try:
        if not df.empty:
            tableHtml = "<table  border='1' cellpadding='1' cellspacing='1' style='width:980px'>"
            columns = df.columns.values.tolist()
            thList = ['<{0}>'.format("tr")]
            for td_th in columns:
                thList.append('<{0}>{1}</{2}>'.format("th", td_th, "th"))
            thList.append('</{0}>'.format("tr"))
            trList=[]
            for idx, row in df.iterrows():
                trList.append('<{0}>'.format("tr"))
                for col_name in df.columns:
                    trList.append('<{0}>{1}</{2}>'.format("td", row[col_name],"td"))
                trList.append('</{0}>'.format("tr"))
            thList = "".join(thList)
            trList = "".join(trList)
            tableHtml = tableHtml + thList + trList + "</table>"
    except Exception as ex:
        Logger.error(ex)
        return tableHtml

    return tableHtml


def getQueryInParam(param):
    """
    将单个或多个查询值格式化成sql in查询字符串
    """
    if type(param) is list:
        paramList = [str(x).lower() for x in param]
    else:
        paramList=[str(param).lower()]

    paramList = "','".join(paramList)
    paramList = "'" + str(paramList) + "'"
    return paramList


def dateFormat(date,join="/"):
    """字符串日期格式：yyyy/mm/dd"""
    try:
        date=str(date)
        if len(date)>=8:
            return date[0:4]+join+date[4:6]+join+date[6:8]
    except:
        pass
    return date

def quarterlyType(s_profitnotice_period):
    """
    根据日期判断季报类型，0331：(一季报)，0630：(中报)，0930：(三季报) 1231：(年报)
    """
    s_profitnotice_period=s_profitnotice_period[4:8]
    if s_profitnotice_period == '0331': s_profitnotice_period = '1季报'
    if s_profitnotice_period == '0630': s_profitnotice_period = '中报'
    if s_profitnotice_period == '0930': s_profitnotice_period = '3季报'
    if s_profitnotice_period == '1231': s_profitnotice_period = '年报'

    return s_profitnotice_period


def formatDigit(value,mode="{:,.2f}"):
    """
    格式化数据
    千分位分隔符 保留两位小数 不足两位的补零
    mode:
    {:.2f}:保留2位小数；
    {:,.2f}：千分位分隔符 ，保留两位小数 不足的补零 字符串
    """
    try:
        value = mode.format(value)  #
        return value
    except:
        return value

def addPercent(value,mode="{:,.2f}"):
    """
    数据添加百分号
    """
    newValue = formatDigit(value,mode)
    if newValue != '-':
        newValue += '%'

    return newValue


def formatDigit2Integer(value):
    '''
    格式化数据
    转化成整数  四舍五入
    '''
    try:
        value = round(float(value))
        return value
    except:
        return value

def timeStr2Datetime(timeStr,mode='%Y-%m-%d %H:%M:%S'):
    """
    将时间转化成datetime
    """
    return datetime.datetime.strptime(timeStr,mode)


def changeTimeFormat(timeSec):
    """
    将时间秒数转化成XX天XX秒的格式
    """
    day = 24*60*60
    hour = 60*60
    min = 60
    if timeSec <60:
        return  "%d 秒"%math.ceil(timeSec)
    elif  timeSec > day:
        days = divmod(timeSec,day)
        return "%d 天, %s"%(int(days[0]),changeTimeFormat(days[1]))
    elif timeSec > hour:
        hours = divmod(timeSec,hour)
        return '%d 小时, %s'%(int(hours[0]),changeTimeFormat(hours[1]))
    else:
        mins = divmod(timeSec,min)
        return "%d 分钟, %d 秒"%(int(mins[0]),math.ceil(mins[1]))

import hashlib
def md5(value):
    """md5"""
    try:
        # encoded = base64.b64encode(str)
        value = value.encode('utf-8')
        md5 = hashlib.md5()
        md5.update(value)
        return (md5.hexdigest())
    except Exception as ex:
        return value

def stmDate(date):
    return dateFormat(date)

def getReportYear(reportDate=''):
    """
    通过获取个股财报日期判断是否需要切换fy0，fy1，fy2，
    """
    nowYear = int(datetime.datetime.today().year) #2018
    preYear=(datetime.datetime.now()+datetime.timedelta(days=-365)).year #2017

    # reportYear=reportDate[0:4]
    if reportDate !='':
        if reportDate==str(nowYear)+"1231" or reportDate==str(preYear)+"1231":
            #出年报了，日期要切换
            year=nowYear
        else:
            #不切换
            year=preYear
    else:
        year=preYear

    fy0 = year - 1
    fy1 = year
    fy2 = year + 1
    fy3 = year + 2
    fyDict={'FY0':str(fy0)[2:4]+"年",'FY1':str(fy1)[2:4]+"年",'FY2':str(fy2)[2:4]+"年",'FY3':str(fy3)[2:4]+"年"}
    return fyDict

def formatDateToChinese(date):
    """
    将日期格式化成中国风格式
    """
    year = date[0:4]
    month = date[4:6]
    day = date[6:8]
    if month[0:1] == '0':
        month = month[1:]
    if day[0:1] == '0':
        day = day[1:]
    dateStr = year+'年'+month+'月'+day+'日'
    return dateStr

def getNowYearStartDate():
    """
    获取今年第一天
    """
    tmp = datetime.datetime.today().year
    return ''.join([str(tmp),'0101'])

def stringEncode(value):
    """
    字符串加密函数
    """
    k = des(DES_KEY,CBC,DES_IV,padmode=PAD_PKCS5)
    encodeStr = k.encrypt(value)
    encodeFinalStr = base64.b64encode(encodeStr).decode('ascii')
    return encodeFinalStr

def stringDecode(value):
    """
    字符串解密函数
    """
    k = des(DES_KEY, CBC, DES_IV, padmode=PAD_PKCS5)
    decodeStr = k.decrypt(base64.decodebytes(value.encode('ascii'))).decode('ascii')
    return decodeStr


def chartMaxLengthHide(chart, size=10):
    chart=str(chart).strip()
    if len(chart) > size:
        return str(chart[0:size]) + "..."
    return chart


def getSeqNo():
    """
    获取序列编号，SEQ_BATCHID ，自增
    """
    from InvestCopilot_App.models.toolsutils import dbutils as dbutils
    return dbutils.getSeqNo()


def isNull(obj):
    """
    判断数据是否为空
    """
    if obj is None or obj == '':
        return True
    if isinstance(obj, str):
        # 去空格
        obj = str(obj).strip()
        if len(obj) == 0:
            return True
        else:
            return False
    if isinstance(obj, list) or isinstance(obj, set) or isinstance(obj, dict):
        if len(obj) == 0:
            return True
        else:
            return False

    if isinstance(obj, pd.DataFrame):
        if obj.empty:
            return True
        else:
            return False

    if obj != "":
        return False

    return False


def bulildInsertSql(tableName, tableColumns):
    # 构建模板插入语句
    tableColumns.append("caseId")
    tableColumns.append("fileId")
    columns = ",".join(tableColumns)
    first = tableColumns.pop(0)
    vP = ",:v_".join(tableColumns)
    vP = ":v_%s,:v_" % first + vP
    i_sql = "insert into %s (%s) values (%s)" % (tableName, columns, vP)
    return i_sql


def compFileTemplate(selfTemplateKeyDict, columns):
    # 匹配文件对应模板
    for columnsIdStr, templateInfo in selfTemplateKeyDict.items():
        fileColumns = columnsIdStr.split(",")
        sameColumns = list(set(fileColumns) & set(columns))
        if len(sameColumns) == len(fileColumns):
            return selfTemplateKeyDict[columnsIdStr]
    return None

def requestGetData(request,name,fill=None):
    gname = request.POST.get(name,fill)
    if gname==fill:
        gname = request.GET.get(name, fill)
    return gname



def dfColumUpper(fdf):
    fdf.columns = fdf.columns.map(lambda x: x.upper())
    return fdf

def fmtdt(dt,fefault=None):
    my_dict = defaultdict(fefault)
    if len(dt)>0:
        for k,v in dt.items():
            my_dict[k]=v
    return my_dict

def requestDataFmt(request,fefault=None):
    rest=ResultData()
    content_type = request.content_type
    if content_type == 'application/json':
        # 请求的数据类型为 JSON
        try:
            data = json.loads(request.body)
            rtdata=fmtdt(data,fefault)
            # rest = {'errorCode': '0000001', 'errorFlag': True, 'translateCode': '', 'errorMsg':"","data": rtdata}
            rest.data=rtdata
            return rest
        except json.JSONDecodeError:
            # rest = {'errorCode': '0000001', 'errorFlag': False, 'translateCode': '', 'errorMsg': '无法解析JSON数据', "data": ""}
            Logger.error(msg=request.body)
            Logger.errLineNo(msg="无法解析JSON数据")
            rest.errorData(errorMsg="无法解析JSON数据")
            return rest
    else:
    # elif content_type == 'multipart/form-data' or content_type == 'application/x-www-form-urlencoded' or content_type == 'text/plain':
        data = request.POST.dict()
        if len(data)==0:
            data=request.GET.dict()
        rtdata = fmtdt(data, fefault)
        rest.data=rtdata
        return rest
    # else:
    #     #其他数据类型
    #     rest.errorData(errorMsg= '无法解析%s数据'%content_type)
    # return rest


def getPinYinInitial(value,isStockCode=True):
    """
    获取中文的拼音首字母
    """
    try:
        if isStockCode:
            res=pinyin.get_initial(value)
            value = str(res).lower().replace(" ",'').replace("'",'').replace("*",'').replace("-u",'').replace("-w",'').replace("(ts)",'').replace("&",'').replace("(ipozz)",'')
        else:
            value = str(value).replace(" ", '')
        return value
    except:
        return value


def curtimestamp():
    """
    timeStamp = 1604387158
    dateArray = datetime.datetime.fromtimestamp(timeStamp)
    otherStyleTime = dateArray.strftime("%Y--%m--%d %H:%M:%S")
    print(otherStyleTime)   # 2013--10--10 23:40:00
    """
    # x =datetime.datetime.strptime(lastUpdated, "%Y-%m-%dT%H:%M:%S.%f")
    t=datetime.datetime.now().timestamp()
    t=str(t).replace(".","")
    return t
if __name__ == '__main__':
    # print(formatDigit(2099999999.0564))
    de = getReportYear()
    # de =floatFormat(3.0,0)
    print(de)
