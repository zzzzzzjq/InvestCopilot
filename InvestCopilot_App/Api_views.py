# coding:utf-8

from django.shortcuts import render
from django.http import HttpResponse
from django.http import StreamingHttpResponse
# from django.core.urlresolvers import reverse
from django.urls import reverse
from django.http import HttpResponseRedirect
from django.http import HttpResponseForbidden
from django.http import FileResponse
from wsgiref.util import FileWrapper
from django.http import JsonResponse
from django.shortcuts import render, get_object_or_404, redirect
from django.utils.safestring import mark_safe
import json
import pandas as pd
import InvestCopilot_App.models.toolsutils.LoggerUtils as Logger_utils
#原始返回对象
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import sys
import os
from django.http import request

Logger = Logger_utils.LoggerUtils()

# 用户状态检查
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco
from InvestCopilot_App.models.user.UserInfoUtil import CheckUserFundPriv

#数据库缓存DF数据表
import  InvestCopilot_App.models.toolsutils.dftodb as dftodb

#openAI任务管理
import InvestCopilot_App.models.openAI.openAiTaskManage as openAiTaskManage

#oracle 默认字符集环境变量
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'

#自选股
import  InvestCopilot_App.models.UserPortfolio.userportfolio as userportfolio
#指标相关
import InvestCopilot_App.models.factor.factorUtils as factorUtils


@userLoginCheckDeco
def proc_userportfolio(request):
    userid = request.session.get('user_id')
    print(userid)
    #userrealname = request.session.get('userrealname')
    rs = ResultData()
    try:
        doMethod = request.POST.get("doMethod", '')    
        new_userportfolio = request.POST.get("new_userportfolio", '')
        portfolioid = request.POST.get("portfolioid", '')
        if doMethod =='':
            jsonRequest = json.loads(request.body)            
            doMethod = jsonRequest['doMethod']
            if 'new_userportfolio' in jsonRequest.keys():
                new_userportfolio = jsonRequest['new_userportfolio']
            else:
                new_userportfolio = ''
            if 'portfolioid' in jsonRequest.keys():
                portfolioid = jsonRequest['portfolioid']
            else:
                portfolioid = ''            
        result =[]
        returnType = 'DF'
        
        if doMethod == 'new_userportfolio':   
            returnType = 'MSG'
            resultCode , resultMsg  = userportfolio.add_new_userportfolio(userid, new_userportfolio)
        elif doMethod == 'get_all_userportfolio':
            returnType = 'DF'
            result = userportfolio.get_userportfolio_list(userid)
        elif doMethod == 'get_userportfolio_detail':
            returnType = 'DF'
            result = userportfolio.get_userportfolio_detail(userid, portfolioid)

        if returnType == 'DF':
            for i in range(len(result)):            
                setattr(rs,'data'+str(i), result[i].round(4).values.tolist())
                setattr(rs,'columns'+str(i), result[i].columns.values.tolist())
        elif returnType=='MSG':             
            if resultCode <100:                
                rs.errorData(errorCode = resultCode, errorMsg=resultMsg)
            else:
                rs.returnCode = resultCode
                rs.returnMsg = resultMsg
        return JsonResponse(rs.toDict())    

    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='抱歉，处理自选股信息数据失败，请稍后重试。')
        return JsonResponse(rs.toDict())



@userLoginCheckDeco
def viewInterResearch(request):
    userid = request.session.get('user_id')
    rs = ResultData()
    try:

        doMethod = request.POST.get("doMethod", '')
        researchId = request.POST.get("researchId", '')
        if doMethod =='':
            doMethod = request.GET.get("doMethod", '')
            researchId = request.POST.get("researchId", '')
        if doMethod =='':
            jsonRequest = json.loads(request.body)
            doMethod = jsonRequest['doMethod']
            researchId = jsonRequest['researchId']
        print("researchId:",researchId)
        result =[]
        returnType = 'DF'
        if doMethod == 'getInterResearchReport':
            returnType = 'FILE'
            #看是否直接下载，或者打开
            download = False
            #这里传入userid是为了以后数据权限控制
            #filepath,reachFileName = report_mode.getReserchFile(userid, researchId)
            filepath = r'C:\Users\env\Downloads\19978009.pdf'
            filepath = r'Z:\cicc\reserach\0027__HK\银河娱乐_期待银河3期放量.pdf'
            reachFileName = "银河娱乐_期待银河3期放量.pdf"
            reachFileName = "test.pdf"
            print("filepath:",filepath)
            reportfile = open(filepath, 'rb')
            data = reportfile.read()
            reportfile.close()

            #如果文件不下载，直接打开，那么在这里直接返回文件流
            # if not download:
            #     return HttpResponse(data, content_type='application/pdf')
            import base64
            #可以根据不同的头实现文件流式下载
            """
            case "pdf": ctype="application/pdf"; break; 
            case "exe": ctype="application/octet-stream"; break; 
            case "zip": ctype="application/zip"; break; 
            case "doc": ctype="application/msword"; break; 
            case "xls": ctype="application/vnd.ms-excel"; break; 
            case "ppt": ctype="application/vnd.ms-powerpoint"; break; 
            case "gif": ctype="image/gif"; break; 
            case "png": ctype="image/png"; break; 
            case "jpeg":ctype="image/jpg"; break;  
            case "jpg": ctype="image/jpg"; break; 
            default: $ctype="application/force-download"; 
            """
            rs.file = 'data:application/pdf;base64,' + \
                  base64.b64encode(data).decode('ascii')
            rs.filename = reachFileName
        else:
            returnType='MSG'
            resultCode= -201
            resultMsg = 'doMethod参数错误，无效的操作'


        if returnType == 'DF':
            for i in range(len(result)):
                setattr(rs,'data'+str(i), result[i].round(4).values.tolist())
                setattr(rs,'columns'+str(i), result[i].columns.values.tolist())
        elif returnType == 'MSG':
            if resultCode <100:
                rs.errorData(errorCode = resultCode, errorMsg=resultMsg)
            else:
                rs.returnCode = resultCode
                rs.returnMsg = resultMsg
        
        return JsonResponse(rs.toDict())

    except Exception as ex:
        Logger.errLineNo()        
        rs.errorData(errorMsg='抱歉，处理内部研报数据失败，请稍后重试。')
        return JsonResponse(rs.toDict())

@userLoginCheckDeco
def procOpenAITaskInfo(request):
    userid = request.session.get('user_id')
    rs = ResultData()
    try:

        doMethod = request.POST.get("doMethod", '')        

        if doMethod =='':
            doMethod = request.GET.get("doMethod", '')
            
        if doMethod =='':
            jsonRequest = json.loads(request.body)
            doMethod = jsonRequest['doMethod']
        
        
        result =[]
        returnType = 'DF'
        if doMethod == 'getOpenAITaskInfo':
            returnType = 'DF'
            result = openAiTaskManage.getOpenAITaskInfo()
        elif doMethod == 'getOpenAIQueueInfo':
            returnType = 'DF'
            result = openAiTaskManage.getOpenAIQueueInfo()
        else:
            returnType='MSG'
            resultCode= -201
            resultMsg = 'doMethod参数错误，无效的操作'


        if returnType == 'DF':
            for i in range(len(result)):            
                setattr(rs,'data'+str(i), result[i].round(4).values.tolist())
                setattr(rs,'columns'+str(i), result[i].columns.values.tolist())
        elif returnType=='MSG':             
            if resultCode <100:                
                rs.errorData(errorCode = resultCode, errorMsg=resultMsg)
            else:
                rs.returnCode = resultCode
                rs.returnMsg = resultMsg
        return JsonResponse(rs.toDict())    

    except Exception as ex:
        Logger.errLineNo()        
        rs.errorData(errorMsg='抱歉，处理内部研报数据失败，请稍后重试。')
        return JsonResponse(rs.toDict())