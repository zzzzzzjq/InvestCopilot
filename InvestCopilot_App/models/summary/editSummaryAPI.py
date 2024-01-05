#摘要编辑
import datetime
import re
import os
import socket
import json
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.summary.audioMode import audioMode
from InvestCopilot_App.models.manager import ManagerUserMode as manager_user_mode
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
import InvestCopilot_App.models.toolsutils.sendmsg as wx_send
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.summary.viewSummaryMode import viewSummaryMode
from django.http import JsonResponse
from django.http import HttpResponse
from django.http import StreamingHttpResponse
from urllib.parse import quote

Logger = logger_utils.LoggerUtils()

@userLoginCheckDeco
def summaryEditAPIHandler(request):
    rest = ResultData()
    try:
        user_id = request.session.get("user_id")
        rest = tools_utils.requestDataFmt(request, fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod = reqData.get("doMethod")
        Logger.debug("summaryEditAPIHandler request:%s"%reqData)
        if doMethod == "getSummaryEditState":#获取当前摘要修改状态
            dataId = reqData.get("dataId")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="请选择需要编辑的文档")
                return JsonResponse(rest.toDict())
            rest = viewSummaryMode().getEditSummarysStatusForUpdate(dataId, user_id)
            return JsonResponse(rest.toDict())
        elif doMethod == "releaseSummaryEditState":#释放当前报告编辑状态
            dataId = reqData.get("dataId")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="请选择需要编辑的文档")
                return JsonResponse(rest.toDict())
            rest = viewSummaryMode().releaseSummaryEditStatus(dataId, user_id)
            return JsonResponse(rest.toDict())
        elif doMethod == "getSummaryEditLastVersionData":#获取修正变摘要版本
            dataId=reqData.get("dataId")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="请选择需要编辑的文档")
                return JsonResponse(rest.toDict())
            languages=reqData.get("languages")
            oneDF = viewSummaryMode().getSummaryModifyLastVersionData(dataId, languages)
            rest.data= {"count":0}
            if not oneDF.empty:
                summayList = oneDF[["SID","RID",'CCONTENT','LANGUAGES','EDITTIME','DTYPE','DSOURCE']].rename(
                    columns=lambda x: x.capitalize()).to_json(orient='records')
                summayList = json.loads(summayList)
                rest.data= {"count":len(summayList),"data":summayList}
            return JsonResponse(rest.toDict())

        elif doMethod == "saveEditSummaryData":#保存修正变摘要版本
            dataId=reqData.get("dataId")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="请选择需要编辑的文档")
            newSummaryData=reqData.get("newSummaryData")
            if newSummaryData=="" or newSummaryData is None:
                rest.errorData(errorMsg="摘要内容不能为空")
            newSummaryData = str(newSummaryData).strip()
            dataL = newSummaryData.encode('gbk', 'ignore')
            maxLength=100
            if len(dataL) < maxLength:
                rest.errorData(errorMsg='摘要内容不能不能低于%d个汉字%s个字符' % (maxLength / 2, maxLength))
                return JsonResponse(rest.toDict())
            rest = tools_utils.charLength(newSummaryData, 10000, '摘要内容')
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            languages=reqData.get("languages")
            rest = viewSummaryMode().editSummaryDataSave(dataId,user_id,newSummaryData,languages)
            return JsonResponse(rest.toDict())
        else:
            errorMsg = '抱歉，请求接口不存在，请重新选择。'
            rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())

    except Exception as ex:
        errorMsg = '抱歉，获取数据失败，请稍后重试。'
        Logger.errLineNo(msg=errorMsg)
        rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())
