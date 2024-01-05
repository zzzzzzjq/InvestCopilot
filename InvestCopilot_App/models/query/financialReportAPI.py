# encoding: utf-8

from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.query import financialReport
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils

Logger = logger_utils.LoggerUtils()


from django.http import JsonResponse

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dsid_mfactors.settings")
from django.core.cache import cache
# 缓存有效期24小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24

@userLoginCheckDeco
def FinancialReportHandler(request):
    #组合管理
    rest = ResultData()
    try:

        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        Logger.debug("FinancialReportHandler request:%s"%reqData)
        if doMethod=="getYearFinancialReport":
            # user_id = request.session.get("user_id")
            windcode=reqData.get("windCode")
            # if user_id=="" or user_id is None:
            #     rest.errorData(errorMsg="Please Login.")
            #     return JsonResponse(rest.toDict())
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = financialReport.FinancialReportMode()
            rest = cm.getYearFinancialReport(windcode)
            return JsonResponse(rest.toDict())
        elif doMethod=="getQuarterFinancialReport":
            # user_id = request.session.get("user_id")
            windcode=reqData.get("windCode")
            # if user_id=="" or user_id is None:
            #     rest.errorData(errorMsg="Please Login.")
            #     return JsonResponse(rest.toDict())
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = financialReport.FinancialReportMode()
            rest = cm.getQuarterFinancialReport(windcode)
            return JsonResponse(rest.toDict())
        elif doMethod=="getQuarterandYearFinancialReport":
            # user_id = request.session.get("user_id")
            windcode = reqData.get("windCode")
            # if user_id=="" or user_id is None:
            #     rest.errorData(errorMsg="Please Login.")
            #     return JsonResponse(rest.toDict())
            if windcode == "" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = financialReport.FinancialReportMode()
            rest = cm.getQuarterandYearFinancialReport(windcode)
            return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        return JsonResponse(rest.toDict())
