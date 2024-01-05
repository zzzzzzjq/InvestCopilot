# encoding: utf-8

from InvestCopilot_App.models.factor import factorMode
from InvestCopilot_App.models.factor.factorMenu import factorMenu

from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.portfolio import portfolioMode
from InvestCopilot_App.models.factor import factorUtils
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
Logger = logger_utils.LoggerUtils()


from django.http import JsonResponse
import time
import datetime
import pandas as pd
import math
import socket
import os
import traceback
import json
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dsid_mfactors.settings")
from django.core.cache import cache
# 缓存有效期24小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24

@userLoginCheckDeco
def factorUtilsAPIHandler(request):
    #组合管理
    rest = ResultData()
    try:
        Logger.debug("factorUtilsAPIHandler request:"%request.GET)

        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        user_id = request.session.get("user_id")
        if doMethod=="get_factors_datas":
            #获得本用户的全部指标组合列表
            user_id = request.session.get("user_id")
            templateNo = reqData.get("templateNo")
            portfolioId = reqData.get("portfolioId")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = factorUtils.factor_utils()
            qrest = cm.get_factors_data(user_id,templateNo,portfolioId)
            return JsonResponse(qrest.toDict())
        elif doMethod=="getFactorsDataByWindcode":
            user_id = request.session.get("user_id")
            templateNo = reqData.get("templateNo")
            windcode_list = reqData.get("windCodestr")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if windcode_list=="" or windcode_list is None:
                rest.errorData(errorMsg="Please enter a windcodestr")
                return JsonResponse(rest.toDict())
            cm = factorUtils.factor_utils()
            qrest = cm.getFactorsDataByWindcode(user_id,templateNo,windcode_list)
            return JsonResponse(qrest.toDict())
        elif doMethod=="getDefaultFactorsDataByWindcode":
            templateNo = reqData.get("templateNo")
            windcode_list = reqData.get("windCodestr")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            if windcode_list=="" or windcode_list is None:
                rest.errorData(errorMsg="Please enter a windcodestr")
                return JsonResponse(rest.toDict())
            cm = factorUtils.factor_utils()
            qrest = cm.getDefaultFactorsDataByWindcode(templateNo, windcode_list)
            return JsonResponse(qrest.toDict())
        elif doMethod=="get_factors_Comparison_among_peers":
            #获得本用户的全部指标组合列表
            windCode = reqData.get("windCode")
            if windCode == "" or windCode is None:
                rest.errorData(errorMsg="Please enter a windCode.")
                return JsonResponse(rest.toDict())
            cm = factorUtils.factor_utils()
            qrest = cm.get_factors_Comparison_among_peers(windcode=windCode)
            return JsonResponse(qrest.toDict())
        elif doMethod=="get_factors_indexcode":
            #  指数查询
            INDEXCODE = reqData.get("indexCode")
            if INDEXCODE == "" or INDEXCODE is None:
                rest.errorData(errorMsg="Please enter a INDEXCODE.")
                return JsonResponse(rest.toDict())
            cm = factorUtils.factor_utils()
            qrest = cm.get_factors_indexcode(INDEXCODE=INDEXCODE)
            return JsonResponse(qrest.toDict())
        elif doMethod=="get_factors_datas_1116":
            #获得本用户的全部指标组合列表
            user_id = request.session.get("user_id")
            templateNo = reqData.get("templateNo")
            portfolioId = reqData.get("portfolioId")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolioId.")
                return JsonResponse(rest.toDict())
            cm = factorUtils.factor_utils()
            qrest = cm.get_factors_data(user_id,templateNo,portfolioId)
            return JsonResponse(qrest.toDict())
        elif doMethod=="get_factors_datas_1115":
            #获得本用户的全部指标组合列表
            user_id = request.session.get("user_id")
            templateNo = reqData.get("templateNo")
            stock_list = reqData.get("stockList")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if stock_list=="" or stock_list is None:
                rest.errorData(errorMsg="Please enter a stock_list.")
                return JsonResponse(rest.toDict())
            stock_list = stock_list.split('|')
            # print(stock_list)  # <class 'list'>
            cm = factorUtils.factor_utils()
            qrest = cm.get_factors_data_1115(user_id,templateNo,stock_list)
            return JsonResponse(qrest.toDict())
        else:
            rest.errorData(errorMsg="There is no such method, Please check the input.")
            return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        # UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())