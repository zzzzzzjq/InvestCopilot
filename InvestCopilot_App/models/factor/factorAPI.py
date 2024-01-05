# encoding: utf-8

from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.factor import factorMode
from InvestCopilot_App.models.factor.factorMenu import factorMenu
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
def factorAPIHandler(request):
    #组合管理
    rest = ResultData()
    try:

        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        user_id = request.session.get("user_id")
        print("factorAPIHandler request:%s"%reqData)
        Logger.debug("factorAPIHandler request:%s"%reqData)
        if doMethod=="createFactorCombination": ##jsr   createFactorCombination
            #添加组合
            templateName=reqData.get("templateName") ##jsr templateName
            user_id = request.session.get("user_id")
            portfolioid = reqData.get("portfolioId")
            if templateName=="" or templateName is None:
                rest.errorData(errorMsg="Please enter a templateName.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if portfolioid =="" or portfolioid is None:
                rest.errorData(errorMsg="Please enter a portfolioid.")
                return JsonResponse(rest.toDict())
            templateName=str(templateName).strip()
            ckrts = tools_utils.charLength(templateName,20)
            if not ckrts.errorFlag:
                rest.errorData(errorMsg="templateName too long")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            rest = cm.addFactorTemplate(templateName,user_id,portfolioId=portfolioid)
            return JsonResponse(rest.toDict())
        elif doMethod=="getAllFactorTemplates":
            #获得本用户的全部指标组合列表
            user_id = request.session.get("user_id")
            portfolioId=reqData.get("portfolioId")
            TemplateType=reqData.get("TemplateType")
            if TemplateType is None:
                TemplateType="100"
            if TemplateType=="":
                rest.errorData(errorMsg="Please enter a TemplateType.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a PortfolioId.")
                return JsonResponse(rest.toDict())
            # print(TemplateType,user_id)  ##100 1967
            cm = factorMode.cfactorMode()
            qrest = cm.getAllFactorTemplate(user_id,portfolioId,templatetype=TemplateType)
            # print("apiqrest: ",qrest.factorTemplateDF)
            if not qrest.errorFlag:
                return JsonResponse(qrest.toDict())
            factorTemplateDF=qrest.factorTemplateDF
            templateList=[]
            templateFirst= {}
            for r in factorTemplateDF.itertuples():
                templateId=str(r.TEMPLATENO)
                templateName=str(r.TEMPLATENAME)
                templateType=str(r.TEMPTYPE)
                # print(templateId,templateName)
                #modify robby 区分自定义与系统模板11.15
                templateList.append({'templateNo':templateId,"templateName":templateName,"templateType":templateType})
            if len(templateList)>0:
                templateFirst=templateList[0]
            # print("templateList",templateList)
            rtdata={"templateList":templateList,"templateFirst":templateFirst,}
            rest.data=rtdata
            return JsonResponse(rest.toDict())
        elif doMethod=="searchPortfolioIdWithSameName":
            #获得本用户的全部指标组合列表
            user_id = request.session.get("user_id")
            Templatename=reqData.get("TemplateName")

            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if Templatename=="" or Templatename is None:
                rest.errorData(errorMsg="Please enter a Templatename.")
                return JsonResponse(rest.toDict())
            # print(TemplateType,user_id)  ##100 1967
            cm = factorMode.cfactorMode()
            qrest = cm.searchPortfolioIdWithSameName(templatename=Templatename, userId=user_id)
            return JsonResponse(qrest.toDict())
        elif doMethod=="addFactorTemplate":
            templateName=reqData.get("templateName")
            # 自选指标组合添加指标，可一次添加多指标用'|'分开
            if templateName=="" or templateName is None:
                rest.errorData(errorMsg="Please enter a templateName.")
                return JsonResponse(rest.toDict())
            user_id = request.session.get("user_id")
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            FactorSymbols = reqData.get("FactorSymbols")  ##FactorSymbols
            if FactorSymbols=="" or FactorSymbols is None:
                rest.errorData(errorMsg="Please enter a FactorSymbols.")
                return JsonResponse(rest.toDict())

            factorNoList = str(FactorSymbols).split("|")
            ffnos=[]
            for fno in factorNoList:
                ffnostr=str(fno).strip()
                if len(ffnostr)==0:
                    continue
                ffnos.append(ffnostr)
            if len(ffnos)<=0:
                rest.errorData(errorMsg="指标数量必须大于零")
                return JsonResponse(rest.toDict())

            portfolioId = reqData.get("portfolioId")
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolioId.")
                return JsonResponse(rest.toDict())
            templateName=str(templateName).strip()
            ckrts = tools_utils.charLength(templateName,20)
            if not ckrts.errorFlag:
                rest.errorData(errorMsg="templateName too long")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            rest = cm.createTemplateAndFactors(templatename=templateName,userId=user_id,factornos=ffnos,portfolioId=portfolioId)
            return JsonResponse(rest.toDict())
        elif doMethod=="copyFactorTemplate":
            templateNo=reqData.get("templateNo")
            if templateNo=="" or templateNo is None:
                rest.errorData(errorMsg="Please enter a templateNo.")
                return JsonResponse(rest.toDict())
            user_id = request.session.get("user_id")
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            # 要拷贝到的股票列表，可一次添加多指标用'|'分开
            portfolioIdsTo = reqData.get("portfolioIdsTo")  ##FactorSymbols
            if portfolioIdsTo=="" or portfolioIdsTo is None:
                rest.errorData(errorMsg="Please enter a portfolioIdsTo.")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            rest = cm.copyFactorTemplate(portfolioIdsTo=portfolioIdsTo,templateNo=templateNo,userId=user_id)
            return JsonResponse(rest.toDict())
        elif doMethod == "copyFactorTemplatewithNameChange":
            templateNo = reqData.get("templateNo")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a templateNo.")
                return JsonResponse(rest.toDict())
            user_id = request.session.get("user_id")
            if user_id == "" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            # 要拷贝到的股票列表，可一次添加多指标用'|'分开
            portfolioIdsTo = reqData.get("portfolioIdsTo")  ##FactorSymbols
            if portfolioIdsTo == "" or portfolioIdsTo is None:
                rest.errorData(errorMsg="Please enter a portfolioIdsTo.")
                return JsonResponse(rest.toDict())
            templateName = reqData.get("templateName")  ##jsr templateName
            cm = factorMode.cfactorMode()
            rest = cm.copyFactorTemplatewithNameChange(portfolioIdsTo=portfolioIdsTo, templateNo=templateNo, userId=user_id,templatename=templateName)
            return JsonResponse(rest.toDict())
        elif doMethod=="addFactorSymbol":
            #自选指标组合添加指标，可一次添加多指标用'|'分开
            symbols=reqData.get("FactorSymbols") ##FactorSymbols
            templateNo=reqData.get("templateNo")
            user_id = request.session.get("user_id")
            if symbols=="" or symbols is None:
                rest.errorData(errorMsg="Please enter a FactorSymbols.")
                return JsonResponse(rest.toDict())
            if templateNo=="" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            # "28699|28822|28843|28788|28812|28762|28769"
            symbols=str(symbols).split("|")
            for symbol in symbols:
                cm = factorMode.cfactorMode()
                factor_rst = cm.addFactorSymbol(symbol,templateNo,user_id)
                if not factor_rst.errorFlag:
                    return JsonResponse(factor_rst.toDict())
            return JsonResponse(rest.toDict())
        elif doMethod=="delFactorSymbol":
            #删除单个指标
            symbols = reqData.get("FactorSymbols")  ##FactorSymbols
            templateNo = reqData.get("templateNo")
            user_id = request.session.get("user_id")
            if symbols=="" or symbols is None:
                rest.errorData(errorMsg="Please enter a FactorSymbols.")
                return JsonResponse(rest.toDict())
            if templateNo=="" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            factor_rst = cm.delFactorSymbol(symbols, templateNo, user_id)
            if not factor_rst.errorFlag:
                return JsonResponse(factor_rst.toDict())
            return JsonResponse(factor_rst.toDict())
        elif doMethod=="delFactorTemplate":
            #删除组合
            templateNo=reqData.get("templateNo")
            user_id = request.session.get("user_id")
            if templateNo=="" or templateNo is None:
                rest.errorData(errorMsg="Please enter a templateFactor.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            qrest = cm.delFactorTemplate(templateNo,user_id)
            if not qrest.errorFlag:
                return JsonResponse(qrest.toDict())
            return JsonResponse(qrest.toDict())
        elif doMethod=="parseFileForSymbols":
            #删除组合
            Symbols=reqData.get("Symbols")
            if Symbols=="" or Symbols is None:
                rest.errorData(errorMsg="Please enter a Symbols.")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            qrest = cm.parseFileForSymbols(Symbols)
            return JsonResponse(qrest.toDict())
        elif doMethod=="parseFileForSymbols_moreinfo":
            #删除组合
            Symbols=reqData.get("Symbols")
            if Symbols=="" or Symbols is None:
                rest.errorData(errorMsg="Please enter a Symbols.")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            qrest = cm.parseFileForSymbols_moreinfo(Symbols)
            return JsonResponse(qrest.toDict())
        elif doMethod=="editDefaultFactorName":
            #自选指标组合添加指标，可一次添加多指标用'|'分开
            symbols=reqData.get("FactorSymbols") ##FactorSymbols
            templateNo=reqData.get("templateNo")
            symbolnames = reqData.get("FactorSymbolNames")
            if symbols=="" or symbols is None:
                rest.errorData(errorMsg="Please enter FactorSymbols.")
                return JsonResponse(rest.toDict())
            if symbolnames=="" or symbolnames is None:
                rest.errorData(errorMsg="Please enter FactorSymbolNames.")
                return JsonResponse(rest.toDict())
            if templateNo=="" or templateNo is None:
                rest.errorData(errorMsg="Please enter a Factortemplate.")
                return JsonResponse(rest.toDict())
            # "28699|28822|28843|28788|28812|28762|28769"
            cm = factorMode.cfactorMode()
            factor_rst = cm.editDefaultFactorName(templateNo=templateNo,factorNoList=symbols,factorNameList=symbolnames)
            return JsonResponse(factor_rst.toDict())
        elif doMethod=="getTemplateFactors":
            #获取指标组合列表
            templateNo = reqData.get("templateNo")
            user_id = request.session.get("user_id")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a templateFactor.")
                return JsonResponse(rest.toDict())
            if user_id == "" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = factorMode.cfactorMode()
            qrest = cm.getTemplateFactors(templateNo, user_id)
            # if not qrest.errorFlag:
            #     return JsonResponse(qrest.toDict())
            return JsonResponse(qrest.toDict())
        elif doMethod=="editFactorTemplate":
            #修改指标组合名称
            templateNo=reqData.get("templateNo")
            templateName=reqData.get("templateName")
            portfolioId=reqData.get("portfolioId")
            user_id = request.session.get("user_id")
            if templateNo == "" or templateNo is None:
                rest.errorData(errorMsg="Please enter a templateFactor.")
                return JsonResponse(rest.toDict())
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolioId.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())

            if templateName is None or templateName=="":
                templateName=""
            else:
                templateName = str(templateName).strip()
                ckrts = tools_utils.charLength(templateName, 20)
                if not ckrts.errorFlag:
                    rest.errorData(errorMsg="templateName too long")
                    return JsonResponse(rest.toDict())

            FactorSymbols = reqData.get("FactorSymbols")  ##FactorSymbols
            if FactorSymbols=="" or FactorSymbols is None:
                rest.errorData(errorMsg="Please enter a FactorSymbols.")
                return JsonResponse(rest.toDict())

            factorNoList = str(FactorSymbols).split("|")
            ffnos=[]
            for fno in factorNoList:
                ffnostr=str(fno).strip()
                if len(ffnostr)==0:
                    continue
                ffnos.append(ffnostr)
            if len(ffnos)<=0:
                rest.errorData(errorMsg="指标数量必须大于零")
                return JsonResponse(rest.toDict())

            cm = factorMode.cfactorMode()
            qrest = cm.editFactorTemplate(portfolioId,templateNo,templateName,user_id,ffnos)
            return JsonResponse(qrest.toDict())
        return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        # UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())

@userLoginCheckDeco
def factorMenuAPIHandler(request):
    #指标搜索
    rest = ResultData()
    try:
        Logger.debug("factorMenuAPIHandler request:"%request.GET)
        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        user_id = request.session.get("user_id")
        if doMethod=="searchFactors":  #指标搜索
            searchKey=reqData.get("searchKey")
            if searchKey=="" or searchKey is None:
                rest.data={"factorData":[],"factorMenu":[],}
                return JsonResponse(rest.toDict())
            indexType="4"
            rest = factorMenu().searchFactors(searchKey,indexType)
            return JsonResponse(rest.toDict())
        elif doMethod=="getMenuFactor":  #指标菜单
            indexType="4"
            rest = factorMenu().getMenuFactor(indexType)
            return JsonResponse(rest.toDict())
        elif doMethod=="getFactorByMenu":  #菜单下指标  + 搜索指标
            categoryId=reqData.get("categoryId")
            indexType="4"
            rest = factorMenu().getFactorByMenu(categoryId,indexType,searchKey=None)
            return JsonResponse(rest.toDict())
        elif doMethod=="getFactorMenuCache":  #所有菜单指标数据
            indexType="4"
            rest = factorMenu().getFactorMenuCache(indexType)
            return JsonResponse(rest.toDict())
        return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        # UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())