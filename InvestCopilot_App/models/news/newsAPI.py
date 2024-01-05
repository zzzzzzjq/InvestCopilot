import datetime
import json
import re
import os
from mistune import markdown as mistune_markdown # mistune-0.8.4 才显示正常
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.news.newsMode import newsMode
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.business.likeMode import likeMode
from InvestCopilot_App.models.news.newsTitleMode import newsTitleMode
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
from  InvestCopilot_App.models.market.snapMarket import snapUtils
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
from django.http import JsonResponse
Logger = logger_utils.LoggerUtils()

@userLoginCheckDeco
def newsAPIHandler(request):
    rest = ResultData()
    try:
        user_id = request.session.get("user_id")
        rest = tools_utils.requestDataFmt(request, fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod = reqData.get("doMethod")
        # Logger.debug("newsAPIHandler request:%s"%reqData)
        if doMethod == "getNewsDataByTitleTag":
            searchTitle = reqData.get("searchTitle")#标题搜索
            beginDate = reqData.get("beginDate")
            endDate = reqData.get("endDate")
            titleTag = reqData.get("titleTag")#主题
            ordTag = reqData.get("ordTag")#排序字段 updateTime show_total_score
            gtScore = reqData.get("gtScore")#大于分数
            ltScore = reqData.get("ltScore")#小于分数
            news_class = reqData.get("news_class")#新闻类别
            if titleTag is None:
                rest = rest.errorData(errorMsg="请选择新闻主题")
                return JsonResponse(rest.toDict())
            if news_class is not None:
                if str(news_class) not in mg_cfg.news_class_dt.keys():
                    rest = rest.errorData(errorMsg="新闻类别参数错误")
                    return JsonResponse(rest.toDict())
            if gtScore is None and ltScore is None:
                gtScore = 0
                ltScore = 100
            else:
                if not tools_utils.isNumber(gtScore):
                    rest = rest.errorData(errorMsg="gtScore必须为数值")
                    return JsonResponse(rest.toDict())
                else:
                    gtScore=float(gtScore)
                if not tools_utils.isNumber(ltScore):
                    rest = rest.errorData(errorMsg="ltScore必须为数值")
                    return JsonResponse(rest.toDict())
                else:
                    ltScore = float(ltScore)
            page = reqData.get("page")
            pageSize = reqData.get("pageSize")

            if beginDate is None or beginDate=="":
                beginDate=None
            if endDate is None or endDate=="":
                endDate=None

            if page is None:
                page = 1
            else:
                page = int(page)

            if pageSize is None:
                pageSize = 20
            else:
                pageSize = int(pageSize)
            translation = reqData.get("translation")  # 中英文显示  zh  en
            if translation is None:
                translation = 'zh'
            vtags=['news']
            if titleTag in mg_cfg.news_total_title_gt_dt:
                title_cores=mg_cfg.news_total_title_gt_dt[titleTag]
            else:
                title_cores=0.73
            title_cores=0.73#無用
            # title_cores=round(title_cores,4)
            queryTitle=None
            if searchTitle is not None:
                searchTitle = str(searchTitle).strip()
                if len(searchTitle)>0:
                    queryTitle=searchTitle
            c_user_dt = user_utils.getCacheUserInfo(user_id)
            if c_user_dt is None:
                rest.errorData(errorMsg="用户信息异常，请重新选择！")
                return JsonResponse(rest.toDict())
            companyId = c_user_dt['COMPANYID']
            rest = newsMode().getNewsDataByTitleTag(titleTag, title_cores,gtScore, ltScore,vtags=vtags,
                                                    translation=translation,ordTag=ordTag,queryTitle=queryTitle,news_class=news_class,
                                                    beginDate=beginDate,endDate=endDate, page=page,pageSize=pageSize,companyId=companyId)
            stockInfo_dt=cache_dict.getStockInfoDT()
            if rest.errorFlag:
                rest_dt=rest.data
                markIds=rest_dt.pop("markIds")
                firstCodes=rest_dt.pop("firstCodes")
                if len(markIds)>0:
                    markRest=likeMode().getMarkData(user_id,markIds,markType="news")
                    if markRest.errorFlag:
                        rest_dt['markData']=markRest.data
                if len(firstCodes)>0:
                    firstCodes=list(set(firstCodes))
                    emminhqDf = snapUtils().getRealStockMarketByWindCode(firstCodes)
                    if emminhqDf.empty:
                        return JsonResponse(rest.toDict())
                    emminhqDf['NOWPRICE'] = emminhqDf['NOWPRICE'].apply(
                        lambda x: tools_utils.formatDigit(x, mode='{:.2f}'))
                    emminhqDf['PCTCHANGE'] = emminhqDf['PCTCHANGE'].apply(
                        lambda x: tools_utils.formatDigit(x, mode='{:.2f}'))
                    rtdata = emminhqDf[["WINDCODE", "NOWPRICE", "PCTCHANGE"]].rename(
                        columns=lambda x: x.capitalize()).to_json(orient='records')
                    pctls={}
                    for r_dt in json.loads(rtdata):
                        Windcode = r_dt['Windcode']
                        if Windcode in  stockInfo_dt:
                            stockInfo_s = stockInfo_dt[Windcode]
                            sarea=stockInfo_s['Area']
                            if sarea in ['AM']:
                                pctls[Windcode]={'Windcode': Windcode, 'Stockname':stockInfo_s['Stockcode'], 'Nowprice':  r_dt['Nowprice'], 'Pctchange': r_dt['Pctchange']}
                            else:
                                pctls[Windcode]={'Windcode': Windcode, 'Stockname': stockInfo_s['Stockname'],'Nowprice': r_dt['Nowprice'], 'Pctchange': r_dt['Pctchange']}
                    nbaseData=[]
                    baseDatas = rest_dt.pop("data")
                    for bts in baseDatas:
                        firstCode = bts['windCode']
                        if firstCode in pctls:
                            pdt = pctls[firstCode]
                            bts["stockName"]=pdt['Stockname']
                            bts["pctChange"]=pdt['Pctchange']
                            bts["nowPrice"]=pdt['Nowprice']
                        nbaseData.append(bts)
                    rest_dt['data']= nbaseData
                return JsonResponse(rest_dt)

            return JsonResponse(rest.toDict())
        elif doMethod == "getNewsTitleTypes":#新闻主题分类
            dataType = reqData.get("dataType")  # 数据类型 news
            translation = reqData.get("translation")  # 中英文显示  zh  en
            # if translation is None:
            #     translation = 'zh'
            # if dataType is None:
            #     dataType="news"
            # if dataType in ['news']:
            #     if translation == 'zh' :
            #         rest.data=mg_cfg.news_titleTag_zh
            #     else:rest.data=mg_cfg.news_titleTag_en
            new_titles=newsTitleMode().getNewsTitle(user_id,translation)
            rest.data=new_titles
            return JsonResponse(rest.toDict())
        elif doMethod == "getNewsClassTypes":#新闻类别
            translation = reqData.get("translation")  # 中英文显示  zh  en
            if translation is None:
                translation = 'zh'
            rtdata=[]
            if translation=='zh':
                for k,v in mg_cfg.news_class_dt.items():
                    rtdata.append({"newTypeId":k,"newTypeName":v})
            else:
                for k,v in mg_cfg.news_class_dt.items():
                    rtdata.append({"newTypeId":k,"newTypeName":k})
            rest.data=rtdata
            return JsonResponse(rest.toDict())
        elif doMethod == "getNewsDaySummary":#新闻日摘要
            translation = reqData.get("translation")  # 中英文显示  zh  en
            title_id = reqData.get("title_id")
            if title_id=="" or title_id is None:
                rest.errorData(errorMsg="Please title_id a params.")
                return JsonResponse(rest.toDict())
            rest=newsMode().getNewsDaySummary(title_id,translation)
            return JsonResponse(rest.toDict())
        elif doMethod=="addMarkData":#添加新闻标注
            sid=reqData.get("sid")
            sMark=reqData.get("sMark")#IMP,NP,LP 重要 (Important) 一般Normal Priority (NP)   不重要  Low Priority (LP)
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if sMark=="" or sMark is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if str(sMark) not in ['IMP', 'NP',"LP","Cancel", "1", "2", "3", "4", "5"]:
                rest = rest.errorData(errorMsg='sMark参数错误')
                return JsonResponse(rest.toDict())
            rest = likeMode().addLike(sid,user_id,sMark,markType="news")
            return JsonResponse(rest.toDict())
        else:
            errorMsg = '抱歉，请求类别不存在，请重新选择。'
            rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())

    except Exception as ex:
        errorMsg = '抱歉，获取数据失败，请稍后重试。'
        Logger.errLineNo(msg=errorMsg)
        rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())

if __name__ == '__main__':
    pass