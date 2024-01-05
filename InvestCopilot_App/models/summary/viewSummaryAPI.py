import datetime
import json
import os
import re
from mistune import markdown as mistune_markdown # mistune-0.8.4 才显示正常
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.summary.viewSummaryMode import viewSummaryMode
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
from InvestCopilot_App.models.portfolio.portfolioMode import cportfolioMode
from InvestCopilot_App.models.manager import ManagerUserMode as manager_user_mode
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.business.likeMode import likeMode
from  InvestCopilot_App.models.market.snapMarket import snapUtils
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from django.http import JsonResponse
from django.http import HttpResponse
from django.http import StreamingHttpResponse
from urllib.parse import quote

Logger = logger_utils.LoggerUtils()


@userLoginCheckDeco
def viewSummaryAPIHandler(request):
    rest = ResultData()
    try:
        user_id = request.session.get("user_id")
        rest = tools_utils.requestDataFmt(request, fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        # 获取用户的配置参数
        user_privilegeset = None
        companyId = None
        userCfg_dt = cache_db.getUserConfig_dt()
        if user_id in userCfg_dt:
            userCfg = userCfg_dt[user_id]
            user_privilegeset = userCfg['PRIVILEGESET']
            companyId = str(userCfg['COMPANYID'])
        reqData = rest.data
        doMethod = reqData.get("doMethod")
        # Logger.debug("viewSummaryAPIHandler request:%s"%reqData)
        if doMethod == "getViewSummaryTitleByType":
            nt = datetime.datetime.now()
            page = reqData.get("page")
            pageSize = reqData.get("pageSize")
            #新闻重要性查询
            gtScore = reqData.get("gtScore")#大于分数
            ltScore = reqData.get("ltScore")#小于分数
            if gtScore is None:
                gtScore = 0
            else:
                if not tools_utils.isNumber(gtScore):
                    rest = rest.errorData(errorMsg="gtScore必须为数值")
                    return JsonResponse(rest.toDict())
                else:
                    gtScore=float(gtScore)
            if ltScore is None:
                ltScore = 100
            else:
                if not tools_utils.isNumber(ltScore):
                    rest = rest.errorData(errorMsg="ltScore必须为数值")
                    return JsonResponse(rest.toDict())
                else:
                    ltScore = float(ltScore)
            portfolioId = reqData.get("portfolioId")
            symbols = reqData.get("symbols")
            dataTypes = reqData.get("dataTypes")
            beginTime = reqData.get("beginTime")
            endTime = reqData.get("endTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-5)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            translation = reqData.get("translation")
            newsDays = reqData.get("newsDays")
            if newsDays is None:
                newsDays = 1
            else:
                if tools_utils.isNumber(newsDays):
                    newsDays = int(newsDays)
                else:
                    newsDays = 1
            vtags = dataTypes
            if dataTypes is None or dataTypes == "":
                vtags = ["news"]
            if dataTypes == "transcripts":
                vtags = ['transcripts', 'Press Releases', 'EarningsCallPresentation']
            # if dataTypes == "Audio":#电话会议数据不展示
            #     rest.data=[]
            #     return JsonResponse(rest.toDict())
            if isinstance(vtags,str):
                vtags=[vtags]
            if translation is None:
                translation = "zh"
            if companyId is None or companyId == "":
                companyIds = []
            else:
                companyIds = [companyId]

            if user_privilegeset == "super":
                companyInfoPdData = manager_user_mode.getAllCompanyInfo()
                companyInfoPdData = tools_utils.dfColumUpper(companyInfoPdData)
                sysCompanyIds = companyInfoPdData['COMPANYID'].values.tolist()
                for fcid in sysCompanyIds:
                    companyIds.append(fcid)
                companyIds = list(set(companyIds))
            else:
                #查询当前公司下的所有会议
                pass
            if page is None:
                page = 1
            else:
                page = int(page)

            if pageSize is None:
                pageSize = 20
            else:
                pageSize = int(pageSize)
            if symbols is None:
                symbols = []
            else:
                symbols = re.split("[|,]", str(symbols))
            if portfolioId is None and len(symbols) == 0:
                rest = rest.errorData(errorMsg="参数不合法，请重新选择")
                return JsonResponse(rest.toDict())
            # print("portfolioId:",portfolioId)
            # print("user_id:",user_id)
            rest = viewSummaryMode().getSummaryViewByTag(vtags=vtags, symbols=symbols, queryId=portfolioId,
                                                         translation=translation, userId=user_id,
                                                         companyIds=companyIds, page=page,
                                                         pageSize=pageSize,gtScore=gtScore, ltScore=ltScore)
            if rest.errorFlag:
                markIds=rest.markIds
                if len(markIds)>0:
                    if "news" in vtags:
                        markType="news"
                    else:
                        markType="summary"
                    markRest=likeMode().getMarkData(user_id,markIds,markType=markType)
                    if markRest.errorFlag:
                        rest.markData=markRest.data
                        rest.markIds=[]
                        return JsonResponse(rest.toDict())
            return JsonResponse(rest.toDict())
        elif doMethod == "getViewSummaryByTag":#替换getViewSummaryTitleByType接口
            nt = datetime.datetime.now()
            page = reqData.get("page")
            pageSize = reqData.get("pageSize")

            #新闻重要性查询
            gtScore = reqData.get("gtScore")#大于分数
            ltScore = reqData.get("ltScore")#小于分数
            if gtScore is None:
                gtScore = 0
            else:
                if not tools_utils.isNumber(gtScore):
                    rest = rest.errorData(errorMsg="gtScore必须为数值")
                    return JsonResponse(rest.toDict())
                else:
                    gtScore=float(gtScore)
            if ltScore is None:
                ltScore = 100
            else:
                if not tools_utils.isNumber(ltScore):
                    rest = rest.errorData(errorMsg="ltScore必须为数值")
                    return JsonResponse(rest.toDict())
                else:
                    ltScore = float(ltScore)

            portfolioId = reqData.get("portfolioId")
            symbols = reqData.get("symbols")
            dataTypes = reqData.get("dataTypes")
            beginTime = reqData.get("beginTime")
            endTime = reqData.get("endTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-5)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            translation = reqData.get("translation")
            newsDays = reqData.get("newsDays")
            if newsDays is None:
                newsDays = 1
            else:
                if tools_utils.isNumber(newsDays):
                    newsDays = int(newsDays)
                else:
                    newsDays = 1
            vtags = dataTypes
            if dataTypes is None or dataTypes == "":
                vtags = ["news"]
            if dataTypes == "transcripts":
                vtags = ['transcripts', 'Press Releases', 'EarningsCallPresentation']
            # if dataTypes == "Audio":#电话会议数据不展示
            #     rest.data=[]
            #     return JsonResponse(rest.toDict())
            if isinstance(vtags,str):
                vtags=[vtags]
            if translation is None:
                translation = "zh"
            if companyId is None or companyId == "":
                companyIds = []
            else:
                companyIds = [companyId]

            if user_privilegeset == "super":
                companyInfoPdData = manager_user_mode.getAllCompanyInfo()
                companyInfoPdData = tools_utils.dfColumUpper(companyInfoPdData)
                sysCompanyIds = companyInfoPdData['COMPANYID'].values.tolist()
                for fcid in sysCompanyIds:
                    companyIds.append(fcid)
                companyIds = list(set(companyIds))
            else:
                #查询当前公司下的所有会议
                pass
            if page is None:
                page = 1
            else:
                page = int(page)

            if pageSize is None:
                pageSize = 20
            else:
                pageSize = int(pageSize)
            if symbols is None:
                symbols = []
            else:
                symbols = re.split("[|,]", str(symbols))
            if portfolioId is None and len(symbols) == 0:
                rest = rest.errorData(errorMsg="参数不合法，请重新选择")
                return JsonResponse(rest.toDict())
            # print("portfolioId:",portfolioId)
            # print("user_id:",user_id)
            rest = viewSummaryMode().getSummaryViewByTag(vtags=vtags, symbols=symbols, queryId=portfolioId,
                                                         translation=translation, userId=user_id,
                                                         companyIds=companyIds, page=page,
                                                         pageSize=pageSize,gtScore=gtScore,ltScore=ltScore)
            stockInfo_dt=cache_dict.getStockInfoDT()
            if rest.errorFlag:
                new_rest=ResultData()
                markIds=rest.markIds
                firstCodes=rest.firstCodes
                summaryData=rest.data
                rtfmtdata = {"data": summaryData}
                if len(markIds)>0:
                    if "news" in vtags:
                        markType="news"
                    else:
                        markType="summary"
                    markRest=likeMode().getMarkData(user_id,markIds,markType=markType)
                    if markRest.errorFlag:
                        rtfmtdata["markData"] = markRest.data
                if len(firstCodes) > 0:
                    emminhqDf = snapUtils().getRealStockMarketByWindCode(firstCodes)
                    if emminhqDf.empty:
                        return JsonResponse(rest.toDict())
                    emminhqDf['NOWPRICE'] = emminhqDf['NOWPRICE'].apply(
                        lambda x: tools_utils.formatDigit(x, mode='{:.2f}'))
                    emminhqDf['PCTCHANGE'] = emminhqDf['PCTCHANGE'].apply(
                        lambda x: tools_utils.formatDigit(x, mode='{:.2f}'))
                    rtdata = emminhqDf[["WINDCODE", "NOWPRICE", "PCTCHANGE"]].rename(
                        columns=lambda x: x.capitalize()).to_json(orient='records')
                    pctls = {}
                    for r_dt in json.loads(rtdata):
                        Windcode = r_dt['Windcode']
                        if Windcode in stockInfo_dt:
                            stockInfo_s = stockInfo_dt[Windcode]
                            sarea = stockInfo_s['Area']
                            if sarea in ['AM']:
                                pctls[Windcode] = {'Windcode': Windcode, 'Stockname': stockInfo_s['Stockcode'],
                                                   'Nowprice': r_dt['Nowprice'], 'Pctchange': r_dt['Pctchange']}
                            else:
                                pctls[Windcode] = {'Windcode': Windcode, 'Stockname': stockInfo_s['Stockname'],
                                                   'Nowprice': r_dt['Nowprice'], 'Pctchange': r_dt['Pctchange']}
                    # rtfmtdata['marketData'] = {"data": pctls, "records": len(pctls)}
                    nbaseData = []
                    for bts in summaryData:
                        firstCode = bts['windCode']
                        if firstCode in pctls:
                            pdt = pctls[firstCode]
                            bts["stockName"] = pdt['Stockname']
                            bts["pctChange"] = pdt['Pctchange']
                            bts["nowPrice"] = pdt['Nowprice']
                        nbaseData.append(bts)
                    rtfmtdata['data'] = nbaseData
                new_rest.data=rtfmtdata
                return JsonResponse(new_rest.toDict())
            return JsonResponse(rest.toDict())
        elif doMethod == "getStrategySumViewByTag":#宏观策略接口
            nt = datetime.datetime.now()
            page = reqData.get("page")
            pageSize = reqData.get("pageSize")
            translation = reqData.get("translation")

            portfolioId = reqData.get("portfolioId")
            symbols = reqData.get("symbols")
            dataTypes = reqData.get("dataTypes")
            beginTime = reqData.get("beginTime")
            endTime = reqData.get("endTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-5)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            if translation is None:
                translation = "zh"
            if page is None:
                page = 1
            else:
                page = int(page)

            if pageSize is None:
                pageSize = 20
            else:
                pageSize = int(pageSize)
            if symbols is None:
                symbols = []
            else:
                symbols = re.split("[|,]", str(symbols))
            # if portfolioId is None and len(symbols) == 0:
            #     rest = rest.errorData(errorMsg="参数不合法，请重新选择")
            #     return JsonResponse(rest.toDict())
            # print("portfolioId:",portfolioId)
            # print("user_id:",user_id)
            rest = viewSummaryMode().getStrategySumViewByTag(queryId="hgcl",translation=translation,page=page,pageSize=pageSize)
            return JsonResponse(rest.toDict())
        elif doMethod == "getDocContent":
            qid = reqData.get("id")  # 文档编号
            datafmt = reqData.get("datafmt")
            secondType = reqData.get("secondType")
            translation = reqData.get("translation")  # 中英文显示  zh  en
            if translation is None:
                translation = 'zh'
            vtransSummaryText = reqData.get("vtransSummaryText")  # 获取修正原文 1:
            vsummary = reqData.get("vSourceContents")  # 是否显示原文 默认不返回 0:全部返回
            if vsummary is None:
                vsummary = "1"
            ids = str(qid).split(",")
            rest = viewSummaryMode().getDocContent(ids)
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            rtdata = rest.data
            qdata = []
            Slable = ""
            Scomments = ""
            if rtdata['count'] > 0:
                qdata = rtdata['data']
            if str(vtransSummaryText) == "1":
                if len(qdata) > 0:
                    qdt = qdata[0]
                    qdt["summaryText"] = ""  # 原文
                    qdt["summary"] = ""  # 中文摘要
                    qdt["summary_en"] = ""  # 英文摘要
                    qdt['attachments']=""
                    qdt["local_path"] = ""
                    qdata = [qdt]
                    rest.data = qdata
                    return JsonResponse(rest.toDict())
            if datafmt in ['link']:
                # 股票代码转换
                if len(qdata) > 0:
                    qdt = qdata[0]
                    if "relationCompanies" in qdt:
                        cpwindowdt = {}
                        cpTickers = qdt['relationCompanies']
                        # print("cpTickers:",cpTickers)
                        if "company" in cpTickers:
                            cpcompanys = cpTickers["company"]
                            for company in cpcompanys:
                                if "symbol" in company and "english_name" in company and "chinese_name" in company:
                                    symbol = company['symbol']
                                    if str(symbol).strip() == "":
                                        continue
                                    english_name = company['english_name']
                                    chinese_name = company['chinese_name']
                                    symbolls = str(symbol).split(".")
                                    if len(symbolls) == 1:
                                        ticker = symbolls[0]
                                        tickerInfo = mg_cfg.getWindCodeInfo(ticker)
                                        if isinstance(tickerInfo, dict):
                                            ticker = tickerInfo['windCode']
                                            codeName = ticker  #
                                            # codeName = tickerInfo['bbgName']
                                        else:
                                            continue
                                        unit = "US"
                                    else:
                                        ticker = symbol
                                        codeName = ticker
                                        if str(ticker).endswith(".SS"):
                                            ticker = str(ticker).replace(".SS", '.SH')
                                        elif str(ticker).endswith(".HK"):
                                            ticker = str(ticker).replace(".HK", '').zfill(4) + ".HK"
                                        codeRst = mg_cfg.getStockInfoByWindCodes(ticker)
                                        if codeRst.errorFlag:
                                            codes = codeRst.data
                                            if len(codes) >= 1:
                                                ticker = codes[0]['windCode']
                                                unit = codes[0]['unit']
                                                codeName = codes[0]['zhName']
                                            else:
                                                continue
                                        else:
                                            continue
                                    cpwindowdt[symbol] = {'chinese_name': chinese_name, 'english_name': english_name,
                                                          'ticker': ticker, "tickerName": codeName, 'unit': unit}
                        if "company" in cpTickers:
                            if translation in ['zh']:
                                summary = qdt['summary']
                            else:
                                summary = qdt['summary_en']
                            if isinstance(summary, list):
                                summarystr = "\n".join(summary)
                            else:
                                summarystr = summary
                            if translation in ['zh']:
                                bidx = summarystr.find("1. 行业/公司：")
                                eidx = summarystr.find("摘要：", len("1. 行业/公司："))
                            else:
                                bidx = summarystr.find("1. Industries/Companies:")
                                eidx = summarystr.find("Abstract:", len("1. Industries/Companies:"))
                            summarystr1 = summarystr[bidx:eidx]
                            summarystr2 = summarystr[eidx:]
                            cktickers = []
                            for symbol, wdt in cpwindowdt.items():
                                chinese_name = wdt['chinese_name']
                                english_name = wdt['english_name']
                                cktickers.append(wdt)
                                ticker = wdt['ticker']
                                unit = wdt['unit']
                                vticker = ticker
                                if unit in ["US"]:
                                    vticker = str(ticker).split(".")[0]
                                tickerName = wdt['tickerName']
                                # print("tickerName:",tickerName)
                                # 特别讨论了Sunny Optical（舜宇光学科技）、BYD（比亚迪）、Gao Wei Electronics（高伟电子）、Zhuhai Crown Royal（珠海皇冠皇家）、Luxshare Precision（立讯精密）和Apple（苹果）
                                # 替换为 舜宇光学(sss.HK) ...
                                # <a href="javascript:goSymbolPage('summaryview.html','SCHW')">SCHW.N  嘉信理财(Charles Schwab)</a>
                                summarystrx = summarystr1.replace(symbol,
                                                                  f"""<a  href="javascript:goSymbolPage('summaryview.html','{ticker}')">{vticker}</a>""")
                                if summarystrx == summarystr1:
                                    summarystrx = summarystr1.replace(english_name,
                                                                      f"""<a href="javascript:goSymbolPage('summaryview.html','{ticker}')">{vticker}</a>""")
                                    if summarystrx == summarystr1:
                                        summarystr1 = summarystr1.replace(chinese_name,
                                                                          f"""<a href="javascript:goSymbolPage('summaryview.html','{ticker}')">{vticker}</a>""")
                                    else:
                                        summarystr1 = summarystrx
                                else:
                                    summarystr1 = summarystrx  #
                            if translation in ['zh']:
                                endsummary = "会议摘要：\n" + summarystr1 + summarystr2
                            else:
                                endsummary = "Meeting Summary:\n" + summarystr1 + summarystr2
                            endsummary = str(endsummary).replace('href="javascript:',
                                                                 ' style="font-weight: bold" href="javascript:')
                            # if translation in ['zh']:
                            #     qdt['summary']=endsummary
                            # else:
                            #     qdt['summary_en'] = endsummary
                            qdt['cktickers'] = cktickers
                            qdata = [qdt]
                            rest.data = qdata
                            return JsonResponse(rest.toDict())
            if str(vsummary) == "1":
                if len(qdata) > 0:
                    qdt = qdata[0]
                    qdt["summaryText"] = ""  # 原文
                    qdt["local_path"] = ""
                    if "outputType" not in qdt:
                        qdt["outputType"] = "text"  # 研报、电话会议、电话业绩会议
                    if qdt["dataType"] in ["research", 'Economics & Strategy']:#宏观策略
                        qdt["source_url"] = ""
                        qdt["html_url"] = ""
                        qdt["view_url"] = ""
                        qdt["local_html"] = ""
                    # print("aaaa",qdt["summary"])
                    # print("qdt['dataType']:",qdt['dataType'])
                    if  qdt['dataType'] in ["research","Audio","Press Releases","news", 'Economics & Strategy']:
                        if translation in ['en']:
                            fmt_summary = qdt['summary_en']
                            if "title_en" in qdt:
                                qdt["title"] = qdt.pop("title_en")
                            qdt["summary_en"] = ""
                        else:
                            fmt_summary = qdt['summary']
                            if "title_zh" in qdt:
                                qdt["title"] = qdt.pop("title_zh")
                            qdt["summary_en"] = ""
                        if qdt['outputType']=="text" and qdt['dataType'] in ["research","Press Releases"]:
                            #历史数据矫正 将txt转markdown模式
                            fmt_summary=researchContentFmt(fmt_summary,translation)
                            qdt["outputType"] = "markdown"#都按markdown展示
                        else:
                            qdt["outputType"] = "markdown"#都按markdown展示

                        if qdt["outputType"] == "markdown":
                            qdt["summary"] = mistune_markdown(fmt_summary, extensions=["nl2br", "tables"],
                                                                  hard_wrap=True)
                        #设定返回的 audio_path
                        if "source_url" in qdt:
                            _audio_path = qdt['source_url']
                            _cuserId=""
                            if "cuserId" in qdt:
                                _cuserId = qdt['cuserId']
                            if "forward" in qdt:
                                fpath,fname = os.path.split(_audio_path)
                                #https://www.intellistock.cn/fileupload/audio/051/cicc.mp3
                                qdt['audio_path']="https://www.intellistock.cn/fileupload/audio/%s/%s"%(_cuserId,fname)
                    elif qdt['dataType'] == "innerResearch":
                        qdt["outputType"] = "markdown"
                        fmtattachments=[]#多个附件
                        if "attachments" in qdt:
                            attachments=qdt['attachments']
                            for attachment in attachments:
                                attachmentId=attachment['attachmentId']
                                attachmentName=attachment['attachmentName']
                                fileType=attachment['fileType']
                                fmtattachments.append({"attachmentId":attachmentId,"attachmentName":attachmentName,"fileType":fileType})
                            qdt['attachments']=fmtattachments
                        qdt["title"] = qdt['title_zh']
                        if translation in ['en']:
                            qdt["summary_zh"] = ""
                            if "title_en" in qdt:
                                qdt["title"] = qdt['title_en']

                    elif qdt['dataType'] == "transcripts":
                        outputType = "text"
                        if "outputType" in qdt:
                            outputType = qdt['outputType']
                        if translation in ['zh']:
                            summary = qdt['summary']
                            if outputType == "markdown":
                                qdt["summary"] = mistune_markdown(summary, extensions=["nl2br", "tables"],hard_wrap=True)
                                qdt["outputType"] = "markdown"
                            qdt["summary_en"] = ""
                            if "title_zh" in qdt:
                                qdt["title"] = qdt['title_zh']
                        else:
                            summary = qdt['summary_en']
                            if outputType == "markdown":
                                qdt["summary"] = mistune_markdown(summary, extensions=["nl2br", "tables"],hard_wrap=True)
                                qdt["outputType"] = "markdown"
                            else:
                                qdt["summary"] = summary
                            qdt["summary_en"] = ""
                            if "title_en" in qdt:
                                qdt["title"] = qdt['title_en']
                        if outputType =='text':#对历史的数据转markdown
                            qdt['outputType']='markdown'
                            vhissummary=hisTranscriptsContentFmt(qdt["summary"],translation)
                            vhissummary = str(vhissummary).replace("**Question:**", "**\nQuestion:**") \
                                .replace("**Question：**", "\n**Question:**")\
                                .replace("**回答:**","\n**回答：**").replace("**回答：**","\n**回答：**") \
                                .replace("**答案:**","\n**答案：**").replace("**答案：**","\n**答案：**")
                            vhissummary = mistune_markdown(vhissummary, extensions=["nl2br", "tables"],hard_wrap=True)
                            qdt["summary"] =vhissummary

                        # 翻译版本
                        if secondType == "fanyi":
                            if "contents_translation" in qdt:
                                contents_translation = qdt["contents_translation"]
                                vsummary = contents_translation['contents']
                                qdt["outputType"] = contents_translation['outputType']
                                #替换第一个开头的#
                                vsummary=replace_first_hash(str(vsummary).strip(),"")
                                qdt["summary"] =  mistune_markdown(vsummary, extensions=["nl2br", "tables"],hard_wrap=True)
                            elif "summarys" in qdt:
                                summarys = qdt["summarys"]
                                if (len(summarys) == 3):  # 两版摘要 第3个是翻译
                                    vsummary = summarys[2]  # 翻译
                                elif (len(summarys) == 2):  # 两版摘要 第2个是翻译
                                    vsummary = summarys[1]  # 第二版
                                else:
                                    vsummary = qdt["summary"]
                                qdt["outputType"] = "markdown"
                                qdt["summary"] = mistune_markdown(vsummary, extensions=["nl2br", "tables"],hard_wrap=True)
                            else:
                                if outputType=="markdown":
                                    qdt["summary"] = mistune_markdown(qdt['transSummaryText'], extensions=["nl2br", "tables"],hard_wrap=True)
                    else:
                        if translation in ['zh']:
                            qdt["summary_en"] = ""
                            if "title_zh" in qdt:
                                qdt["title"] = qdt['title_zh']
                        else:
                            rt = qdt["summary"]
                            if "summary_en" in qdt:
                                rt = qdt["summary_en"]
                                qdt["summary_en"] = ""
                            qdt["summary"] = rt
                            if "title_en" in qdt:
                                qdt["title"] = qdt['title_en']
                        if str(qid).startswith("Trans_"):
                            if "contents_translation" in qdt:
                                contents_translation = qdt["contents_translation"]
                                vsummary = contents_translation['contents']
                                qdt["outputType"] = contents_translation['outputType']
                                # 替换第一个开头的#
                                vsummary = replace_first_hash(str(vsummary).strip(), "")
                                qdt["summary"] = mistune_markdown(vsummary, extensions=["nl2br", "tables"],
                                                                  hard_wrap=True)

                    qdt["transSummaryText"] = ""  # 修正后原文
                    qdt["summarys"] = ""
                    qdt["summarys2"] = ""
                    qdt['title_en'] = ""
                    qdt['title_zh'] = ""
                    #
                    qdata = [qdt]
                    rest.data = qdata
                    return JsonResponse(rest.toDict())
            return JsonResponse(rest.toDict())
        elif doMethod == "addComments":
            # 添加评论
            sid = reqData.get("sid")
            slable = reqData.get("slable")
            scomments = reqData.get("scomments")
            user_id = request.session.get("user_id")
            if slable == "" or slable is None:
                rest.errorData(errorMsg="Please choose a data.")
                return JsonResponse(rest.toDict())
            if sid == "" or sid is None:
                rest.errorData(errorMsg="Please choose a data.")
                return JsonResponse(rest.toDict())
            if scomments == "" or scomments is None:
                rest.errorData(errorMsg="Please choose a data.")
                return JsonResponse(rest.toDict())
            # 评论数据检查
            if tools_utils.charLength(scomments, 1024).errorFlag is False:
                rest = rest.errorData(errorMsg='评论内容超过字数限制')
                return JsonResponse(rest.toDict())
            if str(slable) not in ['P', 'N']:
                rest = rest.errorData(errorMsg='Like参数错误')
                return JsonResponse(rest.toDict())
            scomments = tools_utils.strEncode(scomments)
            rest = cportfolioMode().addComments(user_id, sid, label=slable, labelText=scomments)
            return JsonResponse(rest.toDict())

        return JsonResponse(rest.toDict())

    except Exception as ex:
        errorMsg = '抱歉，获取数据失败，请稍后重试。'
        Logger.errLineNo(msg=errorMsg)
        rest.errorData(errorMsg=errorMsg)
        # dh_user_utils.UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())

@userLoginCheckDeco
def sesearchAPIHandler(request):
    rest = ResultData()
    try:
        user_id = request.session.get("user_id")
        rest = tools_utils.requestDataFmt(request, fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        # 获取用户的配置参数
        user_privilegeset = None
        companyId = None
        userCfg_dt = cache_db.getUserConfig_dt()
        if user_id in userCfg_dt:
            userCfg = userCfg_dt[user_id]
            companyId = str(userCfg['COMPANYID'])
        reqData = rest.data
        doMethod = reqData.get("doMethod")
        if doMethod == "getResearchData":
            # pdf展示
            sid = reqData.get("fileId")
            returnType = reqData.get("returnType")
            if sid == "" or sid is None:
                rest.errorData(errorMsg="Please choose a data.")
                return JsonResponse(rest.toDict())
            if returnType not in ['file', 'stream']:
                returnType = 'file'
            if companyId not in [tools_utils.globa_companyId]:
                # 对外公司不允许查看高盛报告
                if str(sid).startswith("mq_"):
                    rest.errorData(errorMsg="This report is not available to the public at the request of the copyright owner")
                    return JsonResponse(rest.toDict())
            # 检查用户权限
            rest = viewSummaryMode().getDocContent([sid], rtcolumns={"local_path": 1,'source':1})
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            rtdata = rest.data
            filepath = None
            dataSource = None
            if rtdata['count'] > 0:
                qdata = rtdata['data']
                if len(qdata) > 0:
                    qdt = qdata[0]
                    if "source" in qdt:
                        dataSource=qdt['source']
                    if "local_path" in qdt:
                        local_path = qdt['local_path']
                        # C:\virtualD\work\project\download\jpmorgan\reserach\V__N\pdf\GPS-3095475-0.pdf
                        filepath = str(local_path).replace(r"C:\virtualD\work\project\download", "z:")
            if filepath is None:
                rest.errorData(errorMsg="文件不存在！".encode('unicode-escape').decode('utf-8'))
                return JsonResponse(rest.toDict())
            fp, filename = os.path.split(filepath)
            # 获取文件路径
            # 加载文件
            # 看是否直接下载，或者打开
            # 这里传入userid是为了以后数据权限控制
            # filepath,reachFileName = report_mode.getReserchFile(userid, researchId)
            # filepath = r'C:\Users\env\Downloads\19978009.pdf'
            # filepath = r'Z:\cicc\reserach\0027__HK\银河娱乐_期待银河3期放量.pdf'
            # reachFileName = "银河娱乐_期待银河3期放量.pdf"
            # 如果文件不下载，直接打开，那么在这里直接返回文件流
            if dataSource=="yipit":
                content_type = 'text/html'
                if returnType == 'file':
                    with open(filepath, 'r') as wf:
                        fliedata = wf.read()
                        response = HttpResponse(fliedata, content_type)
                        # Set the Content-Disposition header to suggest a filename
                    # response['Content-Disposition'] = 'attachment; filename="%s"'% quote(filename)
                    # 如果文件不下载，直接打开，那么在这里直接返回文件流
                    # if not download:
                    return response
            else:
                content_type = 'application/pdf'
                if returnType == 'file':
                    with open(filepath, 'rb') as wf:
                        fliedata = wf.read()
                        response = HttpResponse(fliedata, content_type)
                        # Set the Content-Disposition header to suggest a filename
                    # response['Content-Disposition'] = 'attachment; filename="%s"'% quote(filename)
                    # 如果文件不下载，直接打开，那么在这里直接返回文件流
                    # if not download:
                    return response
            import base64
            # 可以根据不同的头实现文件流式下载
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
            with open(filepath, 'rb') as wf:
                fliedata = wf.read()
            rest.file = 'data:application/pdf;base64,' + \
                        base64.b64encode(fliedata).decode('ascii')
            rest.filename = filename
            return JsonResponse(rest.toDict())
        return JsonResponse(rest.toDict())

    except Exception as ex:
        errorMsg = '抱歉，获取数据失败，请稍后重试。'.encode('unicode-escape').decode('utf-8')
        Logger.errLineNo(msg=errorMsg)
        rest.errorData(errorMsg=errorMsg)
        # dh_user_utils.UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())

def researchContentFmt(input_string,translation='zh'):
    # 使用正则表达式进行替换
    if translation=='en':
        result_string = re.sub(r'Topic (.*?)\n', r'**\1**', input_string)
    else:
        result_string = re.sub(r'重点 (.*?)\n', r'**\1**', input_string)
    return result_string

def hisTranscriptsContentFmt(input_string,translation='zh'):
    # 使用正则表达式进行替换 正则表达式 (.+：) 匹配以冒号结尾的行，并使用捕获组 (\1) 将匹配的内容加粗。
    pattern = re.compile(r'(.+：|.+:)')
    input_string = pattern.sub(r'**\1**', input_string)
    return input_string

def replace_first_hash(text, replacement):
    # 使用正则表达式查找第一个开头的 "#"
    pattern = re.compile(r'#', re.MULTILINE)
    match = pattern.search(text)
    # 如果找到了 "#"
    if match:
        # 替换第一个开头的 "#"
        text = pattern.sub(replacement, text, count=1)
    return text

if __name__ == '__main__':

    s="因版权方要求，该报告不对外展示".encode('unicode-escape').decode('utf-8')
    print(s)
