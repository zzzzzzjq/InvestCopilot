import datetime
import json

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.summary.summaryMode import summaryMode
from django.http import JsonResponse

import  InvestCopilot_App.models.toolsutils.ToolsUtils as tool_utils
Logger = logger_utils.LoggerUtils()
def summaryAPIHandler(request):
    rest = ResultData()
    try:
        doMethod = request.POST.get("doMethod")
        webSite = request.POST.get("webSite")
        webTag = request.POST.get("webTag")
        if doMethod == "getNewsIds":
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            isPool = request.POST.get("isPool")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-10)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            windCode = request.POST.get("symbol")
            sumaryFlag = request.POST.get("sumaryFlag")
            addQuerys = request.POST.get("addQuerys")
            if addQuerys is not None:
                addQuerys=json.loads(addQuerys)
            else:
                addQuerys={}
            rtColumns = request.POST.get("rtColumns")
            if rtColumns is not None:
                rtColumns=json.loads(rtColumns)
            else:
                rtColumns={}
            rest = summaryMode().getNewsIds(webSite, webTag, beginTime, endTime, windCode=windCode,
                                                sumaryFlag=sumaryFlag,addQuerys=addQuerys,rtColumns=rtColumns,isPool=isPool)
        elif doMethod == "getSetData":
            userName = request.POST.get("userName")
            passWord = request.POST.get("passWord")
            if userName is None or userName=="" or   passWord is None or passWord=="" :
                rest=rest.errorData("请填写登录账号信息")
                return JsonResponse(rest.toDict())
            if not str(userName)=="mgName!@#" :
                rest=rest.errorData("用户名错误")
                return JsonResponse(rest.toDict())
            if not str(passWord)=="pwd@#A!@#" :
                rest=rest.errorData("登录密码错误")
                return JsonResponse(rest.toDict())

            addQuerys = request.POST.get("addQuerys")
            if addQuerys is not None:
                addQuerys=json.loads(addQuerys)
            else:
                addQuerys={}
            rtColumns = request.POST.get("rtColumns")
            if rtColumns is not None:
                rtColumns=json.loads(rtColumns)
            else:
                rtColumns={}
            rest = summaryMode().getSetData(webSite, webTag,addQuerys=addQuerys,rtColumns=rtColumns)
        elif doMethod == "getNewsIds_v1":
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-10)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            windCodes = request.POST.get("symbol")
            if windCodes is not None and windCodes != "":
                windCodes = str(windCodes).split(",")
            else:
                windCodes = []
            sumaryFlag = request.POST.get("sumaryFlag")
            if sumaryFlag is None:
                sumaryFlag="1"
            rest = summaryMode().getNewsIds_v1(beginTime, endTime, windCodes=windCodes, sumaryFlag=sumaryFlag)
        elif doMethod == "summaryFinishSend":
            #会议摘要计算完毕消息通知
            audioId = request.POST.get("audioId")
            if audioId is None or audioId == "":
                rest=rest.errorData("摘要编号错误")
                return JsonResponse(rest.toDict())
            rest=summaryMode().summaryFinishSend(audioId)
        elif doMethod == "getNewsIds_v2":
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-2)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            windCodes = request.POST.get("symbol")
            if windCodes is not None and windCodes != "":
                windCodes = str(windCodes).split(",")
            else:
                windCodes = []
            sumaryFlag = request.POST.get("sumaryFlag")
            if sumaryFlag is None:
                sumaryFlag = "1"
            rest = summaryMode().getNewsIds_v2(beginTime, endTime, windCodes=windCodes, sumaryFlag=sumaryFlag)
        elif doMethod == "getNewsIds_v3":
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            status = request.POST.get("status")
            gptid = request.POST.get("gptid")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-5)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            windCodes = request.POST.get("symbol")
            if windCodes is not None and windCodes != "":
                windCodes = str(windCodes).split(",")
            else:
                windCodes = []
            sumaryFlag = request.POST.get("sumaryFlag")
            if sumaryFlag is None:
                sumaryFlag = "1"
            if status is None or status=="":
                status=-1
            else:
                status = int(status)
            rest = summaryMode().getNewsIds_v3(beginTime, endTime, windCodes=windCodes, sumaryFlag=sumaryFlag, status=status,gptid=gptid)
        elif doMethod == "callbackstatus":
            status = request.POST.get("status")
            message = request.POST.get("message")
            id = request.POST.get("id")
            if id is None:
                rest=rest.errorData(errorMsg="id参数不能为空")
                return JsonResponse(rest.toDict())
            if status is None:
                rest=rest.errorData(errorMsg="status参数不能为空")
                return JsonResponse(rest.toDict())
            if str(status) not in ["9","1"]: #0:未处理，2：pull,3:处理中 ；9：异常 1：完成
                rest=rest.errorData(errorMsg="status状态只能为[9:异常;1:完成]")
                return JsonResponse(rest.toDict())
            status = int(status)
            if message is None:
                message=""
            else:
                message=str(message)
            rest = summaryMode().callBackNewStatus(id,status,message)
        elif doMethod == "getMonitorIds":
            windCodes = request.POST.get("symbol")
            if windCodes is not None and windCodes != "":
                windCodes = str(windCodes).split(",")
            else:
                windCodes = []
            sumaryFlag = request.POST.get("sumaryFlag")
            if sumaryFlag is None:
                sumaryFlag="1"
            rest = summaryMode().getMonitorIds(windCodes=windCodes, sumaryFlag=sumaryFlag)
        elif doMethod == "getHisSummaryCode":
            #获取需要计算的个股的历史summary
            qid = request.POST.get("qid")
            status = request.POST.get("status")
            # if status is   None or status == "":
            #     rest.errorData(errorMsg="status 不能为空")
            #     return JsonResponse(rest.toDict())
            rest = summaryMode().getHisSummaryCode(qid=qid,status=status)
        elif doMethod == "getStockName":
            tickers = request.POST.get("tickers")
            tickers = str(tickers).split(",")
            rest = summaryMode().getStockInfoByWindCodes(windCodes=tickers)
        elif doMethod == "getNewsContents":
            id = request.POST.get("id")
            ids = str(id).split(",")
            # print("ids:",ids)
            rest = summaryMode().getNewsContents(webSite, webTag, ids)
        elif doMethod == "getNewsContents_v1":
            id = request.POST.get("id")
            datafmt = request.POST.get("datafmt")
            translation = request.POST.get("translation")
            ids = str(id).split(",")
            rest = summaryMode().getNewsContents_v1(ids)
            vsummary = request.POST.get("vsummary")
            if str(vsummary)=="1":
                qdata = rest.data
                if len(qdata) > 0:
                    qdt = qdata[0]
                    qdt["summaryText"] = ""#原文
                    qdt["transSummaryText"] = ""#修正后原文
                    qdt["local_path"] = ""
                    # if translation in ['zh']:
                    #     summary = qdt['summary']
                    # else:
                    #     summary = qdt['summary_en']
                    # qdt["summary"] =markdown2.markdown(summary, extras=["tables"])
                    # print(qdt["summary"])
                    qdata=[qdt]
                    rest.data=qdata
            vtransSummaryText = request.POST.get("vtransSummaryText")
            if str(vtransSummaryText)=="1":
                qdata = rest.data
                if len(qdata) > 0:
                    qdt = qdata[0]
                    qdt["summaryText"] = ""#原文
                    qdt["summary"] = ""#中文摘要
                    qdt["summary_en"] = ""#英文摘要
                    qdt["local_path"] = ""
                    qdata=[qdt]
                    rest.data=qdata
            if  datafmt in ['link']:
                #股票代码转换
                qdata=rest.data
                if len(qdata)>0:
                    qdt = qdata[0]
                    if "relationCompanies" in qdt:
                        cpwindowdt={}
                        cpTickers=qdt['relationCompanies']
                        # print("cpTickers:",cpTickers)
                        if "company" in cpTickers:
                            cpcompanys = cpTickers["company"]
                            for company in cpcompanys:
                                if "symbol" in company and "english_name" in company and "chinese_name" in company:
                                    symbol=company['symbol']
                                    if str(symbol).strip()=="":
                                        continue
                                    english_name=company['english_name']
                                    chinese_name=company['chinese_name']
                                    symbolls = str(symbol).split(".")
                                    if len(symbolls) == 1:
                                        ticker = symbolls[0]
                                        tickerInfo = summaryMode().getWindCodeInfo(ticker)
                                        if isinstance(tickerInfo, dict):
                                            ticker = tickerInfo['windCode']
                                            codeName = ticker#
                                            # codeName = tickerInfo['bbgName']
                                        else:
                                            continue
                                        unit="US"
                                    else:
                                        ticker = symbol
                                        codeName=ticker
                                        if str(ticker).endswith(".SS"):
                                            ticker = str(ticker).replace(".SS", '.SH')
                                        elif str(ticker).endswith(".HK"):
                                            ticker = str(ticker).replace(".HK", '').zfill(4)+".HK"
                                        codeRst = summaryMode().getStockInfoByWindCodes(ticker)
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
                                    cpwindowdt[symbol]={'chinese_name':chinese_name,'english_name':english_name,'ticker':ticker,"tickerName":codeName,'unit':unit}
                        if "company" in cpTickers:
                            if translation in ['zh']:
                                summary = qdt['summary']
                            else:
                                summary = qdt['summary_en']
                            if isinstance(summary,list):
                                summarystr="\n".join(summary)
                            else:
                                summarystr=summary
                            if translation in ['zh']:
                                bidx = summarystr.find("1. 行业/公司：")
                                eidx = summarystr.find("摘要：", len("1. 行业/公司："))
                            else:
                                bidx=summarystr.find("1. Industries/Companies:")
                                eidx=summarystr.find("Abstract:",len("1. Industries/Companies:"))
                            summarystr1=summarystr[bidx:eidx]
                            summarystr2=summarystr[eidx:]
                            cktickers=[]
                            for symbol,wdt in cpwindowdt.items():
                                chinese_name=wdt['chinese_name']
                                english_name=wdt['english_name']
                                cktickers.append(wdt)
                                ticker=wdt['ticker']
                                unit=wdt['unit']
                                vticker=ticker
                                if unit in ["US"]:
                                    vticker=str(ticker).split(".")[0]
                                tickerName=wdt['tickerName']
                                # print("tickerName:",tickerName)
                                #特别讨论了Sunny Optical（舜宇光学科技）、BYD（比亚迪）、Gao Wei Electronics（高伟电子）、Zhuhai Crown Royal（珠海皇冠皇家）、Luxshare Precision（立讯精密）和Apple（苹果）
                                #替换为 舜宇光学(sss.HK) ...
                                #<a href="javascript:goSymbolPage('summaryview.html','SCHW')">SCHW.N  嘉信理财(Charles Schwab)</a>
                                summarystrx = summarystr1.replace(symbol,
                                                                  f"""<a  href="javascript:goSymbolPage('summaryview.html','{ticker}')">{vticker}</a>""")
                                if summarystrx==summarystr1:
                                    summarystrx = summarystr1.replace(english_name, f"""<a href="javascript:goSymbolPage('summaryview.html','{ticker}')">{vticker}</a>""")
                                    if summarystrx==summarystr1:
                                        summarystr1 = summarystr1.replace(chinese_name,
                                                                          f"""<a href="javascript:goSymbolPage('summaryview.html','{ticker}')">{vticker}</a>""")
                                    else:
                                        summarystr1 = summarystrx
                                else:
                                    summarystr1=summarystrx #
                            if translation in ['zh']:
                                endsummary="会议摘要：\n"+summarystr1+summarystr2
                            else:
                                endsummary = "Meeting Summary:\n" + summarystr1 + summarystr2
                            endsummary=str(endsummary).replace('href="javascript:',' style="font-weight: bold" href="javascript:')
                            # if translation in ['zh']:
                            #     qdt['summary']=endsummary
                            # else:
                            #     qdt['summary_en'] = endsummary
                            qdt['cktickers'] = cktickers
                            qdata=[qdt]
                            rest.data=qdata
            return JsonResponse(rest.toDict())

        elif doMethod == "getTranslateAudio":
            #获取需要翻译的电话会议
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-10)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            tickers = request.POST.get("tickers")
            if tickers is not None and tickers != "":
                tickers = str(tickers).split(",")
            else:
                tickers = []
            companyIds = request.POST.get("companyIds")
            if companyIds is not None and companyIds != "":
                companyIds = str(companyIds).split(",")
            else:
                companyIds = []
            sumaryFlag = request.POST.get("sumaryFlag")
            if sumaryFlag is None:
                sumaryFlag="1"
            rest = summaryMode().getAudioIds(beginTime, endTime, companyIds=companyIds,
                                               tickers=tickers, sumaryFlag=sumaryFlag,allCols=False,rtcolumns={})
            return JsonResponse(rest.toDict())
        elif doMethod == "fillAudioText":
            #回填音频翻译后的内容
            audioId = request.POST.get("id")
            audioText = request.POST.get("audioText")
            if audioId == "" or audioId is None:
                rest.errorData("编号不能为空!")
                return JsonResponse(rest.toDict())
            if (audioText == "" or audioText is None):
                rest.errorData("音频翻译内容不能为空!")
                return JsonResponse(rest.toDict())
            if len(str(audioText).strip())==0:
                rest.errorData("音频翻译内容不能为空!")
                return JsonResponse(rest.toDict())
            rest = summaryMode().fillAudioText(audioId, audioText=audioText)

        elif doMethod == "fillSummary":
            id = request.POST.get("id")
            summary = request.POST.get("summary")
            if id == "" or id is None:
                rest.errorData("编号不能为空!")
                return JsonResponse(rest.toDict())
            if summary == "" or summary is None:
                rest.errorData("摘要不能为空!")
                return JsonResponse(rest.toDict())
            rest = summaryMode().fillSummary(webSite, webTag, id, summary)
        elif doMethod == "fillSummary_v1":
            id = request.POST.get("id")
            summary_zh = request.POST.get("summary_zh")
            summary_en = request.POST.get("summary_en")
            title_en = request.POST.get("title_en")
            title_zh = request.POST.get("title_zh")
            importance_score = request.POST.get("importance_score")
            symbols_score = request.POST.get("symbols_score")
            summarys = request.POST.get("summarys")
            summarys2 = request.POST.get("summarys2")
            publicSentiment = request.POST.get("publicSentiment")#舆情，新闻正负面 中性
            reason = request.POST.get("reason")#正负面原因
            relationCompanies = request.POST.get("relationCompanies")#关联公司
            influencedCompanies = request.POST.get("influencedCompanies")#受影响公司
            summaryFormat = request.POST.get("summaryFormat")#fmt
            trans_newcontents = request.POST.get("trans_newcontents")#audio 会议修正后原文
            if id == "" or id is None:
                rest.errorData("编号不能为空!")
                return JsonResponse(rest.toDict())
            if (summary_zh == "" or summary_zh is None ) and (summary_en == "" or summary_en is None ) :
                rest.errorData("中文、英文摘要不能同时为空!")
                return JsonResponse(rest.toDict())
            if importance_score is not None:
                if not tool_utils.isNumber(importance_score):
                    rest.errorData("[importance_score]必须为数字类型!")
                    return JsonResponse(rest.toDict())
                importance_score=float(importance_score)
            if publicSentiment=="":
                publicSentiment=None

            if trans_newcontents=="":
                trans_newcontents=None

            if reason=="" or reason is None:
                reason=[]
            else:
                reason=json.loads(str(reason))
            if summaryFormat=="":
                summaryFormat=None
            if influencedCompanies is None:
                influencedCompanies={}
            else:
                influencedCompanies=json.loads(str(influencedCompanies))
            if symbols_score is None:
                symbols_score=[]
            else:
                symbols_score=json.loads(str(symbols_score))
            if relationCompanies is None:
                relationCompanies= {}
            else:
                relationCompanies=json.loads(str(relationCompanies))
            if summarys is None:
                summarys=[]
            else:
                summarys=json.loads(str(summarys))
            if summarys2 is None:
                summarys2=[]
            else:
                summarys2=json.loads(str(summarys2))
            rest = summaryMode().fillSummary_v1(id, summary_zh=summary_zh, summary_en=summary_en,uTitleZh=title_zh,uTitleEn=title_en,importance_score=importance_score,
                                                    symbols_score=symbols_score,summarys=summarys,summarys2=summarys2,publicSentiment=publicSentiment,reason=reason,
                                                    relationCompanies=relationCompanies,influencedCompanies=influencedCompanies,summaryFormat=summaryFormat,
                                                    trans_newcontents=trans_newcontents)
        elif doMethod == "updateSourceContent":
            id = request.POST.get("id")
            transSummaryText = request.POST.get("transSummaryText")
            if id == "" or id is None:
                rest.errorData("编号不能为空!")
                return JsonResponse(rest.toDict())
            if transSummaryText == "" or transSummaryText is None:
                rest.errorData("更新修正原文内容不能为空!")
                return JsonResponse(rest.toDict())
            rest = summaryMode().updateSourceContent(id, transSummaryText)

        elif doMethod == "addNewContent":
            datas = request.POST.get("datas")
            if datas is not None:
                datas = json.loads(datas)
            rest = summaryMode().addNewContent(webSite, webTag, datas)
        elif doMethod == "delNewContent":
            ids = request.POST.get("ids")
            if ids is not None:
                ids = json.loads(ids)
            rest = summaryMode().delNewContent(webSite, webTag, ids)
        elif doMethod == "getEarningsForecastData":
            preDays = request.POST.get("preDays")
            nextDays = request.POST.get("nextDays")
            preDays=int(preDays)
            nextDays=int(nextDays)
            #最近一周出业绩公司
            rest = summaryMode().getEarningsForecastData(preDays=preDays,nextDays=nextDays)
        elif doMethod == "getStockPool":
            trackFlag = request.POST.get("trackFlag")
            preDays = request.POST.get("preDays")
            nextDays = request.POST.get("nextDays")
            dataType = request.POST.get("dataType")
            if preDays is None :
                preDays=-2
            else:
                preDays=int(preDays)
            if nextDays is None :
                nextDays=0
            else:
                nextDays=int(nextDays)
            if trackFlag is None :
                trackFlag="1"
            rest = summaryMode().getStockPool(trackFlag=trackFlag,preDays=preDays,nextDays=nextDays,dataType=dataType)
        elif doMethod == "getTickers":
            windCodes = request.POST.get("windCodes")
            if windCodes is not None:
                windCodes = json.loads(windCodes)
                rest = summaryMode().getTickers(webSite, webTag, windCodes)
        elif doMethod == "buildSummaryPdf":
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-10)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            webSites = request.POST.get("webSites")
            if webSites is not None:
                webSites = json.loads(webSites)
            rest = summaryMode().createSummary(beginTime, endTime, dataBases=webSites)
        elif doMethod == "buildSummaryPdf_v1":
            print("buildSummaryPdf_v1:",request.POST)
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            upBeginTime = request.POST.get("upBeginTime")
            upEndTime = request.POST.get("upEndTime")
            performanceFlag = request.POST.get("performanceFlag")
            isUpdateTime = request.POST.get("isUpdateTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-10)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            if upBeginTime is None:
                upBeginTime = nt + datetime.timedelta(days=-10)
                upBeginTime = upBeginTime.strftime("%Y-%m-%d %H:%M:%S")
            if upEndTime is None:
                upEndTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            if isUpdateTime is None:
                isUpdateTime="0"
            if performanceFlag is None:
                performanceFlag="0"
            windCodes = request.POST.get("symbol")
            if windCodes is not None and windCodes != "":
                windCodes = str(windCodes).split(",")
            else:
                windCodes = []
            Logger.info("doMethod:%s,beginTime:%s,endTime:%s,isUpdateTime:%s"%(doMethod,beginTime,endTime,isUpdateTime))
            print("doMethod:%s,beginTime:%s,endTime:%s,isUpdateTime:%s,performanceFlag:%s,windCodes:%s"%(doMethod,beginTime,endTime,isUpdateTime,performanceFlag,windCodes))
            #筛选出业绩公司
            rest = summaryMode().createSummary_v1(beginTime, endTime,isUpdateTime=isUpdateTime,upBeginTime=upBeginTime, upEndTime=upEndTime,windCodes=windCodes,performanceFlag=performanceFlag)
            # rest = summaryMode().createSummary_v1(beginTime, endTime,isUpdateTime=isUpdateTime,upBeginTime=upBeginTime, upEndTime=upEndTime,windCodes=windCodes,performanceFlag="1")
            # rest = summaryMode().createSummary_v1(beginTime, endTime,isUpdateTime=isUpdateTime,upBeginTime=upBeginTime, upEndTime=upEndTime,windCodes=windCodes)
            # s = summaryMode().createSummary('2023-01-06 13:47:03', "2023-07-24 12:59:22",
            #                         [{"dataBase": 'email', "dbName": 'research_ubs'},
            #                          ])
            # print("rest:",rest.toDict())
        elif doMethod == "buildSummaryPdf_v2":
            print("buildSummaryPdf_v2:",request.POST)
            nt = datetime.datetime.now()
            beginTime = request.POST.get("beginTime")
            endTime = request.POST.get("endTime")
            upBeginTime = request.POST.get("upBeginTime")
            upEndTime = request.POST.get("upEndTime")
            performanceFlag = request.POST.get("performanceFlag")
            isUpdateTime = request.POST.get("isUpdateTime")
            if beginTime is None:
                beginTime = nt + datetime.timedelta(days=-10)
                beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            if endTime is None:
                endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            if upBeginTime is None:
                upBeginTime = nt + datetime.timedelta(days=-10)
                upBeginTime = upBeginTime.strftime("%Y-%m-%d %H:%M:%S")
            if upEndTime is None:
                upEndTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            if isUpdateTime is None:
                isUpdateTime="0"
            if performanceFlag is None:
                performanceFlag=""
            windCodes = request.POST.get("symbol")
            if windCodes is not None and windCodes != "":
                windCodes = str(windCodes).split(",")
            else:
                windCodes = []
            Logger.info("doMethod:%s,beginTime:%s,endTime:%s,isUpdateTime:%s"%(doMethod,beginTime,endTime,isUpdateTime))
            #筛选出业绩公司
            # rest = summaryMode().createSummary_v1(beginTime, endTime,isUpdateTime=isUpdateTime,upBeginTime=upBeginTime, upEndTime=upEndTime,windCodes=windCodes,performanceFlag=performanceFlag)
            # rest = summaryMode().createSummary_v1(beginTime, endTime,isUpdateTime=isUpdateTime,upBeginTime=upBeginTime, upEndTime=upEndTime,windCodes=windCodes,performanceFlag="1")
            rest = summaryMode().createSummary_v2(beginTime, endTime,isUpdateTime=isUpdateTime,upBeginTime=upBeginTime, upEndTime=upEndTime,windCodes=windCodes)
            # s = summaryMode().createSummary('2023-01-06 13:47:03', "2023-07-24 12:59:22",
            #                         [{"dataBase": 'email', "dbName": 'research_ubs'},
            #                          ])
            # print("rest:",rest.toDict())
        return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='抱歉，数据处理失败，请稍后重试。')
        # dh_user_utils.UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())