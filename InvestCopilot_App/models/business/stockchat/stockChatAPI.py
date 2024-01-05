# encoding: utf-8
#股票聊天 调用蔡博士API
import datetime
import threading

import requests
from django.http import JsonResponse
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.business.likeMode import likeMode
from InvestCopilot_App.models.business.stockchat.stockChatMode import stockChatMode
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
Logger = logger_utils.LoggerUtils()

import json
import os
import re

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10


class stockChatApi():
    def __init__(self):
        # self.host="http://47.106.252.91:63001"
        self.host="http://172.18.163.219:63001"#内网地址
        self.meta="get_meta"
        self.query="query"
        self.timeout=(10,20)

    def getMeta(self):
        resp = requests.get("%s/%s"%(self.host,self.meta))
        if resp.status_code==200:
            return resp.json()
        return {}

    def getQuery(self,queryData={}):
        headers = {'Content-Type': 'application/json'}
        testdata ={'Message': '', 'Status': 'Success', 'embedding_time': 0.32012271881103516, 'group_texts': [
             {'date': '2023-11-04 08:57:10',
              'texts': 'Michael Saylor advocates for investment in Bitcoin before expected positive market shifts. Tesla could face a significant sell-off from institutional investors, according to Gordon Johnson\'s analysis.Benzinga examined the prospects for many investors\' favorite stocks over the last week — here\'s a look at some of our top stories. The week on Wall Street was marked by robust gains, with the Dow climbing 5.07% — its strongest showing since October 2022— and both the S&P 500 and Nasdaq posting their best performance since November 2022, with increases of 5.85% and 6.61%, respectively. In its November gathering, the Federal Reserve maintained the interest rate range at 5.25% to 5.5%, matching the expectations of the market and avoiding any declarations about its future rate decisions. The jobs data for October, released on Friday, was less robust than anticipated, adding only 150,000 jobs versus the projected 180,000, hinting that the Fed\'s strategy to temper economic growth and control inflation may be effective.\n ... \nFor additional bullish calls of the past week, check out the following: Ethereum Set For 75% Surge, Predicts Macro Guru Raoul Pal Thanks To This \'Gorgeous Chart\'— Veteran Peter Brandt Turns Bullish Too Jim Farley Reacts As Ford Gets Second Credit Upgrade: \'Important Milepost On The Way To Bigger Things\' Palantir CEO Alex Karp Declares Market Disruption With Artificial Intelligence Loading...Loading...Loading... The Bears "Disney\'s Kingdom Shaken As Activist Investor Stirs Up Boardroom With Bold Stock Play," by Chris Katje, reports on Trian Fund Management\'s Nelson Peltz\' escalating campaign for change at The Walt Disney Company DIS, potentially shaking up its boardroom dynamics. "Bearish Tesla Analyst Warns Of Potential Mass Exodus By Big Institutional Investors," by Shanthi Rexaline, outlines Gordon Johnson\'s analysis that Tesla Inc. TSLA could face a significant sell-off from institutional investors due to deteriorating financial results.',
              'title': "Benzinga Bulls And Bears: Tesla, Microsoft, Nvidia, Disney And Michael Saylor Says Now Is 'Ideal Entry Point' For Bitcoin"},
             {'date': '2023-11-10 15:39:38',
              'texts': "This momentum was partly fueled by Microsoft's Q1 earnings which surpassed expectations, exceeding the estimated earnings of $2.50 per share with actual earnings of $2.99 per share, resulting in a surprise of 19.60%. These positive earnings can significantly impact the stock price by boosting investor confidence and driving the share price higher. The next milestone to watch for is $366, and if the stock can break this barrier, a significant bullish trend may follow. Should the peak of $366 be surpassed, it may trigger a resurgence reminiscent of the remarkable 591% bullish trend witnessed from July 2016 to November 2021. After the closing bell on Thursday, November 9, the stock closed at $360.69, trading down by 0.69%. Loading...Loading...Loading...",
              'title': "Microsoft's Record High And Strategic AI Leap - Marks A New Era Of Success And Investor Confidence!"},
             {'date': '2023-11-22 14:34:20',
              'texts': 'Among tech giants, NVIDIA Corp. NVDA and Tesla Inc. TSLA lost ground, down 2.3% and 3.7%, respectively. The former is introducing a new price discount for its Model Y, while the latter issued a warning about declining sales in China despite reporting strong financial results. Peer Advanced Micro Devices Inc. AMD saw an almost 4% increase, marking the best performance among S&P 500 stocks. Microsoft Corp. MSFT reached an all-time high of 378.9. The latest economic data released today paints an improving picture in the mortgage market, lower-than-expected jobless claims, and an upward revision in the University of Michigan Consumer Confidence index for this month. The U.S. dollar index rose by 0.3%, while Treasury yields held steady. Crude oil prices dropped over 2% as the OPEC+ cartel postponed the meeting scheduled for this Sunday due to disputes over members’ output, leading to the energy sector’s underperformance. Wednesday’s US Index Performance',
              'title': "S&P 500 Flirts With Key Resistance, VIX Drops, Nvidia In Focus: What's Driving Markets Wednesday?"},
             {'date': '2023-11-14 10:50:38',
              'texts': 'Analysts have been eager to weigh in on the Technology sector with new ratings on Tesla ([TSLA](https://www.tipranks.com/stocks/tsla?ref=MCO_STOCK) – [Research Report](https://www.tipranks.com/subscribe/research-report/?symbol=tsla&ref=MCO_STOCK&refersTo=&merge=Markets)), Palo Alto Networks ([PANW](https://www.tipranks.com/stocks/panw?ref=MCO_STOCK) – [Research Report](https://www.tipranks.com/subscribe/research-report/?symbol=panw&ref=MCO_STOCK&refersTo=&merge=Markets)) and Microsoft ([MSFT](https://www.tipranks.com/stocks/msft?ref=MCO_STOCK) – [Research Report](https://www.tipranks.com/subscribe/research-report/?symbol=msft&ref=MCO_STOCK&refersTo=&merge=Markets)). **Tesla (TSLA)** In a report released today, [Mark Delaney](https://www.tipranks.com/analysts/mark-delaney?ref=MCO_EXPERT) from Goldman Sachs maintained a Hold rating on Tesla, with a price target of $235.00. The company’s shares closed last Monday at $223.71.',
              'title': 'Analysts Offer Insights on Technology Companies: Tesla (TSLA), Palo Alto Networks (PANW) and Microsoft (MSFT)'},
             {'date': '2023-12-06 03:55:21',
              'texts': '## **Nvidia Stock (NASDAQ:NVDA)** Nvidia stock has enjoyed a stellar rally this year, with the generative AI boom triggering a spike in the demand for the company’s graphics processing units (GPUs). The semiconductor giant’s fiscal third-quarter results crushed Wall Street’s expectations. [Revenue surged 206% to $18.1 billion](https://www.tipranks.com/stocks/nvda/financials) and [adjusted EPS jumped 593% to $4.02](https://www.tipranks.com/stocks/nvda/earnings). Speaking about the company’s robust prospects, [CEO Jensen Huang](https://www.tipranks.com/experts/insiders/jen-hsun-huang) stated, “NVIDIA GPUs, CPUs, networking, AI foundry services and NVIDIA AI Enterprise software are all growth engines in full throttle.” Nvidia expects continued strength in its business and projects its fiscal fourth quarter revenue to come in at $20 billion (plus or minus 2%), which implies about 231% growth.\xa0\xa0\xa0\xa0\xa0 ## **What is the Target Price for NVDA Stock?**',
              'title': 'MSFT, PLTR, or NVDA: Which is the Most Attractive AI Stock?'}, {'date': '2023-12-08 00:57:36',
                                                                                        'texts': "This would showcase investor confidence and signify the broader market's recognition of the stock's resilience, even in the face of legal challenges. Remarkably, the stock has exhibited outstanding performance this year, boasting a year-to-date growth of 51%. After the closing bell on Wednesday, December 6, the stock closed at $368.80, trading down by 1.00%. This article is from an external contributor. It does not represent Benzinga's reporting and has not been edited for content or accuracy. Loading...Loading...",
                                                                                        'title': 'Microsoft-Activision Deal Faces Another FTC Challenge As Their Stock Plunges 5%'},
             {'date': '2023-11-20 12:54:15',
              'texts': "## 3. Earnings winding down Nearly all S&P 500 companies have posted third-quarter earnings, but a handful of businesses set to report this week will have a lot to say about the state of the economy and U.S. consumers. [Nvidia](/quotes/NVDA/), already closely watched this year as its stock roars on the back of the AI boom, will have even more eyes on its results Tuesday after the bell in the wake of Altman's ouster. Major retailers like [Lowe's](/quotes/LOW/), [Best Buy](/quotes/BBY/) and [Abercrombie & Fitch](/quotes/ANF/) will pack their earnings in on Tuesday morning, and may offer commentary on how companies are faring during the critical holiday quarter. Third-quarter earnings for S&P companies had climbed 6.6% compared with the year-ago quarter as of Friday, but revenue had only increased by 1.4%. Here are the key companies reporting this week:",
              'title': '5 things to know before the stock market opens Monday'}, {'date': '2023-11-23 10:46:43',
                                                                                  'texts': 'The markets resumed their rally in thin pre-holiday trading on Wednesday, after a brief decline in the previous session. Investors brushed aside an increase in consumers’ 12-month inflation expectations from 4.4% to 4.5%, as these unwelcome results were overshadowed by positive news from other directions. Oil slumped, adding to market optimism going into the Thanksgiving break.  Amazon.com (**[AMZN](https://www.tipranks.com/stocks/amzn/forecast)**) rallied in anticipation of robust Black Friday and Cyber Monday sales. Microsoft (**[MSFT](https://www.tipranks.com/stocks/msft/forecast)**) rose as OpenAI was reported to reinstate Sam Altman as a CEO. Tech stocks led the markets higher, overcoming a slump in NVIDIA (**[NVDA](https://www.tipranks.com/stocks/nvda/forecast)**) shares. The chipmaker’s Q3 results by far topped estimates, but the downbeat outlook on its Chinese sales, accounting for a fifth of the total, spooked investors.',
                                                                                  'title': 'Thursday Macro & Markets Update'},
             {'date': '2023-11-27 23:51:53',
              'texts': 'Cramer first pointed to tech stocks like [Nvidia](/quotes/NVDA/), with the semiconductor company priced much higher than expected this year due to its prominent role in generative artificial intelligence. Cramer said Nvidia\'s stock has soared for two reasons: Investors are willing to pay more for the same earnings — known as multiple expansion — and because the company\'s earnings turned out better than expected. But to him, the stock is soaring mostly for the latter reason. "Why should we care? Because, ultimately, we don\'t want to pay more and more for the same earnings, right?" Cramer asked. "When that happens, they get more expensive, and they get risky. We want stocks to turn out to be reasonably priced based on higher earnings because that kind of rally is sustainable."',
              'title': "Cramer says the market needs 'some new heroes' to create a sustainable rally"}],
                  'search_time': 0.04854583740234375, 'summaries': [{'model': 'gpt-4',
                                                                     'summary': '特斯拉的股票价格在这个季度中经历了一些波动。据分析师的研究，由于特斯拉的财务结果可能出现恶化，季度末时可能面临来自大规模机构投资者的大量卖出（参考段落[1]）。尽管特斯拉报告了强劲的财务结果，但其在中国的销售下降警告可能也影响了股价（参考段落[3]）。更具体地说，最近的一份金融报告显示，特斯拉的股票在上周一时以$223.71的价格收盘（参考段落[4]）。这是一份简略的概述，具体的股价表现可能依赖于更具体的时间段和市场条件。',
                                                                     'time': 26.70047616958618},
                                                                    {'model': 'gpt-4-1106-preview',
                                                                     'summary': '特斯拉这个季度的股价表现似乎有所挑战。从提供的段落中可以发现，有分析师警告说由于财务结果恶化，特斯拉可能会面临大型机构投资者的大量抛售(参考段落 [1])。此外，还提到了特斯拉在中国销量下滑的警告，尽管财务报告很强劲，但仍然导致其股价下跌了3.7% (参考段落 [3])。从这些信息中，我们可以推断出特斯拉这个季度的股票表现可能面临一些挑战，尽管没有给出具体的季度股价表现数据。',
                                                                     'time': 14.651612997055054}]}
        # return testdata
        resp = requests.post("%s/%s"%(self.host,self.query),json=queryData,headers=headers)
        Logger.info("getQuery status_code:%s"%resp.status_code)
        Logger.info("getQuery resp.text:%s"%resp.text)
        if resp.status_code==200:
            return resp.json()
        return {}

@userLoginCheckDeco
def stockChatAPIHandler(request):
    #用户点赞
    rest = ResultData()
    try:
        crest=tools_utils.requestDataFmt(request,fefault=None)
        if not crest.errorFlag:
            return JsonResponse(crest.toDict())
        reqData = crest.data
        doMethod=reqData.get("doMethod")
        user_id = request.session.get("user_id")
        if doMethod=="stockChatMeta":#用于拉取目前库内的的信息，用于给用户手动指定范围。
            #http://47.106.252.91:63001/get_meta
            # key="stockchat_meta"
            schat = stockChatApi()
            meta_data = schat.getMeta()
            # rtdata = cache.get(key)
            # if rtdata is None:
            #     if len(meta_data) > 0:
            #         cache.set(key,meta_data,CACHEUTILS_UPDATE_SECENDS)
            #         rest.data=meta_data
            # else:
            rest.data =meta_data
            """
            Status为Success表示成功，否则Message这边会给出后端具体报错信息。            
            * company_ids_and_names是目前库内已有的公司id和列表
            * datatype_ids_and_names是目前库内已有的数据源类型和列表（比如research、news）
            * model_names是后端支持的总结模型的列表（比如gpt-3.5-turbo）
            * months是库内所有信息发布的月份集合（数字，如202312）
            """
            return JsonResponse(rest.toDict())
        if doMethod=="stockChatQuery":
            """ 
            * query_text：用户输入的对话问题
            * chat_id：标明对话id的随机生成的字符串，长为26。如果用户多次查询都在同一个聊天框内，则需要传递同一个chat_id，后端才能获取其之前的记录。
            * model_names：用户指定的总结模型列表，是meta信息获取的模型列表子集，非空
            * auto_filter：是否由GPT自动指定检索过滤条件。如果设为true，后续四个指定的范围无论传递什么，都将失效 
            之后三个都是用户指定的检索约束范围：  
            * company_ids：公司id列表，是meta信息获取的模型列表中的id子集，如果为空则代表全选所有公司
            * datatype_ids：数据源id列表，是meta信息获取的数据源中的id子集，如果为空则代表全选所有数据源
            * months：检索月份范围列表，是meta信息获取的月份子集，如果为空则代表全选所有月份
            """
            query_text = reqData.get("query_text")
            chat_id = reqData.get("chat_id")
            result_id = reqData.get("result_id")#刷新
            model_names = reqData.get("model_names")
            auto_filter = reqData.get("auto_filter")
            company_ids = reqData.get("company_ids")
            datatype_ids = reqData.get("datatype_ids")
            months = reqData.get("months")
            top_k = reqData.get("top_k")#前端加一个选项区分弱模型和强模型，，然后后台对应传一个top_k=8和top_k=20
            if top_k is None or str(top_k).strip()==0:
                top_k=8
            else:
                top_k = int(top_k)

            #参数检查
            if query_text is None or str(query_text).strip()==0:
                rest.errorData(errorMsg="Please enter query_text params.")
            if chat_id is None or str(chat_id).strip()==0:
                rest.errorData(errorMsg="Please enter chat_id params.")
            if model_names is None or str(model_names).strip()==0:
                rest.errorData(errorMsg="Please enter model_names params.")
            if auto_filter is None or str(auto_filter).strip()==0:
                auto_filter=False
            else:
                auto_filter = True
            if company_ids is None or str(company_ids).strip()==0:
                rest.errorData(errorMsg="Please enter company_ids params.")
            if datatype_ids is None or str(datatype_ids).strip()==0:
                rest.errorData(errorMsg="Please enter datatype_ids params.")
            if months is None or str(months).strip()==0:
                rest.errorData(errorMsg="Please enter months params.")

            model_names = re.split("[|,]", str(model_names))
            if len(model_names)==0:
                rest.errorData(errorMsg="Please enter model_names params.")
            company_ids = re.split("[|,]", str(company_ids))
            if len(company_ids)==0:
                rest.errorData(errorMsg="Please enter company_ids params.")
            else:
                #数据类型转int
                company_ids=[int(x) for x in company_ids]
            datatype_ids = re.split("[|,]", str(datatype_ids))
            if len(datatype_ids)==0:
                rest.errorData(errorMsg="Please enter datatype_ids params.")
            else:
                #数据类型转int
                datatype_ids=[int(x) for x in datatype_ids]
            months = re.split("[|,]", str(months))
            if len(months)==0:
                rest.errorData(errorMsg="Please enter months params.")
            else:
                #数据类型转int
                months=[int(x) for x in months]
            schat = stockChatApi()
            #默认参数
            qdata={
                    "query_text": query_text,
                    "chat_id": chat_id,
                    "model_names":["gpt-4-1106-preview"],
                    "auto_filter": True,
                    "company_ids": [],
                    "datatype_ids": [],
                    "months": [],
                    "top_k": top_k
                }
            Logger.debug("[%s]Query qdata%s"%(user_id,qdata))
            c_user_dt = user_utils.getCacheUserInfo(user_id)
            companyId = c_user_dt['COMPANYID']
            create_time=datetime.datetime.now()
            #查询次数限制
            rtnumrs=stockChatMode().getStockChatRequestNum(user_id,companyId)
            if not rtnumrs.errorFlag:
                return JsonResponse(rtnumrs.toDict())

            stockChatMode().addStockChatConv(chat_id,query_text,user_id,create_time)
            meta_data = schat.getQuery(qdata)
            Status=meta_data['Status']
            if Status != "Success":
                rest.errorData(errorMsg=meta_data['Message'])
                return JsonResponse(rest.toDict())
            update_time=datetime.datetime.now()
            #保存用户日志数据数据
            srtd=stockChatMode().addStockChatConvResult(chat_id,user_id,query_text,update_time,meta_data,result_id) #result_id is not None 内容刷新
            result_id="-1"
            if srtd.errorFlag:
                data=srtd.data
                result_id=data['result_id']
            meta_data['result_id']=result_id
            rest.data =meta_data
            """
            Status为Success表示成功，否则Message这边会给出后端具体报错信息。            
            * transform_time, embedding_time, search_time：分别代表问题改写与转换时间、查询向量化时间、库内检索时间（单位：秒）
            * query_filter：GPT自动过滤后得到的过滤器，是个str（这一块只是用于debug校验，不一定要在前端展示）
            * query_text：GPT自动改写后的查询文本，是个str（这一块只是用于debug校验，不一定要在前端展示）
            * group_texts：检索出来的文档片段列表，每项中title为其标题，texts表示文段、date表示文档发布日期
            * summaries：总结模型得到的结果列表，每项中model指总结模型，summary表示总结文本，time表示总结时间（单位：秒）
            """
            return JsonResponse(rest.toDict())
        elif doMethod=="getStockChatConv":
            pageSize = reqData.get("pageSize")
            if pageSize=="" or pageSize is None:
                pageSize=20
            else:
                pageSize=int(pageSize)
            page = reqData.get("page")
            if page=="" or page is None:
                page=1
            else :page=int(page)
            rest=stockChatMode().getStockChatConv(user_id,page=page,pageSize=pageSize)
            return JsonResponse(rest.toDict())
        elif doMethod=="editStockChatConv":#对话名字修改
            conv_id=reqData.get("conv_id")
            if conv_id=="" or conv_id is None:
                rest.errorData(errorMsg="Please enter conv_id params.")
            conv_title=reqData.get("conv_title")#对话名字
            if conv_title=="" or conv_title is None:
                rest.errorData(errorMsg="Please enter conv_title params.")
            rest = tools_utils.charLength(conv_title, 100, '对话名字')
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            rest=stockChatMode().editStockChatConv(user_id,conv_id,conv_title)
            return JsonResponse(rest.toDict())
        elif doMethod=="delStockChatConv":#删除对话
            conv_id=reqData.get("conv_id")#对话主要id
            if conv_id=="" or conv_id is None:
                rest.errorData(errorMsg="Please enter conv_id params.")
            rest=stockChatMode().delStockChatConv(user_id,conv_id)
            return JsonResponse(rest.toDict())
        elif doMethod=="stockChatConvLike":#点赞
            result_id=reqData.get("result_id")#objecid
            if result_id=="" or result_id is None:
                rest.errorData(errorMsg="Please enter result_id params.")
            slike=reqData.get("slike")
            if str(slike) not in ["N","P","Cancel"]:
                rest = rest.errorData(errorMsg='slike参数错误')
                return JsonResponse(rest.toDict())
            rest=stockChatMode().likeStockChatConv(user_id,result_id,slike)
            return JsonResponse(rest.toDict())
        if doMethod=="getStockChatConvResult":
            conv_id=reqData.get("conv_id")
            if conv_id=="" or conv_id is None:
                rest.errorData(errorMsg="Please enter conv_id params.")
                return JsonResponse(rest.toDict())
            pageSize = reqData.get("pageSize")
            if pageSize=="" or pageSize is None:
                pageSize=20
            else:
                pageSize=int(pageSize)
            page = reqData.get("page")
            if page=="" or page is None:
                page=1
            else :page=int(page)
            rest=stockChatMode().getStockChatConvResult(conv_id,user_id,page=page,pageSize=pageSize)
            return JsonResponse(rest.toDict())

        if doMethod=="delComments":
            cid=reqData.get("cid")
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if cid=="" or cid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest = likeMode().delComments(cid,sid,user_id)
            return JsonResponse(rest.toDict())

        elif doMethod=="getCommentsNum":
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest = likeMode().getCommentsNum(sid)
            return JsonResponse(rest.toDict())
        else:
            rest.errorData("stockChatAPI not find")
            return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        #UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())

if __name__ == '__main__':
    als=[]
    with open("d:/lib.txt",'r') as rf:
        ss=rf.read()
        for s in str(ss).split("\n"):

            als.append(str(s).split(">=")[0])
    print("\n".join(als))