#策略

import  logging
import traceback

import pandas as pd

import InvestCopilot_App.models.toolsutils.dbutils as dbutls
class strategyMode():

    def sg_US_StockPriceVolatility(self):
        #sg_US_SPV	Stock Price Volatility	股价波动
        #所有覆盖的美股里面 前一天涨跌幅》5%or 《-5%的 所有股票
        try:
            stockPools = self.getStockPools()
            #美股
            stocksList = [x for x in stockPools if
                          str(x).split(".")[-1] not in ['HK', 'T', 'TT', 'KS', 'SH', 'SZ', 'TW']]
            con,cur = dbutls.getConnect()
            d_sql="delete from  business.news_title_stocks where title_id='sg_US_SPV' and userid='80'"
            cp_sql="insert into  business.news_title_stocks (userid,title_id,windcode) values (%s,%s,%s) select '80','sg_US_SPV',windcode from spider.east_us_stockprice ecs " \
                   "where tradedate =( select max(tradedate)  from  spider.east_us_stockprice where tradedate < ( select max(tradedate) from  spider.east_us_stockprice  ) ) " \
                   "and (ecs.s_dq_pctchange >5 or ecs.s_dq_pctchange <-5)"

            i_sql="insert into  business.news_title_stocks (userid,title_id,windcode) values (%s,%s,%s)"
            qcode_sql="select windcode from spider.east_us_stockprice ecs " \
                   "where tradedate =( select max(tradedate)  from  spider.east_us_stockprice ) " \
                   "and (ecs.s_dq_pctchange >5 or ecs.s_dq_pctchange <-5)"
            newcodeDF = pd.read_sql(qcode_sql,con)
            newcodes =newcodeDF['windcode'].values.tolist()
            qcode_sql2="""
                select b.windcode from (select windcode,sum(s_dq_pctchange)  s_dq_pctchange  from spider.east_us_stockprice ecs  
                   where tradedate >=( select min(tradedate) from (select tradedate from spider.east_us_stockprice ecs where windcode='AAPL.O' order by tradedate desc limit 3) as y )
                  group by windcode ) b  where   (b.s_dq_pctchange >15 or b.s_dq_pctchange <-15)
            """
            newcodeDF2 = pd.read_sql(qcode_sql2,con)
            newcodes2 =newcodeDF2['windcode'].values.tolist()
            newcodes=newcodes+newcodes2
            newcodes = list(set(stocksList)&set(newcodes))
            # print("newcodes:",len(newcodes),newcodes)
            newdatas=[['80','sg_US_SPV',code] for code  in newcodes]
            # print("newdatas:",newdatas)
            cur.execute(d_sql)
            cur.executemany(i_sql,newdatas)
            print(cur.rowcount)
            con.commit()
        except:
            logging.error(traceback.format_exc())
        finally:
            try:
                cur.close()
            except:pass
            try:
                con.close()
            except:pass
    def sg_HK_StockPriceVolatility(self):
        #sg_HK_SPV	Stock Price Volatility	股价波动
        #所有覆盖的美股里面 前一天涨跌幅》5%or 《-5%的 所有股票
        try:
            stockPools = self.getStockPools()
            #美股
            stocksList = [x for x in stockPools if
                          str(x).split(".")[-1] in ['HK']]
            con,cur = dbutls.getConnect()
            d_sql="delete from  business.news_title_stocks where title_id='sg_HK_SPV' and userid='80'"
            cp_sql="insert into  business.news_title_stocks (userid,title_id,windcode) values (%s,%s,%s) select '80','sg_HK_SPV',windcode from spider.east_hk_stockprice ecs " \
                   "where tradedate =( select max(tradedate)  from  spider.east_hk_stockprice where tradedate < ( select max(tradedate) from  spider.east_hk_stockprice  ) ) " \
                   "and (ecs.s_dq_pctchange >5 or ecs.s_dq_pctchange <-5)"
            #所有覆盖的美股里面 前一天涨跌幅》5%or 《-5%的 所有股票
            i_sql="insert into  business.news_title_stocks (userid,title_id,windcode) values (%s,%s,%s)"
            qcode_sql="select windcode from spider.east_hk_stockprice ecs " \
                   "where tradedate =( select max(tradedate)  from  spider.east_hk_stockprice ) " \
                   "and (ecs.s_dq_pctchange >5 or ecs.s_dq_pctchange <-5)"
            newcodeDF = pd.read_sql(qcode_sql,con)
            newcodes =newcodeDF['windcode'].values.tolist()
            qcode_sql2="""
                select b.windcode from (select windcode,sum(s_dq_pctchange)  s_dq_pctchange  from spider.east_hk_stockprice ecs  
                   where tradedate >=( select min(tradedate) from (select tradedate from spider.east_hk_stockprice ecs where windcode='0700.HK' order by tradedate desc limit 3) as y )
                  group by windcode ) b  where   (b.s_dq_pctchange >15 or b.s_dq_pctchange <-15)
            """
            newcodeDF2 = pd.read_sql(qcode_sql2,con)
            newcodes2 =newcodeDF2['windcode'].values.tolist()
            newcodes=newcodes+newcodes2
            newcodes = list(set(stocksList)&set(newcodes))
            # print("newcodes:",len(newcodes),newcodes)
            newdatas=[['80','sg_HK_SPV',code] for code  in newcodes]
            # print("newdatas:",newdatas)
            cur.execute(d_sql)
            cur.executemany(i_sql,newdatas)
            print(cur.rowcount)
            con.commit()
        except:
            logging.error(traceback.format_exc())
        finally:
            try:
                cur.close()
            except:pass
            try:
                con.close()
            except:pass
    def getStockPools(self):
        import requests
        resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
                             # resp = requests.post("http://192.168.2.6:8008/mobileApp/api/informationApi/",
                             data={'doMethod': "getStockPool",
                                   # "preDays":-1,
                                   #  "nextDays":0,
                                   "trackFlag": "1",
                                   "dataType": "sAts_",
                                   # "symbol":'AAPL.O',#'EXPE.O','MNST.O','BKNG.O'
                                   })
        # print(len(resp.json()['fsymbols']), resp.json()['fsymbols'])
        stocksList = resp.json()['fsymbols']
        return stocksList

if __name__ == '__main__':
    strategyMode().sg_US_StockPriceVolatility()
    strategyMode().sg_HK_StockPriceVolatility()
#     q_dd="""
#
# select windcode,(select stockname from config.stockinfo s where s.windcode=ecs.windcode), s_dq_pctchange,tradedate from spider.east_us_stockprice ecs
# where tradedate =( select max(tradedate)  from  spider.east_us_stockprice where tradedate < ( select max(tradedate) from  spider.east_us_stockprice  ) )
# and (ecs.s_dq_pctchange >5 or ecs.s_dq_pctchange <-5)
#     """
#
#     con, cur = dbutls.getConnect()
#     pd.read_sql(q_dd,con).to_excel("d:/work/temp/us_pct5.xlsx")

