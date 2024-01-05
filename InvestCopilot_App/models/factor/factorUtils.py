#指标处理方法
import pandas as pd
import json
import traceback

import InvestCopilot_App.models.toolsutils.dbutils as dbutils
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as Logger_utils
from django.http import JsonResponse
from InvestCopilot_App.models.cache.dict import dictCache as cache_dict
from InvestCopilot_App.models.market.snapMarket import snapUtils
from InvestCopilot_App.models.cache import cacheDB as cache_db
#数据库缓存DF数据表

Logger = Logger_utils.LoggerUtils()

class factor_utils():
    def get_only_factorids_by_template(self,user_id,template_no):
        """
        获取用户对应模板指标
        1、factorrluser 用户配置指标模板 可配置多个。
        2、factortemplate 每个指标模板添加了指标。
        3、factorcell 每个指标对应的表
        """
        # factorrluser 用户模板表 factortemplate 模板配置指标表  factorcell 指标表
        # template_no为单值时
        returnArray = []
        result = []
        con, cur = dbutils.getConnect()
        userId = str(user_id)
        templateno = str(template_no)
        try:
            # 检查该用户是否有该指标组合 modify robby 不检查，有系统模板都可以看
            q_count = "select count(*) from factorrluser where templateno=%s;"
            cur.execute(q_count, [ templateno])
            fcount_1 = cur.fetchall()[0][0]
            if fcount_1 < 1:
                rsDF0 = pd.DataFrame()
            else:
                sql2 = "select factorno,fmtview from factortemplate where templateno='{}' order by orderno desc;".format(
                    template_no)
                rsDF0 = pd.read_sql(sql2, con=con)
        except Exception as ex:
            Logger.errLineNo(traceback.format_exc())
            rsDF0 = pd.DataFrame()
        finally:
            cur.close()
            con.close()
        rsDF0.fillna('',inplace=True)
        returnArray.append(rsDF0)
        return returnArray

    def get_default_only_factorids_by_template(self,template_no):
        """
        获取用户对应模板指标
        1、factorrluser 用户配置指标模板 可配置多个。
        2、factortemplate 每个指标模板添加了指标。
        3、factorcell 每个指标对应的表
        """
        returnArray = []
        result = []
        con, cur = dbutils.getConnect()
        templateno = str(template_no)
        try:
            # 检查该用户是否有该指标组合 modify robby 不检查，有系统模板都可以看
            q_count = f"""select count(*) from factorrluser where userid='{tools_utils.globa_default_template_userId}'  and templatetype='100' and  templateno=%s ;"""
            # print(q_count)
            cur.execute(q_count, [ templateno])
            fcount_1 = cur.fetchall()[0][0]
            if fcount_1 < 1:
                rsDF0 = pd.DataFrame()
            else:
                sql2 = "select factorno,fmtview from factortemplate where templateno='{}' order by orderno desc;".format(
                    template_no)
                rsDF0 = pd.read_sql(sql2, con=con)
        except Exception as ex:
            Logger.errLineNo(traceback.format_exc())
            rsDF0 = pd.DataFrame()
        finally:
            cur.close()
            con.close()

        rsDF0.fillna('', inplace=True)
        returnArray.append(rsDF0)
        return returnArray

    def get_only_windcodes_by_portfolioid(self,user_id,portfolioId):
        windCodes=[]
        con, cur = dbutils.getConnect()
        userId = str(user_id)
        try:
            if portfolioId == "" or portfolioId is None:
                # 检查该用户是否有该指标组合
                q_count = "select distinct windCode from portfolio.user_portfolio_list where userid=%s order by windCode;"
                cur.execute(q_count, [userId])
            else :
                portfolioId = str(portfolioId)
                # 检查该用户是否有该指标组合
                q_count = "select windCode from portfolio.user_portfolio_list where userid=%s and portfolioid=%s order by seqno desc;"
                cur.execute(q_count, [userId, portfolioId])
            windCodes = cur.fetchall()
            if len(windCodes) > 0:
                windCodes = [x[0] for x in windCodes]
        except Exception as ex:
            Logger.errLineNo(traceback.format_exc())
        finally:
            cur.close()
            con.close()
        return windCodes

    def get_factors_data(self, user_id, template_no,portfolioId):
        rs = ResultData()
        rs.data = {}
        rs.data['columns'] = []
        rs.data['data'] = []
        if portfolioId == "" or portfolioId is None:
            template_no = '8'
        else :
            template_no = str(template_no)
            portfolioId = str(portfolioId)
        user_id = str(user_id)
        Match_col = 'windcode'
        con, cur = dbutils.getConnect()
        rsDF0 = pd.DataFrame()
        count_stockcode=0
        rs.data['stockcounts'] = count_stockcode
        columns_rs = []
        factor_columns = []
        try:
            # 获取指标组合的指标
            fview = self.get_only_factorids_by_template(user_id, template_no)
            stockcodes = self.get_only_windcodes_by_portfolioid(user_id, portfolioId=portfolioId)
            # 获取股票组合的股票列表
            # 该指标模板没有指标数据/该指标模板不属于该用户
            count_stockcode = len(stockcodes)
            if (len(fview[0]) == 0):
                columns_rs = ['windcode', 'stockcode']
                select_columns = ','.join(columns_rs)
                if (len(stockcodes) == 0):
                    # 即没有传入股票，也没传入指标
                    rsDF0 = pd.DataFrame(columns=columns_rs)
                else:
                    if (len(stockcodes) == 1):
                        sql_search_null_fview = f"""select {select_columns} from newdata.a_reuters_factors where {Match_col} = '{stockcodes[0]}';"""
                    else:
                        sql_search_null_fview = f"""select {select_columns} from newdata.a_reuters_factors where {Match_col} in {tuple(stockcodes)};"""
                    rsDF0 = pd.read_sql(sql_search_null_fview, con=con)
            else:
                fview_list = [factor_ids[0] for factor_ids in fview[0].round(4).values.tolist()]
                fview = fview[0]
                sql_search_factor_name = ""
                if (len(fview_list) == 1):
                    sql_search_factor_name = f"select lower(concat_ws('',factortname,factorno)) as matchcolname,fview,floatsize,factorno from factorcell where factorno ={fview_list[0]};"
                else:
                    fview_str = ','.join([str(i) for i in fview_list])
                    sql_search_factor_name = f"select lower(concat_ws('',factortname,factorno)) as matchcolname,fview,floatsize,factorno from factorcell where factorno in {tuple(fview_list)} order by position(factorno::TEXT in '{fview_str}');"

                rsDF0 = pd.read_sql(sql_search_factor_name, con=con)
                merged_fview_1 = pd.merge(rsDF0,fview,on='factorno')
                merged_fview_1['fview']=merged_fview_1.apply(lambda row: row['fmtview'] if row['fmtview']!='' else row['fview'],axis=1)
                merged_fview_1.drop(['fmtview','factorno'],axis=1,inplace=True)
                rsDF0 = merged_fview_1

                # 返回的列名，到时候有行情数据，Price,Change
                columns_search = ['windcode', 'stockcode'] + rsDF0['matchcolname'].to_list()
                search_factors_query = ','.join(columns_search)
                columns_rs = ['windcode', 'Symbol','Price','Change %'] + rsDF0['fview'].to_list()
                factor_columns = ['windcode', 'stockcode',"NOWPRICE","PCTCHANGE"] + rsDF0['matchcolname'].to_list()
                # 返回的列规定位数， 0保留整数，-1是表示字符串，字符串类型在建表时转换过了。
                col_for_round2 = list(set(rsDF0['matchcolname'].to_list()) - set(
                    rsDF0[rsDF0['floatsize'].isin([-1, 0])]['matchcolname'].to_list()))
                stype_round2 = pd.Series([2] * len(col_for_round2), index=[s.lower() for s in col_for_round2])
                stype_round0 = rsDF0[rsDF0['floatsize'] == 0]['matchcolname'].to_list()
                stype_round2 = stype_round2._append(
                    pd.Series([0] * len(stype_round0), index=[s.lower() for s in stype_round0]))
                if (len(stockcodes) == 0):
                    rsDF0 = pd.DataFrame(columns=columns_search)
                else:
                    sql_search_stock_factors = ""
                    if (len(stockcodes) == 1):
                        sql_search_stock_factors = f"select {search_factors_query} from newdata.a_reuters_factors where {Match_col} = '{stockcodes[0]}';"
                    else:
                        sql_search_stock_factors = f"select {search_factors_query} from newdata.a_reuters_factors where {Match_col} in {tuple(stockcodes)};"
                    rsDF0 = pd.read_sql(sql_search_stock_factors, con=con)
                rsDF0 = rsDF0.round(stype_round2)

            rsDF0.fillna('', inplace=True)
            #  newdata.a_reuters_factors 中为空股票的显示
            stock_in = rsDF0['windcode'].to_list()
            list_stock_add = list(set(stockcodes)-set(stock_in))
            if(len(list_stock_add)>0):
                index_stock = len(rsDF0)
                StockInfoDT = cache_dict.getStockInfoDT()
                for stock_num in list_stock_add:
                    rsDF0.loc[index_stock] = {'windcode' : stock_num}
                    if stock_num in StockInfoDT:
                        if StockInfoDT[stock_num]['Area'] == 'AM':
                            rsDF0.at[index_stock, 'stockcode'] = StockInfoDT[stock_num]['Stockcode']
                        else:
                            rsDF0.at[index_stock, 'stockcode'] = StockInfoDT[stock_num]['Stockname']
                        index_stock += 1

            sql_search_stock_name = f"""select stockname,stockcode,windcode from config.stockinfo where windcode in (
            select windcode FROM newdata.a_reuters_factors where position('.' in stockcode)>0 and windcode in(
            select windCode from  portfolio.user_portfolio_list where userid='{user_id}' and portfolioid='{portfolioId}' order by seqno desc));"""
            stocknameDF0 = pd.read_sql(sql_search_stock_name, con=con)
            # 替换stockcode，美股是代码，其他市场股票是公司名称(从config.stockinfo中找stockname)
            # 找到要替换的stockname
            merged = rsDF0.merge(stocknameDF0, on='windcode', how='left')
            rsDF0['stockcode'] = merged['stockname'].fillna(rsDF0['stockcode'])
            rsDF0 = rsDF0.sort_values(by=['windcode'], key=lambda x: x.map({v: i for i, v in enumerate(stockcodes)}))
            # 从缓存中找Price和Change% ，拼接返回 最新行情
            emminhqDF = snapUtils().getRealStockMarketByWindCode(stockcodes)
            emminhqDF=emminhqDF[['WINDCODE','NOWPRICE','PCTCHANGE']]
            outDF=pd.merge(rsDF0,emminhqDF,left_on="windcode",right_on="WINDCODE",how='left')
            outDF=outDF.drop(["WINDCODE"],axis=1)
            rsDF0=outDF[factor_columns]
            rsDF0.fillna('-', inplace=True)
            # outDF=outDF.fillna("-")
        except Exception as ex:
            resultCode = -100
            msg = "查询失败！ 请稍后尝试."
            Logger.errLineNo(traceback.format_exc())
            rs.errorData(translateCode="get_factors_dataError", errorMsg=msg)
            return rs
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        # 返回字段 errorCode , errorMsg, errorFlag , columns，datas
        rs.data['columns'] = columns_rs
        rs.data['data'] = rsDF0.values.tolist()
        rs.data['stockcounts'] = count_stockcode
        return rs


    def getDefaultFactorsDataByWindcode(self,template_no, windcode_list):
        rs = ResultData()
        rs.data = {}
        rs.data['columns'] = []
        rs.data['data'] = []
        template_no = str(template_no)
        Match_col = 'windcode'
        con, cur = dbutils.getConnect()
        rsDF0 = pd.DataFrame()
        count_stockcode=0
        rs.data['stockcounts'] = count_stockcode
        columns_rs = []
        factor_columns = []
        rs.data['msg'] = ""
        try:
            # 获取指标组合的指标
            fview = self.get_default_only_factorids_by_template(template_no)
            if (len(fview[0])<1):
                msg = "不存在该默认模板"
                rs.data['msg'] = msg
            windcode_list = str(windcode_list).split('|')
            windcodes_info = cache_db.getStockInfoCache(reload=False)
            windcodes_total = windcodes_info['WINDCODE']
            windcodes_total = windcodes_total.tolist()
            stockcodes = []
            windcode_set=set()
            for windcode in windcode_list:
                if windcode in windcodes_total and windcode not in windcode_set:
                    stockcodes.append(windcode)
                windcode_set.add(windcode)
            stocknameDF0 = windcodes_info[windcodes_info['WINDCODE'].isin(stockcodes)][['STOCKCODE', 'WINDCODE', 'STOCKNAME']]
            stocknameDF0 = stocknameDF0.rename(columns=str.lower)
            # 获取股票组合的股票列表
            # 该指标模板没有指标数据/该指标模板不属于该用户
            count_stockcode = len(stockcodes)
            if (len(fview[0]) == 0):
                columns_rs = ['windcode', 'stockcode']
                select_columns = ','.join(columns_rs)
                if (len(stockcodes) == 0):
                    # 即没有传入股票，也没传入指标
                    rsDF0 = pd.DataFrame(columns=columns_rs)
                else:
                    if (len(stockcodes) == 1):
                        sql_search_null_fview = f"""select {select_columns} from newdata.a_reuters_factors where {Match_col} = '{stockcodes[0]}';"""
                    else:
                        sql_search_null_fview = f"""select {select_columns} from newdata.a_reuters_factors where {Match_col} in {tuple(stockcodes)};"""
                    rsDF0 = pd.read_sql(sql_search_null_fview, con=con)
            else:
                fview_list = [factor_ids[0] for factor_ids in fview[0].round(4).values.tolist()]
                fview = fview[0]
                sql_search_factor_name = ""
                if (len(fview_list) == 1):
                    sql_search_factor_name = f"select lower(concat_ws('',factortname,factorno)) as matchcolname,fview,floatsize,factorno from factorcell where factorno ={fview_list[0]};"
                else:
                    fview_str = ','.join([str(i) for i in fview_list])
                    sql_search_factor_name = f"select lower(concat_ws('',factortname,factorno)) as matchcolname,fview,floatsize,factorno from factorcell where factorno in {tuple(fview_list)} order by position(factorno::TEXT in '{fview_str}');"
                rsDF0 = pd.read_sql(sql_search_factor_name, con=con)
                merged_fview_1 = pd.merge(rsDF0, fview, on='factorno')
                merged_fview_1['fview'] = merged_fview_1.apply(
                    lambda row: row['fmtview'] if row['fmtview'] != '' else row['fview'], axis=1)
                merged_fview_1.drop(['fmtview', 'factorno'], axis=1, inplace=True)
                rsDF0 = merged_fview_1

                # 返回的列名，到时候有行情数据，Price,Change
                columns_search = ['windcode', 'stockcode'] + rsDF0['matchcolname'].to_list()
                search_factors_query = ','.join(columns_search)
                columns_rs = ['windcode', 'Symbol','Price','Change %'] + rsDF0['fview'].to_list()
                factor_columns = ['windcode', 'stockcode',"NOWPRICE","PCTCHANGE"] + rsDF0['matchcolname'].to_list()
                # 返回的列规定位数， 0保留整数，-1是表示字符串，字符串类型在建表时转换过了。
                col_for_round2 = list(set(rsDF0['matchcolname'].to_list()) - set(
                    rsDF0[rsDF0['floatsize'].isin([-1, 0])]['matchcolname'].to_list()))
                stype_round2 = pd.Series([2] * len(col_for_round2), index=[s.lower() for s in col_for_round2])
                stype_round0 = rsDF0[rsDF0['floatsize'] == 0]['matchcolname'].to_list()
                stype_round2 = stype_round2._append(
                    pd.Series([0] * len(stype_round0), index=[s.lower() for s in stype_round0]))
                if (len(stockcodes) == 0):
                    rsDF0 = pd.DataFrame(columns=columns_search)
                else:
                    sql_search_stock_factors = ""
                    if (len(stockcodes) == 1):
                        sql_search_stock_factors = f"select {search_factors_query} from newdata.a_reuters_factors where {Match_col} = '{stockcodes[0]}';"
                    else:
                        sql_search_stock_factors = f"select {search_factors_query} from newdata.a_reuters_factors where {Match_col} in {tuple(stockcodes)};"
                    rsDF0 = pd.read_sql(sql_search_stock_factors, con=con)
                rsDF0 = rsDF0.round(stype_round2)
            rsDF0.fillna('', inplace=True)

            # 替换stockcode，美股是代码，其他市场股票是公司名称(从config.stockinfo中找stockname)
            # 找到要替换的stockname
            merged = rsDF0.merge(stocknameDF0, on='windcode', how='left')
            rsDF0['stockcode'] = merged['stockname'].fillna(rsDF0['stockcode'])
            rsDF0 = rsDF0.sort_values(by=['windcode'], key=lambda x: x.map({v: i for i, v in enumerate(stockcodes)}))
            # 从缓存中找Price和Change% ，拼接返回 最新行情
            emminhqDF = snapUtils().getRealStockMarketByWindCode(stockcodes)
            emminhqDF=emminhqDF[['WINDCODE','NOWPRICE','PCTCHANGE']]
            outDF=pd.merge(rsDF0,emminhqDF,left_on="windcode",right_on="WINDCODE",how='left')
            outDF=outDF.drop(["WINDCODE"],axis=1)
            rsDF0=outDF[factor_columns]
            rsDF0.fillna('-', inplace=True)
        except Exception as ex:
            resultCode = -100
            msg = "查询失败！ 请稍后尝试."
            Logger.errLineNo(traceback.format_exc())
            rs.errorData(translateCode="get_factors_dataError", errorMsg=msg)
            return rs
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        # 返回字段 errorCode , errorMsg, errorFlag , columns，datas
        rs.data['columns'] = columns_rs
        rs.data['data'] = rsDF0.values.tolist()
        rs.data['stockcounts'] = count_stockcode
        return rs

    def getFactorsDataByWindcode(self, user_id, template_no, windcode_list):
        rs = ResultData()
        rs.data = {}
        rs.data['columns'] = []
        rs.data['data'] = []
        template_no = str(template_no)
        user_id = str(user_id)
        Match_col = 'windcode'
        con, cur = dbutils.getConnect()
        rsDF0 = pd.DataFrame()
        count_stockcode=0
        rs.data['stockcounts'] = count_stockcode
        columns_rs = []
        factor_columns = []
        try:
            # 获取指标组合的指标
            fview = self.get_only_factorids_by_template(user_id, template_no)
            if (len(fview[0])<1):
                msg = "该用户不存在该指标模板"
                rs.data['msg'] = msg
            windcode_list = str(windcode_list).split('|')
            windcodes_info = cache_db.getStockInfoCache(reload=False)
            print(windcodes_info.columns)
            windcodes_total = windcodes_info['WINDCODE']
            windcodes_total = windcodes_total.tolist()
            stockcodes = []
            windcode_set=set()
            for windcode in windcode_list:
                if windcode in windcodes_total and windcode not in windcode_set:
                    stockcodes.append(windcode)
                    windcode_set.add(windcode)
            print(stockcodes)
            stocknameDF0 = windcodes_info[windcodes_info['WINDCODE'].isin(stockcodes)][['STOCKCODE', 'WINDCODE', 'STOCKNAME']]
            stocknameDF0 = stocknameDF0.rename(columns=str.lower)
            print(stocknameDF0)
            # 获取股票组合的股票列表
            # 该指标模板没有指标数据/该指标模板不属于该用户
            count_stockcode = len(stockcodes)
            if (len(fview[0]) == 0):
                columns_rs = ['windcode', 'stockcode']
                select_columns = ','.join(columns_rs)
                if (len(stockcodes) == 0):
                    # 即没有传入股票，也没传入指标
                    rsDF0 = pd.DataFrame(columns=columns_rs)
                else:
                    if (len(stockcodes) == 1):
                        sql_search_null_fview = f"""select {select_columns} from newdata.a_reuters_factors where {Match_col} = '{stockcodes[0]}';"""
                    else:
                        sql_search_null_fview = f"""select {select_columns} from newdata.a_reuters_factors where {Match_col} in {tuple(stockcodes)};"""
                    rsDF0 = pd.read_sql(sql_search_null_fview, con=con)
            else:
                fview_list = [factor_ids[0] for factor_ids in fview[0].round(4).values.tolist()]
                fview = fview[0]
                sql_search_factor_name = ""
                if (len(fview_list) == 1):
                    sql_search_factor_name = f"select lower(concat_ws('',factortname,factorno)) as matchcolname,fview,floatsize,factorno from factorcell where factorno ={fview_list[0]};"
                else:
                    fview_str = ','.join([str(i) for i in fview_list])
                    sql_search_factor_name = f"select lower(concat_ws('',factortname,factorno)) as matchcolname,fview,floatsize,factorno from factorcell where factorno in {tuple(fview_list)} order by position(factorno::TEXT in '{fview_str}');"
                rsDF0 = pd.read_sql(sql_search_factor_name, con=con)
                merged_fview_1 = pd.merge(rsDF0, fview, on='factorno')
                merged_fview_1['fview'] = merged_fview_1.apply(
                    lambda row: row['fmtview'] if row['fmtview'] != '' else row['fview'], axis=1)
                merged_fview_1.drop(['fmtview', 'factorno'], axis=1, inplace=True)
                rsDF0 = merged_fview_1

                # 返回的列名，到时候有行情数据，Price,Change
                columns_search = ['windcode', 'stockcode'] + rsDF0['matchcolname'].to_list()
                search_factors_query = ','.join(columns_search)
                columns_rs = ['windcode', 'Symbol','Price','Change %'] + rsDF0['fview'].to_list()
                factor_columns = ['windcode', 'stockcode',"NOWPRICE","PCTCHANGE"] + rsDF0['matchcolname'].to_list()
                # 返回的列规定位数， 0保留整数，-1是表示字符串，字符串类型在建表时转换过了。
                col_for_round2 = list(set(rsDF0['matchcolname'].to_list()) - set(
                    rsDF0[rsDF0['floatsize'].isin([-1, 0])]['matchcolname'].to_list()))
                stype_round2 = pd.Series([2] * len(col_for_round2), index=[s.lower() for s in col_for_round2])
                stype_round0 = rsDF0[rsDF0['floatsize'] == 0]['matchcolname'].to_list()
                stype_round2 = stype_round2._append(
                    pd.Series([0] * len(stype_round0), index=[s.lower() for s in stype_round0]))
                if (len(stockcodes) == 0):
                    rsDF0 = pd.DataFrame(columns=columns_search)
                else:
                    sql_search_stock_factors = ""
                    if (len(stockcodes) == 1):
                        sql_search_stock_factors = f"select {search_factors_query} from newdata.a_reuters_factors where {Match_col} = '{stockcodes[0]}';"
                    else:
                        sql_search_stock_factors = f"select {search_factors_query} from newdata.a_reuters_factors where {Match_col} in {tuple(stockcodes)};"
                    rsDF0 = pd.read_sql(sql_search_stock_factors, con=con)
                rsDF0 = rsDF0.round(stype_round2)
            rsDF0.fillna('', inplace=True)

            # 替换stockcode，美股是代码，其他市场股票是公司名称(从config.stockinfo中找stockname)
            # 找到要替换的stockname
            merged = rsDF0.merge(stocknameDF0, on='windcode', how='left')
            rsDF0['stockcode'] = merged['stockname'].fillna(rsDF0['stockcode'])
            rsDF0 = rsDF0.sort_values(by=['windcode'], key=lambda x: x.map({v: i for i, v in enumerate(stockcodes)}))
            # 从缓存中找Price和Change% ，拼接返回 最新行情
            emminhqDF = snapUtils().getRealStockMarketByWindCode(stockcodes)
            emminhqDF=emminhqDF[['WINDCODE','NOWPRICE','PCTCHANGE']]
            outDF=pd.merge(rsDF0,emminhqDF,left_on="windcode",right_on="WINDCODE",how='left')
            outDF=outDF.drop(["WINDCODE"],axis=1)
            rsDF0=outDF[factor_columns]
            rsDF0.fillna('-', inplace=True)
        except Exception as ex:
            resultCode = -100
            msg = "查询失败！ 请稍后尝试."
            Logger.errLineNo(traceback.format_exc())
            rs.errorData(translateCode="get_factors_dataError", errorMsg=msg)
            return rs
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        # 返回字段 errorCode , errorMsg, errorFlag , columns，datas
        rs.data['columns'] = columns_rs
        rs.data['data'] = rsDF0.values.tolist()
        rs.data['stockcounts'] = count_stockcode
        return rs

    def get_only_windcodes_for_compare(self,windcode):
        # 1、查询同行市值前8的股票代码
        windCodes = []
        con, cur = dbutils.getConnect()
        windcode = str(windcode)
        try:
            q_wind = """select a.windcode from newdata.a_reuters_factors A where (exchangecountry50,gicssubindustry52)
                    in (select  ARF.exchangecountry50 , arf.gicssubindustry52 gicssubindustry52 from newdata.a_reuters_factors arf where windcode =%s)
                    and  a.companymarketcapitalization28747 is not null
                    order by a.companymarketcapitalization28747  desc limit 8;
            """
            cur.execute(q_wind,[windcode])
            windCodes = cur.fetchall()
            if len(windCodes) > 0:
                windCodes = [x[0] for x in windCodes]
        except Exception as ex:
            Logger.errLineNo(traceback.format_exc())
        finally:
            cur.close()
            con.close()
        return windCodes

    def get_factors_Comparison_among_peers(self,windcode):
        rs = ResultData()
        rs.data = {}
        rs.data['columns'] = []
        rs.data['data'] = []
        Match_col = 'windcode'
        con, cur = dbutils.getConnect()
        rsDF0 = pd.DataFrame()
        count_stockcode = 0
        factor_columns = []
        try:
            # 1、查询同行市值前8的股票代码
            stockcodes = self.get_only_windcodes_for_compare(windcode=windcode)
            #主代码第一位展示
            if windcode in stockcodes:
                findx=stockcodes.index(windcode)
                stockcodes.insert(0,stockcodes.pop(findx))
            else:
                stockcodes.insert(0, windcode)
            stockcodes=stockcodes[0:8]
            # 2、通过 windcode 从 newdata.a_reuters_relativevaluation   查询数据。
            if len(stockcodes) == 0:
                rs.errorData(translateCode="get_factors_Comparison_among_peersError", errorMsg="未找到任何同行市值前8的股票代码，请检查输入的股票id")
                return rs
            # 2.1 获取newdata.a_reuters_relativevaluation列名
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #           where table_schema='newdata' and table_name='a_reuters_relativevaluation';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            # print("','".join(columns))
            # columns = ['windcode','stockcode']+columns[3:]
            columns = ['windcode','stockcode','change1d','totalreturn1m','pe_ntm','ps_ntm','pricecashflowpershare_ntm','evrev_ntm','evebitada_ntm','epsgrowth','roe']
            # columns = ['windcode','stockcode','pe_ntm_rv','evebitda_ntm_rv','evsales_ntm_rv','pcf_ntm_rv','pb_ntm_rv','dividend_yield_ntm_rv','pe_ttm_rv','evebitda_ttm_rv','evsales_ttm_rv','pcf_ttm_rv','pb_ttm_rv','dividend_yield_ttm_rv','change1d','totalreturn1m','pe_ntm','ps_ntm','pricecashflowpershare_ntm','evrev_ntm','evebitada_ntm','epsgrowth','roe','tradedate']
            select_columns = ','.join(columns)
            # 2.2通过 windcode 从 newdata.a_reuters_relativevaluation 查询数据
            if (len(stockcodes) == 1):
                sql_search = f"""select {select_columns} from newdata.a_reuters_relativevaluation where windcode='{stockcodes[0]}';"""
            else:
                windcode_str = ','.join([str(i) for i in stockcodes])
                sql_search = f"""select {select_columns} from newdata.a_reuters_relativevaluation where windcode in {tuple(stockcodes)}
                        order by position(windcode::text in '{windcode_str}');"""
            rsDF0 = pd.read_sql(sql_search, con=con)
            rsDF0.fillna('', inplace=True)
            # 2.3 从缓存中 美股stockcode没有带后缀， 其他市场的是带了后缀
            filtered_values = rsDF0[rsDF0['stockcode'].str.contains('.', regex=False)]
            # print("filtered_values      ",filtered_values)
            if(len(filtered_values)!=0):
                # 表示有其他市场的数据，故需要把stockcode换成stockname
                # all_stockinfo = cache_db.getStockInfoDF(reload=False)
                all_stockinfo = cache_db.getStockInfoCache ()
                all_stockinfo = all_stockinfo.fillna('')
                # print(all_stockinfo.columns) #['STOCKCODE', 'WINDCODE', 'EASTCODE', 'STOCKNAME', 'AREA', 'STOCKTYPE', 'PINYIN', 'SEARCH']
                # 美股stockcode没有带后缀， 其他市场的是带了后缀
                stocknameDF0 = all_stockinfo.loc[all_stockinfo['WINDCODE'].isin(filtered_values['windcode']),['STOCKCODE', 'WINDCODE','STOCKNAME']]
                stocknameDF0.rename(str.lower, axis='columns', inplace=True)
                merged = rsDF0.merge(stocknameDF0, on='windcode', how='left')
                rsDF0['stockcode'] = merged['stockname'].fillna(rsDF0['stockcode'])
            rsDF0.fillna('', inplace=True)
            numeric_cols = ['change1d','totalreturn1m','pe_ntm','ps_ntm','pricecashflowpershare_ntm','evrev_ntm','evebitada_ntm','epsgrowth','roe']
            fill_and_round = lambda x: round(float(x), 1) if x != '-' and x!='' else '-'
            rsDF0[numeric_cols] = rsDF0[numeric_cols].applymap(fill_and_round)
            rsDF0 = rsDF0.sort_values(by=['windcode'],key=lambda x: x.map({v: i for i, v in enumerate(stockcodes)}))
            rsDF0.fillna('-', inplace=True)
        except Exception as ex:
            msg = "查询失败！ 请稍后尝试."
            Logger.errLineNo(traceback.format_exc())
            rs.errorData(translateCode="Error", errorMsg=msg)
            return rs
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        # columns_rs = rsDF0.columns.tolist()
        # columns_rs[1]= 'Symbol'
        # columns_rs = ['windcode', 'Symbol', 'change1d', 'totalreturn1m', 'pe_ntm', 'ps_ntm', 'pricecashflowpershare_ntm', 'evrev_ntm', 'evebitada_ntm', 'epsgrowth', 'roe', 'tradedate']
        columns_rs = ['WindCode', 'Symbol', '%1D', '%1M', 'PE(NTM)', 'PS(NTM)', 'P/CFPS(NTM)', 'EV/REV(NTM)', 'EV/EBITADA(NTM)', 'Eps Growth', 'ROE']
        rs.data['columns'] = columns_rs
        rs.data['data'] = rsDF0.values.tolist()
        return rs

    def get_factors_indexcode(self,INDEXCODE):
        rs = ResultData()
        rs.data = {'columns':[],'data':[]}
        con, cur = dbutils.getConnect()
        rsDF0 = pd.DataFrame()
        factor_columns = []
        INDEXCODE = str(INDEXCODE)
        try:
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #           where table_schema='newdata' and table_name='reuters_us_indexprice';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            # print("','".join(columns))
            columns = ['indexcode','indexname','s_dq_close','tradedate']
            select_columns = ','.join(columns)
            sql_search = f"""select {select_columns} from newdata.reuters_us_indexprice where INDEXCODE ='{INDEXCODE}' 
                    order by tradedate desc limit 20;"""
            rsDF0 = pd.read_sql(sql_search, con=con)
            rsDF0.fillna('', inplace=True)
        except Exception as ex:
            msg = "查询失败！ 请稍后尝试."
            Logger.errLineNo(traceback.format_exc())
            rs.errorData(translateCode="Error", errorMsg=msg)
            return rs
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        rs.data['columns'] = columns
        rs.data['data'] = rsDF0.values.tolist()
        return rs


    def get_factors_by_template(self,user_id,template_no):
        """
        获取用户对应模板指标
        1、factorrluser 用户配置指标模板 可配置多个。
        2、factortemplate 每个指标模板添加了指标。
        3、factorcell 每个指标对应的表
        """
        # factorrluser 用户模板表 factortemplate 模板配置指标表  factorcell 指标表
        # template_no为单值时
        returnArray = []
        result = []
        if len(result) != 0:
            for i in range(len(result)):
                result[i].fillna('', inplace=True)
                returnArray.append(result[i])
        else:
            con, cur = dbutils.getConnect()
            user_id = str(user_id)
            template_no = str(template_no)
            try:
                #检查该用户是否有该指标组合
                sql = "select * from factorrluser where userid='{}' and templateno ='{}'".format(user_id,template_no)
                rsDF0 = pd.read_sql(sql, con=con)
                if len(rsDF0) == 0:
                    rsDF0 = pd.DataFrame()
                else:
                    sql2 = "select factorno,templateno,orderno,statementtype,reportperiod from factortemplate where templateno='{}' order by orderno ".format(template_no)
                    rsDF0 = pd.read_sql(sql2, con=con)
            except Exception as ex:
                Logger.errLineNo(traceback.format_exc())
                rsDF0 = pd.DataFrame()
            finally:
                cur.close()
                con.close()
        rsDF0.fillna('', inplace=True)
        returnArray.append(rsDF0)
        return returnArray



def just_test_exp(choice_for_two_function):
    # choice_for_two_function=1
    Factor = factor_utils()
    if choice_for_two_function==1:
        fview = Factor.get_factors_by_template(tools_utils.globa_default_template_userId, tools_utils.globa_default_template_no)
        print("fview",fview)   # 返回一个列表，  type(fview[0]) 为dataframe
    elif choice_for_two_function==2:
        # 可以修改参数的TYPE_CHOICE=2，为1是jsonresponce
        stock_seach = ['002735.SZ', 'FGBIP', '8187.HK', 'MNTA', 'THACU', '6663.HK']
        stock_seach = ['002735.SZ']
        resultCode, resultMsg, resultFlag, columns, datas = Factor.get_factors_data_before(tools_utils.globa_default_template_userId, tools_utils.globa_default_template_no, stockcodes=stock_seach)
        print(resultCode, resultMsg, resultFlag, columns, datas)
    elif choice_for_two_function==3:
        stock_seach = ['002735.SZ', 'FGBIP', '8187.HK', 'MNTA', 'THACU', '6663.HK']
        resultCode, resultMsg, resultFlag, columns, datas = Factor.get_factors_data_before(tools_utils.globa_default_template_userId, tools_utils.globa_default_template_no, stockcodes=stock_seach)
        print(resultCode, resultMsg, resultFlag, columns, datas)
    elif choice_for_two_function==4:
        stock_seach = ['002735.SZ', 'FGBIP', '8187.HK', 'MNTA', 'THACU', '6663.HK'] #测试用例1
        # stock_seach = ['6663.HK'] #测试用例2
        # stock_seach = ['002735.SZ'] #测试用例3
        # stock_seach = [] #测试用例4
        rs = Factor.get_factors_data(tools_utils.globa_default_template_userId,tools_utils.globa_default_template_no, stockcodes=stock_seach)
        print(rs.toDict())
    elif choice_for_two_function==5:
        fview = Factor.get_only_factorids_by_template(tools_utils.globa_default_template_userId,tools_utils.globa_default_template_no)
        print("fview", fview)  # 返回一个列表，  type(fview[0]) 为dataframe


if __name__ == '__main__':
    # just_test_exp(4)

    Factor = factor_utils()
    stock_seach = ['002735.SZ', 'AMD.O', '8187.HK', 'AAPL.O', 'ORCL.N', '6663.HK']  # 测试用例1
    # # stock_seach = ['6663.HK'] #测试用例2
    # # stock_seach = ['002735.SZ'] #测试用例3
    # # stock_seach = [] #测试用例4
    # rs = Factor.get_factors_data("73", "100_17", "self_2")
    # rs = Factor.get_factors_Comparison_among_peers('SMCI.O')
    # rs = Factor.get_only_windcodes_for_compare('AAPL.O')
    rs = Factor.get_only_windcodes_for_compare('MCHP.O')
    print(rs)
    rs = Factor.get_factors_Comparison_among_peers('LRMR.O')
    # rs = Factor.getDefaultFactorsDataByWindcode( '9', ('|').join(stock_seach))
    x = rs.toDict()
    print(x)
    # rs = Factor.getFactorsDataByWindcode('73','100_200',('|').join(stock_seach))
    # x = rs.toDict()
    # print(x)
    # rs = Factor.get_factors_Comparison_among_peers('0371.HK')
    # rs = Factor.get_factors_indexcode('41620')
    # x=rs.toDict()
    # print(x)

    # import smtplib
    # from django.conf import settings
    # from email.mime.multipart import MIMEMultipart
    # from email.mime.text import MIMEText
    # from email.mime.image import MIMEImage
    # from email.header import Header
    # rs = Factor.get_default_only_factorids_by_template("9")
    # print(rs)
    # email ="18817781975@163.com"
    # pwd = "dsadasdd"
    # if settings.SEND_EMAIL_NOTIFICATION_WHEN_CREATE_USER == True:
    #     mail_host = "smtp.exmail.qq.com"
    #     send_addr = "notice@pinnacle-cap.cn"
    #     send_pwd = "gCWoruifU5ZUCn6C"
    #     to_addr = email
    #     text = f"""
    #     <p>尊敬的用户 您好，您的试用账号已开通</p>
    #     <p>您的试用账号已开通。以下是您的登录信息：</p>
    #     <p> 登录网址: <a href="https://www.intellistock.cn/#/user/login">https://www.intellistock.cn/#/user/login</a></p>
    #     <p>用户名: {email}</p>
    #     <p>密码: {pwd}</p>
    #     <p>您可以点击上方链接直接访问登录页面。使用提供的用户名和密码进行登录。</p>
    #     <p>祝您使用愉快！如果您有其他问题，请随时提问。</p>
    #     """
    #     message = MIMEMultipart()
    #     html = MIMEText(text, 'html', 'utf-8')
    #     message['From'] = send_addr
    #     message['To'] = to_addr
    #     subject = "Intelli Stock:试用账号开通成功"  # 主题
    #     message['Subject'] = Header(subject, 'utf-8')
    #     message.attach(html)
    #     rtdata = {"msg": ""}
    #     try:
    #         smtp = smtplib.SMTP_SSL(mail_host, 465)
    #         smtp.login(send_addr, send_pwd)
    #         smtp.sendmail(send_addr, to_addr, message.as_string())
    #         print(message)
    #         print(send_addr)
    #         print(to_addr)
    #         rtdata['msg'] = "邮件发送成功"
    #         smtp.quit()
    #     except Exception as e:
    #         rtdata['msg'] = "Error: 无法发送邮件"
    #     finally:
    #         print(rtdata.items())