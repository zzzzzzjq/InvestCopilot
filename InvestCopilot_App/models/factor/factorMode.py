# -*- coding: utf-8 -*-

import json
import math
import time
import traceback
import datetime
import re
import socket
import os
import sys
import requests
import math
import threading
import pandas as pd
from pymongo import UpdateOne

from django.http import HttpResponseRedirect
sys.path.append("../..")

from io import StringIO
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.user.userMode import cuserMode
from  InvestCopilot_App.models.market.snapMarket import snapUtils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.cache import cacheDB as cache_db

import logging
from InvestCopilot_App.models.toolsutils import dbmongo

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

class cfactorMode():


    # 修改1201，新增portfolioId
    def addFactorTemplate(self,templatename,userId,portfolioId,templatetype='100'):
        #创建指标组合
        userId=str(userId)
        templatename = str(templatename)
        portfolioId = str(portfolioId)
        con, cur = dbutils.getConnect()
        rst = ResultData()
        try:
            #检查是是否存在
            q_one = "select count(1) from factorrluser where userid=%s and templatename=%s and portfolioid=%s and templatetype=%s;"
            cur.execute(q_one,[userId,templatename,portfolioId,templatetype])
            fcount = cur.fetchall()[0][0]
            if fcount>0:
                msg = "This name is already in use."
                rst.errorData(translateCode="FactorCombinationAdd",errorMsg=msg)
                return rst
            selectSql = "SELECT nextval('seq_factorusertemplateid') AS templateno;"
            cur.execute(selectSql)
            templateNo = cur.fetchall()[0][0]
            stemplateNo = templatetype +"_"+ str(templateNo)
            # 获取组合最大排序编号
            q_maxSeqNo = "select coalesce(max(templateorder),1) from  factorrluser where userId=%s;"
            cur.execute(q_maxSeqNo, [userId])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo) + 1
            creatime = datetime.datetime.now()
            insertSql = "INSERT INTO factorrluser (userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate,portfolioid,tcreatetime) VALUES(%s, %s, %s,%s,%s,%s,%s,%s,%s,%s);"
            str_desc ="用户自选指标组合模板"
            cur.execute(insertSql, [userId, stemplateNo, templatename,'1','100',ordNo ,str_desc,'1',portfolioId,creatime])
            con.commit()
            data={"templateno":stemplateNo}
            rst.data=data
            return rst
        except:
            msg = "Sorry, Create FactorCombination failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="FactorCombinationAddError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def delFactorTemplate(self,templateNo,userId):
        # 组合删除
        con, cur = dbutils.getConnect()
        templateNo = str(templateNo)
        userId = str(userId)
        # portfolioId = str(portfolioId)
        rst = ResultData()
        try:
            q_count = "select count(1) from  factorrluser  where userid=%s and templateno =%s;"
            cur.execute(q_count, [userId, templateNo])
            # q_count = "select count(1) from  factorrluser  where userid=%s and templateno =%s and portfolioid =%s;"
            # cur.execute(q_count,[userId,templateNo,portfolioId])
            fnum=cur.fetchone()[0]
            if fnum<1:
                msg = "Sorry, need to reserve 1 templateFactor."
                rst.errorData(translateCode="FactorTemplateDel",errorMsg=msg)
                return rst
            d_factorrluser = "delete from factorrluser where userid=%s and templateno=%s;"
            d_factortemplate = "delete from factortemplate where templateno=%s;"
            cur.execute(d_factortemplate,[templateNo])
            cur.execute(d_factorrluser, [userId, templateNo])
            rowcount=cur.rowcount
            con.commit()
            data={'rowcount':rowcount}
            rst.data=data
            return rst
        except:
            msg = "Sorry, delete FactorTemplate failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="FactorTemplateDelError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def editFactorTemplate(self,portfolioId,templateNo,templatename,userId,factorNoList):
        # 组合修改
        con, cur = dbutils.getConnect()
        rst = ResultData()
        templateNo = str(templateNo)
        userId = str(userId)
        templatename = str(templatename).strip()
        portfolioId = str(portfolioId)
        print(factorNoList)
        if (templateNo=='8'):
            # 默认模板的修改，首先拿到全部是为8的数据，只要调用该接口就创建新模板
            if len(templatename)<=0:
                templatename = "默认模版-change"
            # 1.检查指标名是否存在
            q_one = "select count(1) from factorrluser where userid=%s and templatename=%s and portfolioId=%s"
            cur.execute(q_one, [userId, templatename, portfolioId])
            fcount = cur.fetchall()[0][0]
            if fcount > 0:
                msg = "This template name is already in use."
                rst.errorData(translateCode="editFactorTemplate", errorMsg=msg)
                return rst
            # 3.获取该用户应插入的指标模板号
            selectSql = "SELECT nextval('seq_factorusertemplateid') AS templateno;"
            cur.execute(selectSql)
            templateNo = cur.fetchall()[0][0]
            stemplateNo = "100_" + str(templateNo)
            # 3. 获取该用户指标组合最大排序编号
            q_maxSeqNo = "select coalesce(max(templateorder),1) from factorrluser where userId=%s;"
            cur.execute(q_maxSeqNo, [userId])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo) + 1
            create_time = datetime.datetime.now()
            # 4. 插入指标模板
            insertSql = "INSERT INTO factorrluser (userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate,portfolioid,tcreatetime) VALUES(%s, %s, %s,%s,%s,%s,%s,%s,%s,%s);"
            str_desc = "用户自选指标组合模板"
            cur.execute(insertSql,
                        [userId, stemplateNo, templatename, '1', '100', ordNo, str_desc, '1', portfolioId,create_time])
            templateno = stemplateNo
            # 获取指标模板中最大排序编号
            q_maxSeqNo = "select coalesce(max(orderno),0) from factortemplate where templateno=%s"
            cur.execute(q_maxSeqNo, [templateno])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo)
            adfs = []
            factorNoList.reverse()
            factorDescData = cache_db.getFactorCellDF(reload=False)
            factornos_cache = factorDescData['FACTORNO']
            for ord, factorno in enumerate(factorNoList):
                if factorno in factornos_cache.values:
                    adfs.append([str(templateno), factorno, ord + 1])
            if (len(adfs)>0):
                insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
                cur.executemany(insertSql, adfs)
            con.commit()
            data = {"templateno": templateno}
            con.commit()
            rst.data =data
            return rst
        else:
            try:
                q_count = "select * from factorrluser where userid=%s and templateno=%s and portfolioId=%s"
                oneDF=pd.read_sql(q_count,con,params=[userId,templateNo,portfolioId])
                oneDF=tools_utils.dfColumUpper(oneDF)
                if oneDF.empty:
                    msg = "Sorry, templateno not found."
                    rst.errorData(translateCode="FactorTemplateEdit",errorMsg=msg)
                    return rst
                utdt = oneDF.iloc[0]
                #update templatename修改指标模板名称
                templatename_rowcount=0
                if len(templatename)>0:
                    db_templatename = str(utdt.TEMPLATENAME)
                    updt = {}
                    if str(db_templatename)!=templatename:
                        updt['templatename']=templatename
                        usql="""update factorrluser set templatename=%s where userid=%s and templateno=%s;"""
                        cur.execute(usql,[templatename,userId,templateNo])
                        templatename_rowcount = cur.rowcount
                #update  factorno顺序，删除，新增指标,但不会检查该指标是否存在，默认存在
                # 从本地获取指标名列表
                search_col = f"select * from factortemplate where templateno='{templateNo}';"
                cur.execute(search_col)
                local_factorsymbols =cur.fetchall()
                local_factorsymbols = [str(row[1]) for row in local_factorsymbols]
                # add_factor_list = list(set(factorNoList)-set(local_factorsymbols))
                add_factor_list = [value for value in factorNoList if value not in local_factorsymbols]
                del_list = list(set(local_factorsymbols)-set(factorNoList))
                upfs=[]
                adfs=[]
                factorNoList.reverse()
                # 删除删除的指标
                if(len(del_list)>0):
                    for row in del_list:
                        delete_sql= f"delete from factortemplate where templateno=%s and factorno=%s;"
                        cur.execute(delete_sql,[str(templateNo),row])
                for ord, factorno in enumerate(factorNoList):
                    if factorno in add_factor_list:
                        adfs.append([str(templateNo), factorno, ord+1])
                    else:
                        upfs.append( [ord+1,str(templateNo), factorno])
                if(len(upfs)>0):
                    update_ord = "update factortemplate set orderno=%s where templateno=%s and factorno=%s"
                    cur.executemany(update_ord,upfs)
                if(len(adfs)>0):
                    factorDescData = cache_db.getFactorCellDF(reload=False)
                    factornos_cache = factorDescData['FACTORNO']
                    insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
                    for adfs_i in adfs:
                        if adfs_i[1] in factornos_cache.values:
                            cur.execute(insertSql, adfs_i)
                factor_rowcount = cur.rowcount
                con.commit()
                rst.data={"templatename_rowcount":templatename_rowcount,"factor_rowcount":factor_rowcount,}
                return rst
            except:
                msg = "Sorry, modify templateName failed. Please try again later."
                Logger.errLineNo(msg=msg)
                rst.errorData(translateCode="FactorTemplateEditError",errorMsg=msg)
                return rst
            finally:
                try:cur.close()
                except:pass
                try:con.close()
                except:pass

    def copyFactorTemplatewithNameChange(self,portfolioIdsTo,templateNo,userId,templatename=''):
        # 拷贝指标模板到其他的股票组合下
        # 无需porfolioIdFrom,因为所有的templateno是唯一的
        # 该用户的portfolioIdsTo股票均具有templateno模板
        con, cur = dbutils.getConnect()
        rst = ResultData()
        templateNo = str(templateNo)
        userId = str(userId)
        portfolioIdsTo= str(portfolioIdsTo)
        templatename = str(templatename)
        # porfolioIdFrom = str(porfolioIdFrom)
        portfolioIds = portfolioIdsTo.split('|')
        portfolioIdsTo=[]
        for portfolid_i in portfolioIds:
            portstr = str(portfolid_i).strip()
            if len(portstr) == 0:
                continue
            portfolioIdsTo.append(portstr)
        if len(portfolioIdsTo) <= 0:
            msg = "股票组合数量必须大于零"
            rst.errorData(translateCode="FactorTemplatewithNameChangeCopy",errorMsg=msg)
            return rst
        try:
            quserId=userId
            q_one = "select userid from factorrluser where templateno= %s "
            tdf = pd.read_sql(q_one, con, params=[templateNo])
            if tdf.empty:
                msg = "Sorry, templateno not found."
                rst.errorData(translateCode="copyFactorTemplatewithNameChange", errorMsg=msg)
                return rst
            if str(tdf.values.tolist()[0][0]) == tools_utils.globa_default_template_userId:
                quserId = tools_utils.globa_default_template_userId  # 默认模版 都可以查询

            # 1. 检查指标模板是否存在该用户名下（可以注释掉，即默认只会传入合法参数）
            q_count = "select * from factorrluser where userid=%s and templateno=%s;"
            oneDF=pd.read_sql(q_count,con,params=[quserId,templateNo])
            oneDF=tools_utils.dfColumUpper(oneDF)
            if oneDF.empty:
                msg = "Sorry, templateno not found."
                rst.errorData(translateCode="FactorTemplatewithNameChangeCopy",errorMsg=msg)
                return rst
            templatetype = oneDF["TEMPLATETYPE"].iloc[0]
            if templatename == '' or templatename == None or templatename == 'None':
                templatename = oneDF["TEMPLATENAME"].iloc[0]
            # 2. 获取要插入的数据
            factor_orderno ="select factorno,orderno from factortemplate where templateno=%s order by orderno;"
            cur.execute(factor_orderno,[templateNo])
            factor_orderno = cur.fetchall()
            factor_list = [[row[0],row[1]] for row in factor_orderno]
            count_not_exist_portfolio=[]
            count_exist_templatename =[]
            for portfolioId in portfolioIdsTo:
                # 3. 检查该用户是否有要添加指标模板的 股票组合id列表（可以注释掉，即默认只会传入合法参数）
                q_count = "select count(1) from portfolio.user_portfolio where userid=%s and portfolioid=%s;"
                cur.execute(q_count,[userId,portfolioId])
                if_exists = cur.fetchone()[0]
                if(if_exists<1):
                    count_not_exist_portfolio.append(portfolioId)
                    continue
                # 4. 插入factorrluser该模板
                # 4.1 检查是否有该模板名,有的话不操作,计数
                q_one = "select count(1) from factorrluser where userid=%s and templatename=%s and portfolioId=%s and templatetype=%s"
                cur.execute(q_one, [userId, templatename, portfolioId, templatetype])
                fcount = cur.fetchall()[0][0]
                if fcount > 0:
                    str_temp = portfolioId +" already have templatename "+str(templatename)
                    count_exist_templatename.append(str_temp)
                    continue
                # 4.2 获取该用户应插入的指标模板id
                selectSql = "SELECT nextval('seq_factorusertemplateid') AS templateno;"
                cur.execute(selectSql)
                templateNo = cur.fetchall()[0][0]
                stemplateNo = templatetype + "_" + str(templateNo)
                # 4.3 获取该用户指标组合最大排序编号
                q_maxSeqNo = "select coalesce(max(templateorder),1) from factorrluser where userId=%s;"
                cur.execute(q_maxSeqNo, [userId])
                ordNo = cur.fetchone()[0]
                ordNo = int(ordNo) + 1
                createtime = datetime.datetime.now()
                str_desc = "用户自选指标组合模板"
                insertSql = f"""INSERT INTO factorrluser (userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate,portfolioid,tcreatetime) 
                VALUES('{userId}','{stemplateNo}','{templatename}','1','{templatetype}','{ordNo}','{str_desc}','1','{portfolioId}','{createtime}');"""
                cur.execute(insertSql)
                con.commit()
                # 5. 插入指标
                factor_list_index = [[str(stemplateNo)]+row for row in factor_list]
                insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
                cur.executemany(insertSql, factor_list_index)
                con.commit()
            msg=''
            if(len(count_not_exist_portfolio)>0):
                msg += ' , '.join(count_not_exist_portfolio) +" These portfolioid do not exist!"
            if(len(count_exist_templatename)>0):
                msg += ' , '.join(count_exist_templatename)
            data = {"not_exist_portfolioid":len(count_not_exist_portfolio),"exist_templatename":len(count_exist_templatename),"sucess_count":len(portfolioIdsTo)-len(count_exist_templatename)-len(count_not_exist_portfolio),"comment":msg}
            rst.data = data
            return rst
        except:
            msg = "Sorry, modify templateName failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="FactorTemplateEditError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def copyFactorTemplate(self,portfolioIdsTo,templateNo,userId):
        # 拷贝指标模板到其他的股票组合下
        # 无需porfolioIdFrom,因为所有的templateno是唯一的
        # 该用户的portfolioIdsTo股票均具有templateno模板
        con, cur = dbutils.getConnect()
        rst = ResultData()
        templateNo = str(templateNo)
        userId = str(userId)
        portfolioIdsTo= str(portfolioIdsTo)
        # porfolioIdFrom = str(porfolioIdFrom)
        portfolioIds = portfolioIdsTo.split('|')
        portfolioIdsTo=[]
        for portfolid_i in portfolioIds:
            portstr = str(portfolid_i).strip()
            if len(portstr) == 0:
                continue
            portfolioIdsTo.append(portstr)
        if len(portfolioIdsTo) <= 0:
            msg = "股票组合数量必须大于零"
            rst.errorData(translateCode="FactorTemplateCopy",errorMsg=msg)
            return rst
        try:
            # 1. 检查指标模板是否存在该用户名下（可以注释掉，即默认只会传入合法参数）
            q_count = "select * from factorrluser where userid=%s and templateno=%s;"
            oneDF=pd.read_sql(q_count,con,params=[userId,templateNo])
            oneDF=tools_utils.dfColumUpper(oneDF)
            if oneDF.empty:
                msg = "Sorry, templateno not found."
                rst.errorData(translateCode="FactorTemplateCopy",errorMsg=msg)
                return rst
            templatetype = oneDF["TEMPLATETYPE"].iloc[0]
            templatename = oneDF["TEMPLATENAME"].iloc[0]

            # 2. 获取要插入的数据
            factor_orderno ="select factorno,orderno from factortemplate where templateno=%s order by orderno;"
            cur.execute(factor_orderno,[templateNo])
            factor_orderno = cur.fetchall()
            factor_list = [[row[0],row[1]] for row in factor_orderno]
            count_not_exist_portfolio=[]
            count_exist_templatename =[]
            for portfolioId in portfolioIdsTo:
                # 3. 检查该用户是否有要添加指标模板的 股票组合id列表（可以注释掉，即默认只会传入合法参数）
                q_count = "select count(1) from portfolio.user_portfolio where userid=%s and portfolioid=%s;"
                cur.execute(q_count,[userId,portfolioId])
                if_exists = cur.fetchone()[0]
                if(if_exists<1):
                    count_not_exist_portfolio.append(portfolioId)
                    continue
                # 4. 插入factorrluser该模板
                # 4.1 检查是否有该模板名,有的话不操作,计数
                q_one = "select count(1) from factorrluser where userid=%s and templatename=%s and portfolioId=%s and templatetype=%s"
                cur.execute(q_one, [userId, templatename, portfolioId, templatetype])
                fcount = cur.fetchall()[0][0]
                if fcount > 0:
                    str_temp = portfolioId +" already have templatename "+str(templatename)
                    count_exist_templatename.append(str_temp)
                    continue
                # 4.2 获取该用户应插入的指标模板id
                selectSql = "SELECT nextval('seq_factorusertemplateid') AS templateno;"
                cur.execute(selectSql)
                templateNo = cur.fetchall()[0][0]
                stemplateNo = templatetype + "_" + str(templateNo)
                # 4.3 获取该用户指标组合最大排序编号
                q_maxSeqNo = "select coalesce(max(templateorder),1) from factorrluser where userId=%s;"
                cur.execute(q_maxSeqNo, [userId])
                ordNo = cur.fetchone()[0]
                ordNo = int(ordNo) + 1
                createtime = datetime.datetime.now()
                str_desc = "用户自选指标组合模板"
                insertSql = f"""INSERT INTO factorrluser (userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate,portfolioid,tcreatetime) 
                VALUES('{userId}','{stemplateNo}','{templatename}','1','{templatetype}','{ordNo}','{str_desc}','1','{portfolioId}','{createtime}');"""
                cur.execute(insertSql)
                con.commit()
                # 5. 插入指标
                factor_list_index = [[str(stemplateNo)]+row for row in factor_list]
                insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
                cur.executemany(insertSql, factor_list_index)
                con.commit()
            msg=''
            if(len(count_not_exist_portfolio)>0):
                msg += ' , '.join(count_not_exist_portfolio) +" These portfolioid do not exist!"
            if(len(count_exist_templatename)>0):
                msg += ' , '.join(count_exist_templatename)
            data = {"not_exist_portfolioid":len(count_not_exist_portfolio),"exist_templatename":len(count_exist_templatename),"sucess_count":len(portfolioIdsTo)-len(count_exist_templatename)-len(count_not_exist_portfolio),"comment":msg}
            rst.data = data
            return rst
        except:
            msg = "Sorry, modify templateName failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="FactorTemplateEditError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def addFactorSymbol(self,factorno,templateno,userId):
        # 组合添加指标代码
        con, cur = dbutils.getConnect()
        rst = ResultData()
        templateno = str(templateno)
        userId = str(userId)
        factorno = str(factorno)
        try:
            ### 1.检查一下该用户是否有该指标组合，但是应该是有才会调用的
            q_count = "select count(1) from factorrluser where userid=%s and templateno=%s"
            cur.execute(q_count, [userId, templateno])
            fcount_1 = cur.fetchall()[0][0]
            # print("fcount_1",fcount_1)
            if fcount_1<1:
                msg = "Sorry, templateno not found."
                rst.errorData(translateCode="FactorSymbolAdd",errorMsg=msg)
                return rst
            ###end
            # 2. 检查指标是否存在于factortemplate，有就不再重复插入
            q_one = "select count(1) from factortemplate where templateno =%s and factorno=%s;"
            cur.execute(q_one, [templateno,factorno])
            fcount = cur.fetchall()[0][0]
            if fcount > 0:
                msg = "factorno is exists."
                rst.errorData(errorMsg=msg)
                return rst
            # 3. 查看该指标factorno是否存在于缓存中/factorcell中，不存在，不插入
            factorDescData = cache_db.getFactorCellDF(reload=False)
            factornos_cache = factorDescData['FACTORNO']
            if factorno in factornos_cache.values:
                # 获取组合最大排序编号
                q_maxSeqNo = "select coalesce(max(orderno),0) from factortemplate where templateno=%s"
                cur.execute(q_maxSeqNo,[templateno])
                ordNo = cur.fetchone()[0]
                ordNo = int(ordNo) + 1
                insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
                cur.execute(insertSql, [templateno, factorno, ordNo])
                con.commit()
                rst.errorMsg = "组合添加指标成功"
            else:
                msg = "factorno not exists in factorcell, please check it."
                rst.errorData(errorMsg=msg)
            return rst
        except:
            msg = "Sorry, add symbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="addFactorSymbolError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def createTemplateAndFactors(self,templatename,userId,factornos,portfolioId,templatetype='100',portfoliotype='self'):
        # 创建指标组合addFactorTemplate
        userId = str(userId)
        templatename = str(templatename)
        portfolioId = str(portfolioId)
        con, cur = dbutils.getConnect()
        rst = ResultData()
        try:
            # 1.检查指标名是否存在
            q_one = "select count(1) from factorrluser where userid=%s and templatename=%s and portfolioId=%s and templatetype=%s"
            cur.execute(q_one, [userId, templatename,portfolioId,templatetype])
            fcount = cur.fetchall()[0][0]
            if fcount > 0:
                msg = "This template name is already in use."
                rst.errorData(translateCode="createTemplateAndFactors", errorMsg=msg)
                return rst
            # # 2.在user_portfolio检查股票id是否存在,一个股票组合可以有多个模板
            # q_one = "select count(1) from portfolio.user_portfolio where userid=%s and portfolioid= %s and portfoliotype=%s;"
            # cur.execute(q_one, [userId, portfolioId,portfoliotype])
            # fcount = cur.fetchall()[0][0]
            # if fcount < 1:
            #     msg = "The stock portfolio does not exist."
            #     rst.errorData(translateCode="FactorTemplateAdd", errorMsg=msg)
            #     return rst
            # 指标名不存在，可以创建指标，且股票组合存在。
            # 3.获取该用户应插入的指标模板号
            selectSql = "SELECT nextval('seq_factorusertemplateid') AS templateno;"
            cur.execute(selectSql)
            templateNo = cur.fetchall()[0][0]
            stemplateNo = templatetype + "_" + str(templateNo)
            # 3. 获取该用户指标组合最大排序编号
            q_maxSeqNo = "select coalesce(max(templateorder),1) from  factorrluser where userId=%s;"
            cur.execute(q_maxSeqNo, [userId])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo) + 1
            createtime=datetime.datetime.now()
            # 4. 插入指标模板
            insertSql = "INSERT INTO factorrluser (userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate,portfolioid,tcreatetime) VALUES(%s, %s, %s,%s,%s,%s,%s,%s,%s,%s);"
            str_desc = "用户自选指标组合模板"
            cur.execute(insertSql, [userId, stemplateNo, templatename, '1', '100', ordNo, str_desc, '1',portfolioId,createtime])
            templateno  =stemplateNo
            msg = ""
            # 获取指标模板中最大排序编号
            q_maxSeqNo = "select coalesce(max(orderno),0) from factortemplate where templateno=%s"
            cur.execute(q_maxSeqNo, [templateno])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo)
            adfs=[]
            factornos.reverse()
            for ord,factorno in enumerate(factornos):
                adfs.append( [str(templateno), factorno, ord+1])
            d_sql = "delete from factortemplate where templateno=%s"
            insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
            cur.execute(d_sql, [str(templateno)])
            cur.executemany(insertSql, adfs)
            con.commit()
            data = {"templateno": templateno}
            rst.errorMsg = msg+"  In addition, Successfully added factor template, template added factors successfully"
            rst.data = data
            return rst
        except:
            msg = "Sorry, addFactorTemplate failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="createTemplateAndFactorsError", errorMsg=msg)
            return rst
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass

    def delFactorSymbol(self, factorno, templateno, userId):
        # 组合删除股票代码
        con, cur = dbutils.getConnect()
        rst = ResultData()
        templateno = str(templateno)
        userId = str(userId)
        factorno = str(factorno)
        try:
            ### 可以检查一下该用户是否有该指标组合，但是应该是有才会调用的
            q_count = "select count(*) from factorrluser where userid=%s and templateno=%s"
            cur.execute(q_count, [userId, templateno])
            fcount_1 = cur.fetchall()[0][0]
            if fcount_1 < 1:
                msg = "Sorry, templateno not found."
                rst.errorData(translateCode="FactorSymbolDelete", errorMsg=msg)
                return rst
            ###end
            # 检查指标是否存在
            q_one = "select count(1) from factortemplate where templateno =%s and factorno=%s;"
            cur.execute(q_one, [templateno, factorno])
            fcount = cur.fetchall()[0][0]
            print(fcount)
            if fcount < 1:
                msg = "factorno is not exists."
                rst.errorData(errorMsg=msg)
                return rst
            cur.execute("BEGIN;")
            insertSql = "delete from factortemplate where templateno=%s and factorno = %s;"
            cur.execute(insertSql, [templateno,factorno])
            rowcount=cur.rowcount
            cur.execute("COMMIT;")
            con.commit()
            rtdata={'rowcount':rowcount}
            rst.data=rtdata
            return rst
        except:
            msg = "Sorry, delete symbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="delFactorSymbolError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getfactorInfo(self,templateno,userId,templatetype='100'):
        #查询用户某个指标组合信息
        # 自选股templatetype='100'
        templateno = str(templateno)
        userId = str(userId)
        q_factor_combination = """select templateno,templatename,userid,templatedesc from factorrluser
            where userid=%s and templateno=%s and templatetype=%s;"""
        qDF = dbutils.getPDQueryByParams(q_factor_combination,params=[userId,templateno,templatetype])
        qDF=tools_utils.dfColumUpper(qDF)
        return qDF

    def getAllFactorTemplate(self,userId,portfolioId,templatetype='100'):
        #查询用户下的所有指标组合信息
        con, cur = dbutils.getConnect()
        rst = ResultData()
        userId = str(userId)
        try:
            q_factor_tem = """select * from factorrluser where userid=%s 
                            and templatetype=%s 
                            and portfolioId=%s order by templateorder desc;"""
            # modify robby 区分自定义与系统模板11.15
            q_factor_tem = """ select *, 'self' as temptype from factorrluser where userid=%s  and  portfolioId=%s  and templatetype=%s
              union  select *, 'sys' as temptype from factorrluser where (userid=%s and userid!=%s)  and templatetype=%s and display='1' order by templateorder desc """
            # print("q_factor_tem:",q_factor_tem)
            # print("q_factor_tem:",[userId,portfolioId,templatetype,tools_utils.globa_default_template_userId,templatetype])
            qDF = pd.read_sql(q_factor_tem,con,params=[userId,portfolioId,templatetype,tools_utils.globa_default_template_userId,userId,templatetype])
            qDF=tools_utils.dfColumUpper(qDF)
            rst.factorTemplateDF=qDF
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getAllFactorTemplateError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def searchPortfolioIdWithSameName(self,templatename,userId):
        # 创建指标组合addFactorTemplate
        userId = str(userId)
        templatename = str(templatename)
        con, cur = dbutils.getConnect()
        rst = ResultData()
        try:
            # 1.检查指标名是否存在
            q_one = "select distinct portfolioid from factorrluser where userid = %s and templatename = %s;"
            tdf = pd.read_sql(q_one, con, params=[userId, templatename])
            print(tdf)
            data_list = tdf['portfolioid'].values.tolist()
            print(data_list)
            data = {"portfolioList": data_list,"msg":"success"}
            rst.data = data
            return rst
        except:
            msg = "Sorry,searchPortfolioIdWithSameName failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="searchPortfolioIdWithSameNameError", errorMsg=msg)
            return rst
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass


    def getTemplateFactors(self,templateno,userId):
        # def getTemplateFactors(self, templateno, userId, portfolioid):
        #获取组合指标代码  组合代码修改
        con, cur = dbutils.getConnect()
        rst = ResultData()
        rst.data=[]
        templateno = str(templateno)
        userId = str(userId)
        try:
            # 检查该用户是否有该指标模版
            q_one = "select userid from factorrluser where templateno= %s "
            tdf=pd.read_sql(q_one,con,params=[templateno])
            if tdf.empty:
                msg = "Sorry, templateno not found."
                rst.errorData(translateCode="getTemplateFactors", errorMsg=msg)
                return rst
            if str(tdf.values.tolist()[0][0])==tools_utils.globa_default_template_userId:
                userId=tools_utils.globa_default_template_userId #默认模版 都可以查询
            q_one="select f.factorno,f.factordesc, f.fview, t.fmtview from factorcell f , factortemplate t where f.factorno =t.factorno " \
                  "and t. templateno=(select templateno from factorrluser where templateno=%s and userid=%s)  order by t.orderno desc "
            qDF = pd.read_sql(q_one, con,params=[templateno,userId])
            # print(qDF[['fview','fmtview']])
            qDF['fview'] = qDF.apply(lambda row:row['fmtview'] if (row['fmtview'] !='' and pd.notnull(row['fmtview'])) else row['fview'] , axis=1)
            qDF.drop(['fmtview'],axis=1, inplace=True)
            qDF.columns=qDF.columns.map(lambda x: x.lower())
            data_list=qDF.to_dict(orient='records')
            rst.data = data_list
            return rst
        except:
            msg = "Sorry, get factorsymbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getTemplateFactorsError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
    def getTemplateFactorsInfo(self,templateno,userId):
        #获取组合股票代码 数据展示
        con, cur = dbutils.getConnect()
        rst = ResultData()
        templateno = str(templateno)
        userId = str(userId)
        rst.TemplateFactorsInfo=[]
        try:
            q_one = "select factorno,orderno from factortemplate where templateno=%s order by orderno desc;"
            cur.execute(q_one, [templateno])
            factorNos = cur.fetchall()
            if len(factorNos)>0:
                factorNos=[x for x in factorNos]
            rst.TemplateFactors =factorNos
            return rst
        except:
            msg = "Sorry, get TemplateFactors failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getTemplateFactorsInfoError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
    def get_file_type(self,filename):
        # 获取文件的后缀名
        file_extension = os.path.splitext(filename)[1]

        # 将后缀名转换为对应的文件类型
        file_types = {
            # 各种类型的 CSV 文件
            '.csv': 'csv',
            '.txt': 'csv',
            '.tsv': 'csv',
            '.dat': 'csv',
            # excel 类型
            '.xls': 'excel',    # Excel 97-2003
            '.xlsx': 'excel',   # Excel 2007及以上

        }
        # 根据后缀名获取文件类型
        file_type = file_types.get(file_extension.lower())

        if file_type is None:
            return 'Unknown'
        else:
            return file_type
    def readFileForSymbols(self,file_path,encoding_type):
        # 1.判断文件是csv还是xlsx
        # cc =pd.read_csv(file_path,encoding=encoding_type,na_filter=False)
        pass
        # 2.读取
        # 3.从stockinfo缓存匹配
    def parseFileForSymbols(self,symbols):
        rst = ResultData()
        symbols = str(symbols)
        try:
            symbols_list = symbols.split('|')
            # 从缓存中处理
            dataset = cache_db.getStockInfoCache()
            # print(dataset.columns)
            rst_list=[]
            windcode_list = dataset['WINDCODE']
            msg = ''
            for symbol in symbols_list:
                temp_1 = symbol.split('.')
                if(len(temp_1)==1):
                    windcode = dataset.loc[dataset['STOCKCODE'] == symbol,'WINDCODE']
                    if(len(windcode)>0):
                        rst_list.append(windcode.values[0])
                    else:
                        msg += str(symbol)+" "
                elif(len(temp_1)==2):
                    if symbol in windcode_list.values:
                        rst_list.append(symbol)
                    else:
                        msg += str(symbol)+" "
                else :
                    msg +=str(symbol)+" "
            if(len(msg)>0):
                msg +=" not matched"
            rst.data = rst_list
            rst.errorMsg= msg
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="parsefileError", errorMsg=msg)
            return rst

    def parseFileForSymbols_moreinfo(self,symbols):
        rst = ResultData()
        symbols = str(symbols)
        try:
            symbols_list = symbols.split('|')
            # 从缓存中处理
            dataset = cache_db.getStockInfoCache()
            rst_list=[]
            msg = ''
            rst_name = []
            for symbol in symbols_list:
                temp_1 = symbol.split('.')
                if(len(temp_1)==1):
                    symbol_name = dataset.loc[dataset['STOCKCODE']==symbol,['WINDCODE','STOCKNAME']]
                    if(len(symbol_name)>0):
                        rst_list.append(symbol_name['WINDCODE'].values[0])
                        rst_name.append(symbol_name['STOCKNAME'].values[0])
                    else:
                        msg += str(symbol)+" "
                elif(len(temp_1)==2):
                    symbol_name = dataset.loc[dataset['WINDCODE'] == symbol, 'STOCKNAME']
                    if (len(symbol_name) > 0):
                        rst_list.append(symbol)
                        rst_name.append(symbol_name.values[0])
                    else:
                        msg += str(symbol)+" "
                else :
                    msg +=str(symbol)+" "
            if(len(msg)>0):
                msg +=" not matched"
            rst.data = {"Symbols":rst_list,"Symbols_name":rst_name}
            rst.errorMsg= msg
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="parsefileError", errorMsg=msg)
            return rst

    def editDefaultFactorName(self,templateNo, factorNoList, factorNameList):
        # 组合修改
        con, cur = dbutils.getConnect()
        rst = ResultData()
        templateNo = str(templateNo)
        rst.data['msg'] = ""
        rst.data['factorname_rowcount'] = 0
        try:
            # 1. 检查是否存在该默认模板
            q_count = f"""select count(*) from factorrluser where userid='{tools_utils.globa_default_template_userId}'  and templatetype='100' and  templateno=%s ;"""
            cur.execute(q_count, [templateNo])
            fcount_1 = cur.fetchall()[0][0]
            if fcount_1 < 1:
                msg = "不存在该默认模板."
                rst.data['msg'] = msg
                rst.errorData(translateCode="DefaultFactorNameedit", errorMsg=msg)
                return rst
            # 2. 查询目前的默认指标
            sql2 = "select factorno, fmtview, orderno from factortemplate where templateno='{}' order by orderno desc;".format(templateNo)
            rsDF0 = pd.read_sql(sql2, con=con)
            fview_list = [str(factor_ids[0]) for factor_ids in rsDF0.values.tolist()]
            # 3. 想要修改的factorno与fmtview对应匹配
            fview_edit_list = factorNoList.split('|')
            fviewfmt_edit_list = factorNameList.split('|')
            if(len(fview_edit_list) != len(fviewfmt_edit_list)):
                msg = "传入的需修改的指标数与指标名数不相等."
                rst.data['msg'] = msg
                rst.errorData(translateCode="DefaultFactorNameedit", errorMsg=msg)
                return rst
            update_list = []
            for i in range(0,len(fview_edit_list)):
                if fview_edit_list[i] in fview_list:
                    update_list.append([fviewfmt_edit_list[i],templateNo,fview_edit_list[i]])
            if len(update_list)==0:
                msg = "传入的需修改的指标均不存在该输入默认模板内."
                rst.data['msg'] = msg
                rst.errorData(translateCode="DefaultFactorNameedit", errorMsg=msg)
                return rst
            # 4. 更新该模板的fmtfiew
            usql = """update factortemplate set fmtview = %s where templateno = %s and factorno = %s;"""
            cur.executemany(usql, update_list)
            factorname_rowcount = cur.rowcount
            con.commit()
            rst.data={"factorname_rowcount":factorname_rowcount, "msg":"修改成功"}
            return rst
        except:
            msg = "Sorry, modify templateName failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="FactorTemplateEditError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass






        #
        # if (templateNo=='8'):
        #     # 默认模板的修改，首先拿到全部是为8的数据，只要调用该接口就创建新模板
        #     if len(templatename)<=0:
        #         templatename = "默认模版-change"
        #     # 1.检查指标名是否存在
        #     q_one = "select count(1) from factorrluser where userid=%s and templatename=%s and portfolioId=%s"
        #     cur.execute(q_one, [userId, templatename, portfolioId])
        #     fcount = cur.fetchall()[0][0]
        #     if fcount > 0:
        #         msg = "This template name is already in use."
        #         rst.errorData(translateCode="editFactorTemplate", errorMsg=msg)
        #         return rst
        #     # 3.获取该用户应插入的指标模板号
        #     selectSql = "SELECT nextval('seq_factorusertemplateid') AS templateno;"
        #     cur.execute(selectSql)
        #     templateNo = cur.fetchall()[0][0]
        #     stemplateNo = "100_" + str(templateNo)
        #     # 3. 获取该用户指标组合最大排序编号
        #     q_maxSeqNo = "select coalesce(max(templateorder),1) from factorrluser where userId=%s;"
        #     cur.execute(q_maxSeqNo, [userId])
        #     ordNo = cur.fetchone()[0]
        #     ordNo = int(ordNo) + 1
        #     create_time = datetime.datetime.now()
        #     # 4. 插入指标模板
        #     insertSql = "INSERT INTO factorrluser (userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate,portfolioid,tcreatetime) VALUES(%s, %s, %s,%s,%s,%s,%s,%s,%s,%s);"
        #     str_desc = "用户自选指标组合模板"
        #     cur.execute(insertSql,
        #                 [userId, stemplateNo, templatename, '1', '100', ordNo, str_desc, '1', portfolioId,create_time])
        #     templateno = stemplateNo
        #     # 获取指标模板中最大排序编号
        #     q_maxSeqNo = "select coalesce(max(orderno),0) from factortemplate where templateno=%s"
        #     cur.execute(q_maxSeqNo, [templateno])
        #     ordNo = cur.fetchone()[0]
        #     ordNo = int(ordNo)
        #     adfs = []
        #     factorNoList.reverse()
        #     factorDescData = cache_db.getFactorCellDF(reload=False)
        #     factornos_cache = factorDescData['FACTORNO']
        #     for ord, factorno in enumerate(factorNoList):
        #         if factorno in factornos_cache.values:
        #             adfs.append([str(templateno), factorno, ord + 1])
        #     if (len(adfs)>0):
        #         insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
        #         cur.executemany(insertSql, adfs)
        #     con.commit()
        #     data = {"templateno": templateno}
        #     con.commit()
        #     rst.data =data
        #     return rst
        # else:
        #     try:
        #         q_count = "select * from factorrluser where userid=%s and templateno=%s and portfolioId=%s"
        #         oneDF=pd.read_sql(q_count,con,params=[userId,templateNo,portfolioId])
        #         oneDF=tools_utils.dfColumUpper(oneDF)
        #         if oneDF.empty:
        #             msg = "Sorry, templateno not found."
        #             rst.errorData(translateCode="FactorTemplateEdit",errorMsg=msg)
        #             return rst
        #         utdt = oneDF.iloc[0]
        #         #update templatename修改指标模板名称
        #         templatename_rowcount=0
        #         if len(templatename)>0:
        #             db_templatename = str(utdt.TEMPLATENAME)
        #             updt = {}
        #             if str(db_templatename)!=templatename:
        #                 updt['templatename']=templatename
        #                 usql="""update factorrluser set templatename=%s where userid=%s and templateno=%s;"""
        #                 cur.execute(usql,[templatename,userId,templateNo])
        #                 templatename_rowcount = cur.rowcount
        #         #update  factorno顺序，删除，新增指标,但不会检查该指标是否存在，默认存在
        #         # 从本地获取指标名列表
        #         search_col = f"select * from factortemplate where templateno='{templateNo}';"
        #         cur.execute(search_col)
        #         local_factorsymbols =cur.fetchall()
        #         local_factorsymbols = [str(row[1]) for row in local_factorsymbols]
        #         # add_factor_list = list(set(factorNoList)-set(local_factorsymbols))
        #         add_factor_list = [value for value in factorNoList if value not in local_factorsymbols]
        #         del_list = list(set(local_factorsymbols)-set(factorNoList))
        #         upfs=[]
        #         adfs=[]
        #         factorNoList.reverse()
        #         # 删除删除的指标
        #         if(len(del_list)>0):
        #             for row in del_list:
        #                 delete_sql= f"delete from factortemplate where templateno=%s and factorno=%s;"
        #                 cur.execute(delete_sql,[str(templateNo),row])
        #         for ord, factorno in enumerate(factorNoList):
        #             if factorno in add_factor_list:
        #                 adfs.append([str(templateNo), factorno, ord+1])
        #             else:
        #                 upfs.append( [ord+1,str(templateNo), factorno])
        #         if(len(upfs)>0):
        #             update_ord = "update factortemplate set orderno=%s where templateno=%s and factorno=%s"
        #             cur.executemany(update_ord,upfs)
        #         if(len(adfs)>0):
        #             factorDescData = cache_db.getFactorCellDF(reload=False)
        #             factornos_cache = factorDescData['FACTORNO']
        #             insertSql = "INSERT INTO factortemplate (templateno, factorno, orderno) VALUES(%s,%s,%s);"
        #             for adfs_i in adfs:
        #                 if adfs_i[1] in factornos_cache.values:
        #                     cur.execute(insertSql, adfs_i)
        #         factor_rowcount = cur.rowcount
        #         con.commit()
        #         rst.data={"templatename_rowcount":templatename_rowcount,"factor_rowcount":factor_rowcount,}
        #         return rst
        #     except:
        #         msg = "Sorry, modify templateName failed. Please try again later."
        #         Logger.errLineNo(msg=msg)
        #         rst.errorData(translateCode="FactorTemplateEditError",errorMsg=msg)
        #         return rst
        #     finally:
        #         try:cur.close()
        #         except:pass
        #         try:con.close()
        #         except:pass



if __name__ == '__main__':
    cu =cfactorMode()
    # a=cu.getTemplateFactors('8', '051')
    # print(a.toDict())
    # time.sleep(10000)
    method = "test_get"
    method=2543535
    print(tools_utils.globa_default_template_userId)
    # rst = cu.editDefaultFactorName('10','28723|28760|63','change_name28723|change_name28760|kk')
    # rst = cu.editDefaultFactorName('10','','')

    # print(rst.toDict())
    # if method=="test_1":
    #     a=  cu.parseFileForSymbols("600519|0700.HK|AAPL|NVDA.O|scsc|234fdscs")
    #     print(a.toDict())
    #     a = cu.parseFileForSymbols_moreinfo("600519|0700.HK|AAPL|NVDA.O|scsc|234fdscs")
    #     print(a.toDict())
    # elif method=="test_file":
    #     file_path_1 = "C:/Users/jsr_23/Desktop/uplod(1).csv"
    #     a = cu.get_file_type(file_path_1)
    #     print(a)
    #     print(type(a))
    # elif method=="test_get":
    #     a = cu.getTemplateFactors('100_224','73')
    #     print(a.toDict())
    # elif method=="test_copyFactorTemplate":
    #     userid ="2000"
    #     templateno="100_120"
    #     portfolid = ['self_61','self_87','self_62','self_60','self_59','self_87']
    #     portfolid_str = '|'.join(portfolid)
    #     r=cu.copyFactorTemplate(portfolid_str,templateno,userid)
    #     print(r.toDict())
    # elif method==2:
    #     templateno =tools_utils.globa_default_template_no
    #     portfolid = ['self_102', 'self_103', 'self_104', 'self_60', 'self_59', 'self_87']
    #     portfolid_str = '|'.join(portfolid)
    #     r = cu.copyFactorTemplatewithNameChange(portfolid_str, templateno, "73",templatename="change_for_time1215")
    #     print(r.toDict())
    # elif method=="file":
    #     file_path_1 = "C:/Users/jsr_23/Desktop/uplod(1).csv"
    #     # cu.print_excel("C:/Users/jsr_23/Desktop/uplod(1).csv")
    #     cu.readFileForSymbols(file_path_1,"utf-8")
    # elif method=="1208":
    #     ss = "28826|53|28812|49|28788|67|90|28844|28843|28842|28841|32|28862|28743|28769"
    #     ss = ss.split('|')
    #     cu.editFactorTemplate("self_125","100_224","",'73',ss)
    #
    # else:
    #     pass
    tuserid="73"
    # rst = cu.getTemplateFactors("100_185",tuserid)
    rst = cu.searchPortfolioIdWithSameName( "1234567","73")
    print(rst.toDict())
    # # 1. 创建指标组合，返回ResultData 里面有templateno的字典
    # template_name = "指标组合1215_2"
    # rst =cu.addFactorTemplate(template_name, "73", '100')
    # print("增加了指标组合：",rst.toDict())
    # if(rst.data.get('templateno') !=None):
    #     templateId=rst.data['templateno']
    #     #为其增加指标
    #     templateId = "100_"+str(templateId)
    #     print("指标组合id：", templateId)
    #     # 2. 修改指标组合的名称，为“change_name_i"
    #     change_str ="change"+template_name
    #     rst_1 = cu.editFactorTemplate(templateId,change_str,tuserid)
    #     print("修改了指标组合的名字",rst_1.toDict())
    #     # 3.查看指标组合的信息
    #     check_name = cu.getfactorInfo(templateId,tuserid)
    #     print(check_name)
    #     # 4. 为指标组合增加指标
    #     add_factor_list = ['28699','28822','28843','28788','28812','28762','28769']
    #     for factor_id in add_factor_list:
    #         s = cu.addFactorSymbol(factor_id,templateId,tuserid)
    #         print("增加了指标：",s.toDict())
    #     # 5.删除单个的指标
    #     s = cu.delFactorSymbol(add_factor_list[0],templateId,tuserid)
    #     print(f"删除了指标{add_factor_list[0]} :",s.data)
    #     # # 6. 删除之前的插入的多余的指标组合()
    #     delete_list = ['100_3','100_26','100_27']
    #     for delete_id in delete_list:
    #         s = cu.delFactorTemplate(delete_id,tuserid)
    #         print(s.data)
    #     # 7. 查询用户下的所有指标组合信息
    #     s = cu.getAllFactorTemplate(tuserid)
    #     print(s.factorTemplateDF)
    #     # 8. 查询新插的指标组合的全部指标信息
    #     s = cu.getTemplateFactors(templateId,tuserid)
    #     print(s.factorNos)
    #     # 9.查询指标详情（没什么用）
    #     s = cu.getTemplateFactorsInfo(templateId,tuserid)
    #     print(s.TemplateFactors)
