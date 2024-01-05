import datetime
import os
import socket
import time
import socket

import numpy as  np
import requests
import traceback
import re
import pandas as pd
import sys

sys.path.append('../../../..')

import  InvestCopilot_App.models.toolsutils.dbmongo as  dbmongo
import  InvestCopilot_App.models.toolsutils.dbutils as  dbutils
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils

isAly=False
if socket.gethostname()=="iZebwatodogxv8Z":
    isAly = True

def fmt_df_column_upper(df):
    df.columns=[str(x).upper() for x in df.columns]
    return df

class relativevaluationutils():

    def make_valid_filename(self, filename):
        import re
        # 替换无效字符为下划线
        valid_filename = re.sub(r'[^\w\s-]', '', filename.strip())
        # 删除连续的空格
        valid_filename = re.sub(r'\s+', '', valid_filename)
        # 替换空格为下划线
        valid_filename = valid_filename.strip().replace(' ', '')
        valid_filename = valid_filename.replace('-', '')
        return valid_filename


    def sync_reuters_insider (self):
        fdname = (datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")
        #同步财务报告
        sctx=r"Y:\insider\%s"%(fdname)
        pfs = os.listdir(sctx)
        cp_pfs=[str(x).strip() for x in pfs]
        con,cur = dbutils.getConnect()
        isql="""
        INSERT INTO newdata.reuters_insider 
        (rowid,windcode, stockcode, ric, InsiderFullName, InsiderTitle, InsiderSharesTradedAdjusted, InsiderTransactionPrice, InsiderTransactionTypeShort, tradedate) 
        VALUES(%s,%s,%s,%s, %s, %s, %s, %s, %s, %s)"""
        qcodes = "select * from config.reuters_usstocks u where  windcode is not null"
        # dsql="delete from newdata.reuters_insider where rowid in (%s)"
        codeDF=pd.read_sql(qcodes,con)
        codeDF=fmt_df_column_upper(codeDF)
        for cdt in codeDF.itertuples():
            ric = str(cdt.RIC)
            windCode = cdt.WINDCODE
            stockCode = cdt.TICKER
            fname  = str(ric).replace(".","_")+".xlsx"
            print("fname:",fname)
            isfindfile=False
            if fname in cp_pfs:
                isfindfile = True
            if not isfindfile:
                if str(ric).endswith(".O"):
                    fname = str(ric).replace(".O",".OQ").replace(".", "_") + ".xlsx"
            if fname in cp_pfs:
                fileidx=cp_pfs.index(fname)
                xlsx_filename=os.path.join(sctx,pfs[fileidx])
                print("xlsx_filename:",xlsx_filename)
                ddf=pd.read_excel(xlsx_filename)
                ddf.replace([np.inf, -np.inf],np.NAN, inplace=True)
                ddf=ddf.replace({np.NAN:None})
                reportIds = ddf.columns.values.tolist()
                reportIds=[str(x).replace("(","").replace(")","").replace(" ","") for x in reportIds]
                ddf.columns=reportIds
                if "Instrument" not in reportIds:
                    continue
                #['Instrument', 'InsiderFullName', 'InsiderTitle', 'InsiderTransactionDate', 'InsiderSharesTradedAdjusted', 'InsiderTransactionPrice', 'InsiderTransactionTypeShort']
                # print("reportIds:",reportIds)
                add_dt={}
                for _rid,its in ddf.iterrows():
                    if pd.isnull(its.InsiderTransactionDate):
                        continue
                    if pd.isnull(its.InsiderSharesTradedAdjusted):
                        continue
                    rows=its.values.tolist()
                    ustr=",".join([str(x) for x in rows])
                    uuid=tools_utils.md5(ustr)
                    ndt=[uuid,windCode,stockCode,ric,its.InsiderFullName,its.InsiderTitle,its.InsiderSharesTradedAdjusted,its.InsiderTransactionPrice,
                         its.InsiderTransactionTypeShort,str(its.InsiderTransactionDate).replace("-","")]
                    add_dt[uuid]=ndt
                    print("ndt:",ndt)
                #排查历史数据
                exDF=pd.read_sql("select rowid from newdata.reuters_insider where rowid in (%s)"%dbutils.getQueryInParam(list(add_dt.keys())),con)
                exists_rowids= exDF['rowid'].values.tolist()
                ndts=[]
                for _uuid,vdt in add_dt.items():
                    if _uuid in exists_rowids:
                        continue
                    ndts.append(vdt)
                cur.executemany(isql,ndts)
                print("add num:",windCode,cur.rowcount)
                con.commit()
        con.commit()
        cur.close()
        con.close()

    def modifyLastDay(self,vtradedate):
        factortypes = ["relativevaluation",]
        con, cur = dbutils.getConnect()
        for cptable in factortypes:
            tablename = "newdata.a_reuters_%s" % cptable
            up_date="update %s set tradedate='%s' "%(tablename,vtradedate)
            cur.execute(up_date)
            con.commit()
        cur.close()
        con.close()

    def copyNewToHis(self, vtradedate):
        factortypes = ["relativevaluation"]
        con, cur = dbutils.getConnect()
        for cptable in factortypes:
            #qcolumns
            table_name = "a_reuters_%s" % cptable
            new_tablename = "newdata.a_reuters_%s" % cptable
            his_tablename = "newdata.a_reuters_%s_his" % cptable
            # print("table_name:",table_name)
            q_columns=" SELECT column_name FROM information_schema.columns WHERE table_name = %s and table_schema ='newdata'"
            # print("q_columns:",q_columns)
            columnsDF=pd.read_sql(q_columns,con,params=[table_name])
            columnsDF=fmt_df_column_upper(columnsDF)
            tbcolumns=columnsDF['COLUMN_NAME'].values.tolist()
            # print("tbcolumns:",tbcolumns)
            stbcolumns=",".join(tbcolumns)
            ccolumns=stbcolumns.replace("tradedate","'%s'"%vtradedate)
            # d_sql = "delete from newdata.t_reuters_%s where tradedate='%s'" % (cptable, vtradedate)
            cp_sql = "insert into {his_tablename} ({icolumns}) select {ccolumns} from  {new_tablename} " \
                .format(**{"his_tablename":his_tablename,"new_tablename":new_tablename,"icolumns":stbcolumns,
                           "ccolumns":ccolumns})
            cp_d="delete from %s  where tradedate='%s'"%(his_tablename,vtradedate)
            print("cp_d:",cp_d)
            print("cp_sql:",cp_sql)
            cur.execute(cp_d)
            print("del his_tablename:%s,num:%s"%(his_tablename,cur.rowcount))
            cur.execute(cp_sql)
            print("copy new_tablename:%s to his_tablename:%s, num:%s"%(new_tablename,his_tablename,cur.rowcount))
            con.commit()
        cur.close()
        con.close()

if __name__ == '__main__':
    fu = relativevaluationutils()
    # fu.test_()
    vtradeDate = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime("%Y%m%d")
    print("vtradeDate:", vtradeDate)
    bt1=datetime.datetime.now()
    # fu.modifyLastDay(vtradeDate)  # 更新最新表数据日期
    bt2=datetime.datetime.now()
    print("modifyLastDay:%s(s)"%(bt2-bt1).total_seconds())
    fu.sync_reuters_insider()
    # fu.copyNewToHis(vtradeDate)