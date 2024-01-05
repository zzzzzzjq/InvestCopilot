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

    def newfactorsdataparserByArea(self, vtradeDate):
        fdname = (datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")
        source_path=r"Y:\relative_valuation_factors\%s\all_data.xlsx"%(fdname)
        print("source_path:",source_path)
        if not os.path.exists(source_path):
            return
        basicDF = pd.read_excel(source_path)
        basicDF.replace([np.inf, -np.inf],np.NAN, inplace=True)
        basicDF=basicDF.replace({np.NAN:None})
        # ['ric', 'PE_NTM', 'evebitda_NTM', 'evsales_NTM', 'pcf_NTM', 'pb_NTM', 'dividend_yield_NTM', 'PE_TTM', 'evebitda_TTM', 'evsales_TTM', 'pcf_TTM', 'pb_TTM', 'dividend_yield_TTM', 'change1D', 'totalreturn1M', 'PE_NTM1', 'PS_NTM', 'PriceCashFlowPerShare_NTM', 'EVREV_NTM', 'EVEBITDA_NTM', 'EPSGrowth', 'ROE']
        # basicDF.replace([np.inf, -np.inf,np.NaN],None, inplace=True)
        # basicDF.replace([np.NaN], None, inplace=True)
        vcolumns = basicDF.columns.values.tolist()
        ncolumns=[self.make_valid_filename(str(x).replace("TR.","").replace("*100","").replace("Period","").replace("=","").strip()) for x in vcolumns]
        basicDF.columns=ncolumns
        print("ncolumns:",ncolumns)
        tablecolumns= ['ric', 'PE_NTM_RV', 'evebitda_NTM_RV', 'evsales_NTM_RV', 'pcf_NTM_RV', 'pb_NTM_RV', 'dividend_yield_NTM_RV', 'PE_TTM_RV', 'evebitda_TTM_RV', 'evsales_TTM_RV', 'pcf_TTM_RV', 'pb_TTM_RV', 'dividend_yield_TTM_RV', 'change1D', 'totalreturn1M', 'pe_NTM', 'ps_NTM', 'PriceCashFlowPerShare_NTM', 'evrev_NTM', 'evebitada_NTM', 'epsGrowth', 'roe']
        column_dt=dict(zip(ncolumns,tablecolumns))
        q_data="select  id,windcode,ric,ticker   from config.reuters_usstocks f where windcode is not null "
        con, cur = dbutils.getConnect()
        ricDF=pd.read_sql(q_data,con)
        ricDF=fmt_df_column_upper(ricDF)
        ric_dt ={r.RIC : dict(r) for i,r in ricDF.iterrows()}
        # fillTables=["newdata.a_reuters_basic"]
        ntablename=["windcode","stockcode"]
        tableName=" newdata.a_reuters_relativevaluation "
        for tcname in tablecolumns:
            ntablename.append(tcname)
        ntablename.append("tradedate")
        basesql = "insert into %s (%s) values(%s)" % (tableName, ",".join(ntablename), ",".join(["%s"] * len(ntablename)))
        grabDF = basicDF[ncolumns]
        add_rows=[]
        # print("ntablename:",tableName,ntablename)
        for _i,r in grabDF.iterrows():
            atr = r.values.tolist()
            ric=atr[0]
            if ric in ric_dt:
                # print("ric_dt[ric]:",ric_dt[ric])
                windcode=ric_dt[ric]['WINDCODE']
                stockCode=ric_dt[ric]['TICKER']
                add_rows.append([windcode,stockCode]+atr+[vtradeDate])
        if len(add_rows) > 0:
            d_sql="delete from %s where tradedate='%s' " % (tableName, vtradeDate)
            cur.execute(d_sql)
            # print("d_sql:", d_sql)
            # print("del %s rowcount:%s" %(tableName, cur.rowcount))
            # print("basesql:",basesql)
            cur.executemany(basesql, add_rows)
            print("add %s  rowcount:%s" %(tableName, cur.rowcount))
                # wx_send.send_wx_msg_operation("%s add %s (条)[%s]" % (ntablename, cur.rowcount, vtradeDate))
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
    fu.modifyLastDay(vtradeDate)  # 更新最新表数据日期
    bt2=datetime.datetime.now()
    print("modifyLastDay:%s(s)"%(bt2-bt1).total_seconds())
    fu.newfactorsdataparserByArea(vtradeDate)
    fu.copyNewToHis(vtradeDate)