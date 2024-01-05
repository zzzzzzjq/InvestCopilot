#同步财务报告数据

import datetime
import sys
import traceback

sys.path.append('../../../..')
import  InvestCopilot_App.models.sync.reuters.syncFactors as factor_utls

def syncHk():
    try:
        fu = factor_utls.fmtutils()
        vtradeDate = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime("%Y%m%d")
        print("vtradeDate:", vtradeDate)
        bt1=datetime.datetime.now()
        fu.sync_financial_report(vtradeDate,period='year',area='hk')#年度数据
        bt2=datetime.datetime.now()
        print("year:%s(s)"%(bt2-bt1).total_seconds())
        fu.sync_financial_report(vtradeDate,period='quarter',area='hk')#季度数据
        bt3=datetime.datetime.now()
        print("year:%s(s)"%(bt2-bt1).total_seconds())
        print("quarter:%s(s)"%(bt3-bt2).total_seconds())
    except:
        print(traceback.format_exc())

def syncAM():
    try:
        fu = factor_utls.fmtutils()
        vtradeDate = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime("%Y%m%d")
        print("vtradeDate:", vtradeDate)
        bt1=datetime.datetime.now()
        fu.sync_financial_report(vtradeDate,period='year',area='us')#年度数据
        bt2=datetime.datetime.now()
        print("year:%s(s)"%(bt2-bt1).total_seconds())
        fu.sync_financial_report(vtradeDate,period='quarter',area='us')#季度数据
        bt3=datetime.datetime.now()
        print("year:%s(s)"%(bt2-bt1).total_seconds())
        print("quarter:%s(s)"%(bt3-bt2).total_seconds())
    except:
        print(traceback.format_exc())
if __name__ == '__main__':
    syncAM()
    syncHk()
    print("end...")