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

class fmtutils():
    def arm(self, tickerId, itemName):
        rtfdt = {}
        fp = os.path.join(r"C:\mys\ref_data\starmine\arm\%s.xlsx" % tickerId)
        if os.path.exists(fp):
            fdt = pd.read_excel(fp, index_col=False)
            tfdt = fdt[fdt['ITEMNAME'] == itemName]
            tfdt = tfdt.sort_values(['STARTDATE'], ascending=False)
            if len(tfdt) > 0:
                rtfdt = dict(tfdt.iloc[0])
                rtfdt.pop("Unnamed: 0")
                print("tfdt:", rtfdt)
        return rtfdt

    def arm2(self, tickerId, itemName):
        rtfdt = {}
        fp = os.path.join(r"C:\mys\ref_data\starmine\arm\%s.xlsx" % tickerId)
        if os.path.exists(fp):
            fdt = pd.read_excel(fp, index_col=False)
            tfdt = fdt[fdt['ITEMNAME'] == itemName]
            tfdt = tfdt.sort_values(['STARTDATE'], ascending=False)
            if len(tfdt) > 0:
                rtfdt = dict(tfdt.iloc[0])
                rtfdt.pop("Unnamed: 0")
                print("tfdt:", rtfdt)
        return rtfdt

    def eq(self, tickerId, itemName):
        fp = os.path.join(r"C:\mys\ref_data\starmine\eq\%s.xlsx" % tickerId)
        rtfdt = {}
        if os.path.exists(fp):
            fdt = pd.read_excel(fp, index_col=False)
            tfdt = fdt[fdt['ITEMNAME'] == itemName]
            tfdt = tfdt.sort_values(['STARTDATE'], ascending=False)
            if len(tfdt) > 0:
                rtfdt = dict(tfdt.iloc[0])
                rtfdt.pop("Unnamed: 0")
                print("tfdt:", rtfdt)
        return rtfdt

    def vi(self, tickerId, itemName):
        fp = os.path.join(r"C:\mys\ref_data\starmine\iv\%s.xlsx" % tickerId)
        rtfdt = {}
        if os.path.exists(fp):
            fdt = pd.read_excel(fp, index_col=False)
            tfdt = fdt[fdt['ITEMNAME'] == itemName]
            tfdt = tfdt.sort_values(['STARTDATE'], ascending=False)
            if len(tfdt) > 0:
                rtfdt = dict(tfdt.iloc[0])
                rtfdt.pop("Unnamed: 0")
                print("tfdt:", rtfdt)
        return rtfdt

    def val(self, tickerId, itemName):
        fp = os.path.join(r"C:\mys\ref_data\starmine\val\%s.xlsx" % tickerId)
        rtfdt = {}
        if os.path.exists(fp):
            fdt = pd.read_excel(fp, index_col=False)
            tfdt = fdt[fdt['ITEMNAME'] == itemName]
            tfdt = tfdt.sort_values(['STARTDATE'], ascending=False)
            if len(tfdt) > 0:
                rtfdt = dict(tfdt.iloc[0])
                rtfdt.pop("Unnamed: 0")
                print("tfdt:", rtfdt)
        return rtfdt

    def getStocks(self):
        fp = os.path.join(r"C:\mys\id_tickers.xlsx")
        spd = pd.read_excel(fp, index_col=False)
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["reutersDBCodes"]
            rows = []
            for i, r in spd.iterrows():
                r = dict(r)
                r.pop("Unnamed: 0")
                # print(r)
                # if (i > 10): break
                rows.append(r)
                # if reutersSet.find_one({"Id":r['Id']}) is None:
                #     reutersSet.insert_one(r)
            reutersSet.insert_many((rows))

    def getReutersDBStock(self, windCodes=[]):
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["reutersDBCodes"]
            rts = reutersSet.find({"windCode": {"$in": windCodes}})
            return {r['windCode']: r for r in rts}

    def compTicker(self):

        # 所有美股
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        print("amCodes:", amCodes)

        amDBCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["reutersDBCodes"]
            dbStocks = reutersSet.find({"dataFlag": "1"})
            for s in dbStocks:
                amDBCodes[s['Ticker']] = s

        rdbu = {}
        for ticker, tv in amCodes.items():
            if ticker in amDBCodes:
                dbt = amDBCodes[ticker]
                # if dbt['ExchCode'] in ['NYSE','NASD','AMEX']:
                rdbu[ticker] = {"Id": amDBCodes[ticker]['Id'], "windCode": tv['windCode'], 'symbol': tv['symbol']}

        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["reutersDBCodes"]
            for r, v in rdbu.items():
                # print("r:", r, v)
                # fnc = reutersSet.find({'Ticker': r})
                # tickers=[f for f in fnc]
                # if len(tickers)>1:
                #     print("tickers:",tickers)
                #     continue
                # print("up:",v)
                rts = reutersSet.update_one({'Id': v["Id"]},
                                            {"$set": {'windCode': v['windCode'], 'symbol': v['symbol'],
                                                      'compFlag': "1"}})
                print(rts.modified_count)

    def getAlyStockPool(self, trackFlag="1"):
        fsymbols = []
        trackSymbols = []
        try:
            data = {'doMethod': "getStockPool"}
            if trackFlag is not None:
                data['trackFlag'] = trackFlag
            headers = {"Accept": "application/json, text/plain, */*",
                       "Accept-Encoding": "gzip, deflate, br",
                       "Accept-Language": "zh-CN,zh;q=0.9",
                       "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36", }
            resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/", data=data, headers=headers,
                                 timeout=(10, 30))
            if resp.status_code == 200:
                rtjs = resp.json()
                if rtjs['errorFlag']:
                    if trackFlag is not None:
                        return rtjs['fsymbols'], rtjs['trackCodes'],
                    return rtjs['fsymbols'], trackSymbols
                return fsymbols, trackSymbols
            else:
                pass
            return fsymbols, trackSymbols
        except:
            print("getAlyStockPool error")
            print(traceback.format_exc())
            pass

        return fsymbols, trackSymbols

    def parserXlsxData(self):
        stockPools, tradeCode = self.getAlyStockPool()

        reuterCodes = self.getReutersDBStock(stockPools)
        counts = len(stockPools)
        for _idx, windCode in enumerate(stockPools):
            print("_idx:%s,windCode:%s" % (counts - _idx, windCode))
            if windCode in reuterCodes:
                rdt = reuterCodes[windCode]
                tickerId = rdt['Id']
                print(reuterCodes[windCode])

                armdt = fu.arm(tickerId, "ARM Country Rank")
                armdata = {}
                if len(armdt) > 0:
                    armdata = {"windCode": windCode}
                    armdata.update(armdt)
                eqdt = fu.eq(tickerId, "Earnings Quality Country Rank Current")
                eqdata = {}
                if len(eqdt) > 0:
                    eqdata = {"windCode": windCode}
                    eqdata.update(eqdt)
                vi1dt = fu.vi(tickerId, "IV Forward 10Year EPS CAGR")
                vi1data = {}
                if len(vi1dt) > 0:
                    vi1data = {"windCode": windCode}
                    vi1data.update(vi1dt)
                vi2dt = fu.vi(tickerId, "Price / Intrinsic Value Country Rank")
                vi2data = {}
                if len(vi2dt) > 0:
                    vi2data = {"windCode": windCode}
                    vi2data.update(vi2dt)
                valdt = fu.val(tickerId, "Price Mo Country Rank")
                valdata = {}
                if len(valdt) > 0:
                    valdata = {"windCode": windCode}
                    valdata.update(valdt)

                print("armdata:", armdata)
                print("eqdata:", eqdata)
                print("vi1data:", vi1data)
                print("vi2data:", vi2data)
                print("valdata:", valdata)

                with Mongo("starmine") as md:
                    mydb = md.db
                    if len(armdata) > 0:
                        if pd.isnull(armdata['ENDDATE']):
                            armdata['ENDDATE'] = ""
                        if not pd.isnull(armdata['STARTDATE']):
                            armdata['STARTDATE'] = (armdata['STARTDATE']).strftime("%Y-%m-%d %H:%M:%S")
                            print(armdata['STARTDATE'], "pd.to_datetime(armdata['STARTDATE'])", armdata)
                        armdata['ID'] = str(armdata["ID"])
                        armdata['ITEM'] = int(armdata["ITEM"])
                        armdata['SECCODE'] = int(armdata["SECCODE"])
                        armdata['STARMINE_SECID'] = int(armdata["STARMINE_SECID"])
                        armdata['RANK_'] = float(armdata["RANK_"])
                        armSet = mydb["arm"]
                        armSet.insert_one(armdata)

                    if len(eqdata) > 0:
                        if pd.isnull(eqdata['ENDDATE']):
                            eqdata['ENDDATE'] = ""
                        if not pd.isnull(eqdata['STARTDATE']):
                            eqdata['STARTDATE'] = (eqdata['STARTDATE']).strftime("%Y-%m-%d %H:%M:%S")
                            print(eqdata['STARTDATE'], "pd.to_datetime(eqdata['STARTDATE'])", eqdata)
                        eqdata['ID'] = str(eqdata["ID"])
                        eqdata['ITEM'] = int(eqdata["ITEM"])
                        eqdata['SECCODE'] = int(eqdata["SECCODE"])
                        eqdata['STARMINE_SECID'] = int(eqdata["STARMINE_SECID"])
                        eqdata['RANK_'] = float(eqdata["RANK_"])
                        eqSet = mydb["eq"]
                        eqSet.insert_one(eqdata)

                    if len(vi1data) > 0:
                        if pd.isnull(vi1data['ENDDATE']):
                            vi1data['ENDDATE'] = ""
                        if not pd.isnull(vi1data['STARTDATE']):
                            vi1data['STARTDATE'] = (vi1data['STARTDATE']).strftime("%Y-%m-%d %H:%M:%S")
                            print(vi1data['STARTDATE'], "pd.to_datetime(vi1data['STARTDATE'])", vi1data)
                        vi1data['ID'] = str(vi1data["ID"])
                        vi1data['ITEM'] = int(vi1data["ITEM"])
                        vi1data['SECCODE'] = int(vi1data["SECCODE"])
                        vi1data['STARMINE_SECID'] = int(vi1data["STARMINE_SECID"])
                        vi1data['RANK_'] = float(vi1data["RANK_"])
                        viSet = mydb["vi"]
                        viSet.insert_one(vi1data)
                    if len(vi2data) > 0:
                        if pd.isnull(vi2data['ENDDATE']):
                            vi2data['ENDDATE'] = ""
                        if not pd.isnull(vi2data['STARTDATE']):
                            vi2data['STARTDATE'] = (vi2data['STARTDATE']).strftime("%Y-%m-%d %H:%M:%S")
                            print(vi2data['STARTDATE'], "pd.to_datetime(vi2data['STARTDATE'])", vi2data)
                        vi2data['ID'] = str(vi2data["ID"])
                        vi2data['ITEM'] = int(vi2data["ITEM"])
                        vi2data['SECCODE'] = int(vi2data["SECCODE"])
                        vi2data['STARMINE_SECID'] = int(vi2data["STARMINE_SECID"])
                        vi2data['RANK_'] = float(vi2data["RANK_"])
                        viSet = mydb["vi"]
                        viSet.insert_one(vi2data)

                    if len(valdata) > 0:
                        if pd.isnull(valdata['ENDDATE']):
                            valdata['ENDDATE'] = ""
                        if not pd.isnull(valdata['STARTDATE']):
                            valdata['STARTDATE'] = (valdata['STARTDATE']).strftime("%Y-%m-%d %H:%M:%S")
                            print(valdata['STARTDATE'], "pd.to_datetime(valdata['STARTDATE'])", valdata)
                        valdata['ID'] = str(valdata["ID"])
                        valdata['ITEM'] = int(valdata["ITEM"])
                        valdata['SECCODE'] = int(valdata["SECCODE"])
                        valdata['STARMINE_SECID'] = int(valdata["STARMINE_SECID"])
                        valdata['RANK_'] = float(valdata["RANK_"])
                        valSet = mydb["val"]
                        valSet.insert_one(valdata)

    def findTicker(self):
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["reutersDBCodes"]
            rdatas = reutersSet.find({})
            for d in rdatas:
                Id = d['Id']
                fp = os.path.join(r"C:\mys\ref_data\starmine\eq\%s.xlsx" % Id)
                if os.path.exists(fp):
                    reutersSet.update_one({"Id": Id}, {"$set": {"dataFlag": "1"}})

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

    def newfactors(self, sheet_name):
        # D:\demand\mys\starmine
        # basicDF = pd.read_excel(r"D:\demand\mys\stock_pool_v3_values.xlsx",sheet_name=sheet_name)
        basicDF = pd.read_excel(r"D:\demand\mys\starmine\stock_pool_v3.xlsx", sheet_name=sheet_name)
        vtradeDate = datetime.datetime.now().strftime("%Y%m%d")
        # 所有美股
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        # print("amCodes:", amCodes)
        bcols = basicDF.columns.tolist()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        basicDF.columns = bcols
        basicDF = basicDF.drop([bcols[2]], axis=1)
        print("bcols:", basicDF.columns)
        bcols = basicDF.columns.tolist()
        ntablename = "newdata.reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockcode", "RIC"]
        for bc in bcols:
            if bc in ['name', "RIC", 'Ticker_Symbol', 'nan']:
                continue
            ncols.append(bc)
            acs.append("%s numeric(22,6)" % (bc))
        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")
        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        print(ctables)
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
        print("bcols:", bcols)
        ric_dt = {}
        for r in basicDF.itertuples():
            Ticker_Symbol = str(r.Ticker_Symbol)
            symbol = Ticker_Symbol.replace(".", "_")
            if symbol in amCodes:
                # print("amCodes:",amCodes[symbol])
                ric_dt[r.RIC] = amCodes[symbol]['windCode']
            else:
                # print("not find....", Ticker_Symbol, r)
                pass
            pass
        # ric_dt
        basic_rows = []
        for r in basicDF.itertuples():
            RIC = str(r.RIC)
            if RIC in ric_dt:
                windCode = ric_dt[RIC]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                Price_Close = float(r.Price_Close)
                Volume = float(r.Volume)
                Average_Daily_Volume___10_Days = float(r.Average_Daily_Volume___10_Days)
                basic_rows.append(
                    [windCode, str(windCode).split(".")[0], RIC, Price_Close, Volume, Average_Daily_Volume___10_Days,
                     vtradeDate])
        # print("basesql:", basesql)
        if len(basic_rows) > 0:
            con, cur = getConnect()
            # cur.execute("delete from %s "%ntablename)
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            cur.executemany(basesql, basic_rows)
            print("rowcount cur:", cur.rowcount)
            con.commit()
            cur.close()
            con.close()

    def newfactors2(self, sheet_name):
        xlsxpath = r"D:\demand\mys\starmine\stock_pool_v3.xlsx"
        basicDF = pd.read_excel(xlsxpath, sheet_name="basic")
        vtradeDate = datetime.datetime.now().strftime("%Y%m%d")
        # 所有美股
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        # print("amCodes:", amCodes)

        bcols = basicDF.columns.tolist()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        basicDF.columns = bcols
        basicDF = basicDF.drop([bcols[2]], axis=1)
        print("bcols:", basicDF.columns)
        ric_dt = {}
        for r in basicDF.itertuples():
            Ticker_Symbol = str(r.Ticker_Symbol)
            symbol = Ticker_Symbol.replace(".", "_")
            if symbol in amCodes:
                # print("amCodes:",amCodes[symbol])
                ric_dt[r.RIC] = amCodes[symbol]['windCode']
            else:
                print("not find....", Ticker_Symbol, r)
            pass
        # ric_dt
        sheetDF = pd.read_excel(xlsxpath, sheet_name=sheet_name)
        bcols = sheetDF.columns.tolist()
        fixs = sheet_name[0]
        bcols = [fixs + self.make_valid_filename(str(x)) for x in bcols]
        sheetDF.columns = bcols
        print("sheetDF bcols:", bcols)

        sheetDF = sheetDF.drop([bcols[0]], axis=1)
        print("bcols:", sheetDF.columns)
        bcols = sheetDF.columns.tolist()

        ntablename = "newdata.reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockcode", "RIC"]
        dcols = []
        for bc in bcols[1:]:
            if bc in [fixs + 'name', fixs + "RIC", fixs + 'Ticker_Symbol', fixs + 'nan']:
                continue
            dcols.append(bc)
            ncols.append(bc)
            acs.append("%s numeric(22,6)" % (bc))
        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")
        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        print(ctables)
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))

        basic_rows = []
        for i, r in sheetDF.iterrows():
            ndr = dict(r)
            RIC = str(ndr[bcols[0]])
            if RIC in ric_dt:
                windCode = ric_dt[RIC]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                drw = [windCode, str(windCode).split(".")[0], RIC]
                for gd in dcols:
                    gdvalue = float(ndr[gd])
                    drw.append(gdvalue)
                drw.append(vtradeDate)
                basic_rows.append(drw)
        # print("basesql:", basesql)
        print(sheet_name + ":", len(basic_rows))
        if len(basic_rows) > 0:
            con, cur = getConnect()
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            cur.executemany(basesql, basic_rows)
            print("%s rowcount:%s", cur.rowcount  %(ntablename, cur.rowcount))
            con.commit()
            cur.close()
            con.close()

    def newfactors_new(self, sheet_name, vtradeDate):
        basicDF = pd.read_excel(r"D:\demand\mys\starmine\output\%s.xlsx" % sheet_name)
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        # print("amCodes:", amCodes)
        bcols = basicDF.columns.tolist()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        basicDF.columns = bcols
        print("bcols:", basicDF.columns)
        ntablename = "newdata.reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockCode", "RIC"]
        for bc in bcols:
            if bc in ['name', "Instrument", 'Ticker_Symbol', 'nan']:
                continue
            ncols.append(bc)
            acs.append("%s numeric(22,6)" % (bc))
        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")
        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        print(ctables)
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
        print("bcols:", bcols)
        ric_dt = {}
        for r in basicDF.itertuples():
            Ticker_Symbol = str(r.Ticker_Symbol)
            symbol = Ticker_Symbol.replace(".", "_")
            if symbol in amCodes:
                # print("amCodes:",amCodes[symbol])
                ric_dt[r.Ticker_Symbol] = amCodes[symbol]['windCode']
            else:
                print("not find....", Ticker_Symbol, r)
            pass
        # ric_dt
        basic_rows = []
        for r in basicDF.itertuples():
            Ticker_Symbol = str(r.Ticker_Symbol)
            if Ticker_Symbol in ric_dt:
                windCode = ric_dt[Ticker_Symbol]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                Price_Close = float(r.Price_Close)
                Volume = float(r.Volume)
                Average_Daily_Volume___10_Days = float(r.Average_Daily_Volume___10_Days)
                basic_rows.append([windCode, str(windCode).split(".")[0], str(r.Instrument), Price_Close, Volume,
                                   Average_Daily_Volume___10_Days, vtradeDate])
        print("basesql:", basesql)
        if len(basic_rows) > 0:
            con, cur = getConnect()
            # cur.execute("delete from %s "%ntablename)
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            cur.executemany(basesql, basic_rows)
            print("rowcount cur:", cur.rowcount)
            con.commit()
            cur.close()
            con.close()

    def newfactors_new2(self, sheet_name, vtradeDate):
        xlsxpath = r"D:\demand\mys\starmine\output\basic.xlsx"
        basicDF = pd.read_excel(xlsxpath)
        # 所有美股
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        # print("amCodes:", amCodes)

        bcols = basicDF.columns.tolist()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        basicDF.columns = bcols
        ric_dt = {}
        for r in basicDF.itertuples():
            Ticker_Symbol = str(r.Ticker_Symbol)
            symbol = Ticker_Symbol.replace(".", "_")
            if symbol in amCodes:
                # print("amCodes:",amCodes[symbol])
                ric_dt[r.Instrument] = amCodes[symbol]['windCode']
            else:
                print("not find....", Ticker_Symbol, r)
            pass
        # ric_dt
        xlsxpath = r"D:\demand\mys\starmine\output\%s.xlsx" % sheet_name
        sheetDF = pd.read_excel(xlsxpath)
        bcols = sheetDF.columns.tolist()
        fixs = sheet_name[0]
        bcols = [fixs + self.make_valid_filename(str(x)) for x in bcols]
        if sheet_name in ['valuation']:
            if fixs + "PERATIO" in bcols:
                bcols[bcols.index(fixs + "PERATIO")] = fixs + "P__E__"
        sheetDF.columns = bcols
        print("sheetDF bcols:", bcols)
        ntablename = "newdata.reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockcode", "RIC"]
        dcols = []
        for bc in bcols[1:]:
            if bc in [fixs + 'name', fixs + "RIC", fixs + 'Ticker_Symbol', fixs + 'nan']:
                continue
            dcols.append(bc)
            ncols.append(bc)
            acs.append("%s numeric(22,6)" % (bc))
        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")
        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        print(ctables)
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
        basic_rows = []
        for i, r in sheetDF.iterrows():
            ndr = dict(r)
            RIC = str(ndr[bcols[0]])
            if RIC in ric_dt:
                windCode = ric_dt[RIC]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                drw = [windCode, str(windCode).split(".")[0], RIC]
                for gd in dcols:
                    gdvalue = float(ndr[gd])
                    drw.append(gdvalue)
                drw.append(vtradeDate)
                basic_rows.append(drw)
        print("basesql:", basesql)
        print(sheet_name + ":", len(basic_rows))
        if len(basic_rows) > 0:
            con, cur = getConnect()
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            cur.executemany(basesql, basic_rows)
            print("%s rowcount:%s" %(ntablename, cur.rowcount))
            con.commit()
            cur.close()
            con.close()

    def newfactors_apend(self, sheet_name, vtradeDate):
        # basicDF = pd.read_excel(r"D:\demand\mys\starmine\output\%s.xlsx"%sheet_name)
        fdname = datetime.datetime.now().strftime("%Y-%m-%d")
        basicDF = pd.read_excel(r"D:\demand\mys\starmine\outputapend\%s\v4\%s.xlsx" % (fdname, sheet_name))

        factorsDF.replace([np.NAN], None, inplace=True)

        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        jpCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["jpStocks"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                jpCodes[s['windCode']] = s
        # print("amCodes:", amCodes)
        print("jpCodes:", len(jpCodes))
        bcols = basicDF.columns.tolist()
        oldbcols = bcols.copy()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        colsdt = dict(zip(bcols, oldbcols))
        basicDF.columns = bcols
        print("bcols:", basicDF.columns)
        ntablename = "newdata.n_reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockCode", "RIC"]
        dcols = []
        for bc in bcols:
            if bc in ['name', "Instrument", 'Ticker_Symbol', 'nan', 'GICS_Industry_Name', 'GICS_Sub_Industry_Name',
                      'GICS_Sector_Name', 'Country_of_Exchange']:
                continue
            dcols.append(bc)
            bc = re.sub(r'_+', '_', bc)
            bc = str(bc).replace("_", "")
            ncols.append(bc)
            acs.append("%s numeric(22,6)" % (bc))

        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")
        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        # print(ctables)
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
        # print("bcols:", bcols)
        ric_dt = {}
        for r in basicDF.itertuples():
            Country_of_Exchange = str(r.Country_of_Exchange)
            if Country_of_Exchange in ['United States of America']:
                Ticker_Symbol = str(r.Ticker_Symbol)
                symbol = Ticker_Symbol.replace(".", "_")
                if symbol in amCodes:
                    # print("amCodes:",amCodes[symbol])
                    ric_dt[r.Instrument] = {"windCode": amCodes[symbol]['windCode'], 'area': "US"}
                else:
                    # print("not find....", Ticker_Symbol, r)
                    pass
            elif Country_of_Exchange in ['Japan']:
                Instrument = str(r.Instrument)
                if Instrument in jpCodes:
                    ric_dt[Instrument] = {"windCode": jpCodes[Instrument]['windCode'], 'area': "JP"}
                else:
                    # print("not find....", jpCodes, r)
                    pass
            pass
        # ric_dt
        basic_rows = []
        # for r in basicDF.itertuples():
        #     Ticker_Symbol=str(r.Ticker_Symbol)
        #     if Ticker_Symbol in ric_dt:
        #         windCode = ric_dt[Ticker_Symbol] # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
        #         Price_Close=float(r.Price_Close)
        #         Volume=float(r.Volume)
        #         Average_Daily_Volume___10_Days=float(r.Average_Daily_Volume___10_Days)
        #         Price____Intrinsic_Value_Country_Rank=float(r.Price____Intrinsic_Value_Country_Rank)
        #         basic_rows.append([windCode,str(windCode).split(".")[0],str(r.Instrument),Price_Close,Volume,Average_Daily_Volume___10_Days,Price____Intrinsic_Value_Country_Rank,vtradeDate])

        for i, r in basicDF.iterrows():
            ndr = dict(r)
            Instrument = str(ndr[bcols[0]])
            if Instrument in ric_dt:
                ric_IF = ric_dt[Instrument]
                windCode = ric_IF["windCode"]
                if ric_IF["area"] in ['US']:
                    stockCode = str(windCode).split(".")[0]
                else:
                    stockCode = windCode
                drw = [windCode, stockCode, str(ndr[bcols[0]])]
                for gd in dcols:
                    if pd.isnull(ndr[gd]):
                        gdvalue = None
                    else:
                        gdvalue = float(ndr[gd])
                    drw.append(gdvalue)
                drw.append(vtradeDate)
                basic_rows.append(drw)
        t_gpt_json = {"windCode": "Wind code is used to identify a company."}
        for dcol in dcols:
            i_factors = "INSERT INTO dsidmfactors.factorcell(factorno, factorclass, factortype, factordesc, factortable, factortname, factororder, regxnumber, regxtype, floatsize, enable, qmode, fdesc) " \
                        "VALUES((select nextval('seq_factor')), NULL, '{factortype}', '{factordesc}', '{ntablename}', '{factortname}', NULL, NULL, NULL, NULL, NULL, NULL, NULL);" \
                .format(**{"factortype": str(ntablename).replace("newdata.", ""), "ntablename": ntablename,
                           "factordesc": self.fmtfactor_dt[dcol][1],
                           "factortname": self.fmtfactor_dt[dcol][1]})  # factordesc colsdt[dcol]
            # print(i_factors)
            #"update factorcell set searchkey ='Price / Intrinsic Value Country Rank', factordesc ='Price / Intrinsic Value Country Rank' where factorno  =28702 and factortname='%s' "
            searchkey= self.fmtfactor_dt[dcol][4] + " " +self.fmtfactor_dt[dcol][5]
            fview = self.fmtfactor_dt[dcol][1]
            if self.fmtfactor_dt[dcol][4] == self.fmtfactor_dt[dcol][5]:
                searchkey = self.fmtfactor_dt[dcol][4]
            if self.fmtfactor_dt[dcol][-1] !="":
                searchkey=searchkey + " " +self.fmtfactor_dt[dcol][-1]
                fview=self.fmtfactor_dt[dcol][-1]

            u_factors="update factorcell set searchkey ='{searchkey}', factordesc ='{factordesc}' , FVIEW ='{fview}' where factortable='{ntablename}'  and factortname='{factortname}'; "\
                .format(**{"factortname": self.fmtfactor_dt[dcol][1],
                       "searchkey": searchkey,
                           "ntablename": ntablename,
                           "fview": fview,
                       "factordesc": self.fmtfactor_dt[dcol][4] })  # factordesc colsdt[dcol]
            print(u_factors)

            t_gpt_json[self.fmtfactor_dt[dcol][1]] = self.fmtfactor_dt[dcol][-1]
        t_gpt_json['tradedate'] = "The 'Tradedate' is the date that the financial indicator is recorded."
        sgtp_json = self.gtp_json
        sgtp_json[ntablename] = t_gpt_json
        self.gtp_json = sgtp_json
        # select nextval('seqtest') as seqtest
        # print("basesql:", basesql)
        return
        if len(basic_rows) > 0:
            con, cur = getConnect()
            # cur.execute("delete from %s "%ntablename)
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            cur.executemany(basesql, basic_rows)
            print("rowcount cur:", cur.rowcount)
            wx_send.send_wx_msg_operation("%s add %s (条)[%s]" % (ntablename, cur.rowcount, vtradeDate))
            con.commit()
            cur.close()
            con.close()

    def newfactors_apend2(self, sheet_name, vtradeDate):
        fdname = datetime.datetime.now().strftime("%Y-%m-%d")
        xlsxpath = r"D:\demand\mys\starmine\outputapend\%s\v4\basic.xlsx" % (fdname)

        basicDF = pd.read_excel(xlsxpath)
        # 所有美股
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        # print("amCodes:", amCodes)

        jpCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["jpStocks"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                jpCodes[s['windCode']] = s

        # print("jpCodes:", len(jpCodes))

        bcols = basicDF.columns.tolist()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        basicDF.columns = bcols
        ric_dt = {}
        for r in basicDF.itertuples():
            Country_of_Exchange = str(r.Country_of_Exchange)
            if Country_of_Exchange in ['United States of America']:
                Ticker_Symbol = str(r.Ticker_Symbol)
                symbol = Ticker_Symbol.replace(".", "_")
                if symbol in amCodes:
                    # print("amCodes:",amCodes[symbol])
                    ric_dt[r.Instrument] = {"windCode": amCodes[symbol]['windCode'], 'area': "US"}
                else:
                    # print("not find....", Ticker_Symbol, r)
                    pass
            elif Country_of_Exchange in ['Japan']:
                Instrument = str(r.Instrument)
                if Instrument in jpCodes:
                    ric_dt[Instrument] = {"windCode": jpCodes[Instrument]['windCode'], 'area': "JP"}
                else:
                    # print("not find....", jpCodes, r)
                    pass
        # ric_dt
        xlsxpath = r"D:\demand\mys\starmine\outputapend\%s\v4\%s.xlsx" % (fdname, sheet_name)
        sheetDF = pd.read_excel(xlsxpath)
        bcols = sheetDF.columns.tolist()
        fixs = sheet_name[0]
        oldbcols = bcols.copy()
        bcols = [fixs + self.make_valid_filename(str(x)) for x in bcols]
        colsdt = dict(zip(bcols, oldbcols))
        # if sheet_name in ['valuation']:
        #     if fixs+"PERATIO" in bcols:
        #         bcols[bcols.index(fixs+"PERATIO")]=fixs+"P__E__"
        sheetDF.columns = bcols
        # print("sheetDF bcols:", bcols)
        ntablename = "newdata.n_reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockcode", "RIC"]
        dcols = []
        factorCcols = []
        for bc in bcols[1:]:
            if bc in [fixs + 'name', fixs + "RIC", fixs + 'Ticker_Symbol', fixs + 'nan']:
                continue
            dcols.append(bc)  # 带前缀 下划线
            # print(len(bc), bc)
            bc = re.sub(r'_+', '_', bc)
            bc = str(bc).replace("_", "")  # 无下划线
            factorCcols.append(bc)
            ncols.append(bc)
            acs.append("%s numeric(22,6)" % (bc))
        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")

        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        # print("drop table %s;" % ntablename)
        # print(ctables)

        # 创建指标名称
        t_gpt_json = {"windCode": "Wind code is used to identify a company."}
        print("--****************")
        for _idx, dcol in enumerate(dcols):
            i_factors = "INSERT INTO dsidmfactors.factorcell(factorno, factorclass, factortype, factordesc, factortable, factortname, factororder, regxnumber, regxtype, floatsize, enable, qmode, fdesc) " \
                        "VALUES((select nextval('seq_factor')), NULL, '{factortype}', '{factordesc}', '{ntablename}', '{factortname}', NULL, NULL, NULL, NULL, NULL, NULL, NULL);" \
                .format(**{"factortype": str(ntablename).replace("newdata.", ""), "ntablename": ntablename,
                           "factordesc": self.fmtfactor_dt[dcol[len(fixs):]][1], "factortname": factorCcols[_idx]})
            # print(i_factors)
            searchkey = self.fmtfactor_dt[dcol[len(fixs):]][4]
            if self.fmtfactor_dt[dcol[len(fixs):]][4] == self.fmtfactor_dt[dcol[len(fixs):]][5]:
                searchkey = self.fmtfactor_dt[dcol[len(fixs):]][4]
            fview = self.fmtfactor_dt[dcol[len(fixs):]][4]
            if self.fmtfactor_dt[dcol[len(fixs):]][-1] !="":
                searchkey=searchkey + " " + self.fmtfactor_dt[dcol[len(fixs):]][-1]
                fview=self.fmtfactor_dt[dcol[len(fixs):]][-1]

            u_factors = "update factorcell set searchkey ='{searchkey}', factordesc ='{factordesc}', FVIEW ='{fview}' where factortable='{ntablename}'  and factortname='{factortname}'; " \
                .format(**{"factortname": factorCcols[_idx],
                           "searchkey": searchkey, "ntablename": ntablename, "fview": fview,
                           "factordesc": self.fmtfactor_dt[dcol[len(fixs):]][4]})  # factordesc colsdt[dcol]
            print(u_factors)
            t_gpt_json[factorCcols[_idx]] = self.fmtfactor_dt[dcol[len(fixs):]][-1]
        print("--****************")
        t_gpt_json['tradedate'] = "The 'Tradedate' is the date that the financial indicator is recorded."
        sgtp_json = self.gtp_json
        sgtp_json[ntablename] = t_gpt_json
        self.gtp_json = sgtp_json
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
        basic_rows = []
        for i, r in sheetDF.iterrows():
            ndr = dict(r)
            Instrument = str(ndr[bcols[0]])
            if Instrument in ric_dt:
                ric_IF = ric_dt[Instrument]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                windCode = ric_IF["windCode"]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                if ric_IF["area"] in ['US']:
                    stockCode = str(windCode).split(".")[0]
                else:
                    stockCode = windCode
                drw = [windCode, stockCode, Instrument]
                for gd in dcols:
                    if pd.isnull(ndr[gd]):
                        gdvalue = None
                    else:
                        gdvalue = float(ndr[gd])
                    drw.append(gdvalue)
                drw.append(vtradeDate)
                basic_rows.append(drw)
        # print("basesql:", basesql)
        return
        print(sheet_name + ":", len(basic_rows))
        if len(basic_rows) > 0:
            con, cur = getConnect()
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            cur.executemany(basesql, basic_rows)
            # print("rowcount:", cur.rowcount)
            # wx_send.send_wx_msg_operation("%s add %s (条)[%s]" % (ntablename, cur.rowcount, vtradeDate))
            con.commit()
            cur.close()
            con.close()

    def newfactors_apend3(self, sheet_name, vtradeDate):
        fdname = datetime.datetime.now().strftime("%Y-%m-%d")
        xlsxpath = r"D:\demand\mys\starmine\outputapend\%s\v4\basic.xlsx" % fdname
        basicDF = pd.read_excel(xlsxpath)
        # 所有美股
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        # print("amCodes:", amCodes)

        jpCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["jpStocks"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                jpCodes[s['windCode']] = s

        print("jpCodes:", len(jpCodes))
        bcols = basicDF.columns.tolist()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        basicDF.columns = bcols
        ric_dt = {}
        for r in basicDF.itertuples():
            Country_of_Exchange = str(r.Country_of_Exchange)
            if Country_of_Exchange in ['United States of America']:
                Ticker_Symbol = str(r.Ticker_Symbol)
                symbol = Ticker_Symbol.replace(".", "_")
                if symbol in amCodes:
                    # print("amCodes:",amCodes[symbol])
                    ric_dt[r.Instrument] = {"windCode": amCodes[symbol]['windCode'], 'area': "US"}
                else:
                    print("not find....", Ticker_Symbol, r)
            elif Country_of_Exchange in ['Japan']:
                Instrument = str(r.Instrument)
                if Instrument in jpCodes:
                    ric_dt[Instrument] = {"windCode": jpCodes[Instrument]['windCode'], 'area': "JP"}
                else:
                    print("not find....", jpCodes, r)

        # ric_dt
        xlsxpath = r"D:\demand\mys\starmine\outputapend\%s\v4\%s.xlsx" % (fdname, sheet_name)
        sheetDF = pd.read_excel(xlsxpath)
        bcols = sheetDF.columns.tolist()
        fixs = sheet_name[0]
        oldbcols = bcols.copy()
        bcols = [fixs + self.make_valid_filename(str(x)) for x in bcols]
        colsdt = dict(zip(bcols, oldbcols))
        # if sheet_name in ['valuation']:
        #     if fixs+"PERATIO" in bcols:
        #         bcols[bcols.index(fixs+"PERATIO")]=fixs+"P__E__"
        sheetDF.columns = bcols
        print("sheetDF bcols:", bcols)
        ntablename = "newdata.n_reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockcode", "RIC"]
        dcols = []
        factorCcols = []
        for bc in bcols[1:]:
            if bc in [fixs + 'name', fixs + "RIC", fixs + 'Ticker_Symbol', fixs + 'nan']:
                continue
            dcols.append(bc)  # 带前缀 下划线
            print(len(bc), bc)
            bc = re.sub(r'_+', '_', bc)
            bc = str(bc).replace("_", "")  # 无下划线
            factorCcols.append(bc)
            ncols.append(bc)
            acs.append("%s varchar(200)" % (bc))
        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")

        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        print("drop table %s;" % ntablename)
        print(ctables)

        # 创建指标名称
        t_gpt_json = {"windCode": "Wind code is used to identify a company."}
        for _idx, dcol in enumerate(factorCcols):
            i_factors = "INSERT INTO dsidmfactors.factorcell(factorno, factorclass, factortype, factordesc, factortable, factortname, factororder, regxnumber, regxtype, floatsize, enable, qmode, fdesc) " \
                        "VALUES((select nextval('seq_factor')), NULL, '{factortype}', '{factordesc}', '{ntablename}', '{factortname}', NULL, NULL, NULL, NULL, NULL, NULL, NULL);" \
                .format(**{"factortype": str(ntablename).replace("newdata.", ""), "ntablename": ntablename,
                           "factordesc": dcol, "factortname": dcol})
            print(i_factors)
            t_gpt_json[dcol] = dcol
        t_gpt_json['tradedate'] = "The 'Tradedate' is the date that the financial indicator is recorded."
        sgtp_json = self.gtp_json
        sgtp_json[ntablename] = t_gpt_json
        self.gtp_json = sgtp_json
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
        basic_rows = []
        for i, r in sheetDF.iterrows():
            ndr = dict(r)
            Instrument = str(ndr[bcols[0]])
            if Instrument in ric_dt:
                ric_IF = ric_dt[Instrument]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                windCode = ric_IF["windCode"]  # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
                if ric_IF["area"] in ['US']:
                    stockCode = str(windCode).split(".")[0]
                else:
                    stockCode = windCode
                drw = [windCode, stockCode, Instrument]
                for gd in dcols:
                    if pd.isnull(ndr[gd]):
                        gdvalue = None
                    else:
                        gdvalue = str(ndr[gd])
                    drw.append(gdvalue)
                drw.append(vtradeDate)
                basic_rows.append(drw)
        print("basesql:", basesql)
        print(sheet_name + ":", len(basic_rows))
        if len(basic_rows) > 0:
            con, cur = getConnect()
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            print("%s rowcount:%s"  % (ntablename,cur.rowcount))
            cur.executemany(basesql, basic_rows)
            print("%s rowcount:%s", (cur.rowcount,cur.rowcount))
            wx_send.send_wx_msg_operation("%s add %s (条)[%s]" % (ntablename, cur.rowcount, vtradeDate))
            con.commit()
            cur.close()
            con.close()

    def newfactors_desc(self):
        xlsxpath = r"D:\demand\mys\starmine\factor_names_descriptions_values_filled.xlsx"
        xlsxpath = r"D:\demand\mys\starmine\factor_names_descriptions_values_filled_0914.xlsx"
        xlsxpath = r"D:\demand\mys\starmine\factor_names_descriptions_values_filled_v4.xlsx"
        xlsxpath = r"D:\demand\mys\starmine\factor_names_descriptions_values_filled_v5.xlsx"
        xlsxpath = r"D:\demand\mys\starmine\factor_names_v4_tc.xlsx"
        # xlsxpath = r"D:\demand\mys\starmine\factor_names_descriptions_values_filled_v3指标分类.xlsx"
        factorsDF = pd.read_excel(xlsxpath)
        import numpy as np
        factorsDF.replace([np.inf, -np.inf], np.nan, inplace=True)
        # print("factorsDF:", factorsDF)
        baseColumns = factorsDF.columns.tolist()
        newColumns = [str(x).replace(" ", "") for x in baseColumns]
        factorsDF.columns = newColumns
        print("factorsDF.colunns", factorsDF.columns)
        fmtfactor_dt = {}
        fmtfactor_list = []
        factortypes = list(set(factorsDF['系统指标分类'].values.tolist()))
        #select  *  FROM dsidmfactors.menufactor where menutype='4' order by menuid  ;
        con, cur = getConnect()
        q_menu="select  *  FROM dsidmfactors.menufactor where menutype='4' order by menuid"
        menuDF=pd.read_sql(q_menu,con)
        menudt={}
        for i,m in menuDF.iterrows():
            menudt[m.MENUNAME]=dict(m)

        # q_menucfg="SELECT TT.*  FROM dsidmfactors.menufactor t INNER JOIN dsidmfactors.menufactorsconfig tt ON t.menuid = tt.menuid and T. menutype='4'"
        # menucfgDF=pd.read_sql(q_menucfg,con)
        # menucfgdt={}
        #
        # for i,m in menuDF.iterrows():
        #     TmenucfgDF=menucfgDF[menucfgDF['MENUID']==m.MENUID]

        factortypes=["basic","dividend","ownership","debt","earnings","mom","profitability","valuation","growth","trading","starminecore","analyst"]
        create_tables=[]
        create_factors=[]
        create_menucfg=[]
        for ft in factortypes:
            if pd.isnull(ft):
                continue
            t_factorsDF=factorsDF[factorsDF['系统指标分类']==str(ft)]
            sheet_name=ft
            ntablename = "newdata.a_reuters_%s" % sheet_name
            ctb = "create table %s \n(" % ntablename
            acs = ["windCode varchar(20)"]
            acs.append("stockcode varchar(20)")
            acs.append("RIC varchar(20)")

            ncols = ["windCode", "stockcode", "RIC"]
            add_factors=[]
            add_menufactorsconfig=[]

            menuInfo = menudt[ft]
            menuID=menuInfo['MENUID']
            ord=0
            #检查是否有重复key
            ck_list=[]
            for i, r in t_factorsDF.iterrows():
                ck_list.append(str( r.factor_codes).replace("TR.","").replace("*100","").replace("Period","").replace("=","").strip())
            if len(ck_list) != len(set(ck_list)):
                print("列表中没重复数据")
                return

            for i, r in t_factorsDF.iterrows():
                valuetype = r.valuetype
                factor_code =str( r.factor_codes).replace("TR.","").replace("*100","").replace("Period","").replace("=","").strip()
                selffactorno = int(r.selffactorno)
                factor_code = self.make_valid_filename(factor_code)
                indicator_name = str(r.newindicatorname).replace(" ?","")
                SysFactorName = r.SysFactorName
                BBG = r.BBG  # 显示
                if pd.isnull(BBG):
                    BBG =  r.newindicatorname
                if pd.isnull(SysFactorName):
                    SysFactorName =  r.newindicatorname
                fmt_indicator_name = self.make_valid_filename(indicator_name)
                # 字段超长处理
                # if len(fmt_indicator_name)>40:
                #     fmt_indicator_name = re.sub(r'_+', '_', fmt_indicator_name)
                #     fmt_indicator_name = str(fmt_indicator_name).replace("_","")
                vbc = re.sub(r'_+', '_', fmt_indicator_name)
                vbc = str(vbc).replace("_", "")
                long_description = r.longdescription
                fmtfactor_dt[factor_code] = dict(r)
                fmtfactor_list.append(
                    [factor_code, fmt_indicator_name, vbc, indicator_name, long_description, SysFactorName, BBG])
                floatsize=None
                if str(valuetype)=="str":
                    acs.append("%s varchar(100)" % (factor_code))
                    floatsize=-1
                else:
                    acs.append("%s numeric(22,6)" % (factor_code))

                if factor_code in ['name', "RIC", 'Ticker_Symbol', 'nan']:
                    continue
                ncols.append(factor_code)

                fview = BBG
                if BBG == SysFactorName:
                    searchkey = fview +" " + SysFactorName
                else:
                    searchkey =fview    +" " +  SysFactorName  +" " +  indicator_name

                if floatsize is None:
                    i_factors = "INSERT INTO dsidmfactors.factorcell(factorno, factorclass, factortype, factordesc, factortable, factortname, factororder, regxnumber, regxtype, " \
                                "floatsize, enable, qmode, fdesc,searchkey,fview) " \
                                "VALUES({factorno}, NULL, '{factortype}', '{factordesc}', '{ntablename}', '{factortname}', NULL, NULL, NULL, NULL, NULL, NULL, NULL,'{searchkey}','{fview}');" \
                        .format(**{"factorno": selffactorno,"factortype": str(ntablename).replace("newdata.", ""), "ntablename": ntablename,
                                   "floatsize": floatsize, "factordesc": r.newindicatorname,
                                   "factortname": factor_code,
                               "fview": fview,    "searchkey": searchkey,})  # factordesc colsdt[dcol]
                else:
                    i_factors = "INSERT INTO dsidmfactors.factorcell(factorno, factorclass, factortype, factordesc, factortable, factortname, factororder, regxnumber, regxtype, " \
                            "floatsize, enable, qmode, fdesc,searchkey,fview) " \
                            "VALUES({factorno}, NULL, '{factortype}', '{factordesc}', '{ntablename}', '{factortname}', NULL, NULL, NULL, {floatsize}, NULL, NULL, NULL,'{searchkey}','{fview}');" \
                    .format(**{"factorno": selffactorno,"factortype": str(ntablename).replace("newdata.", ""), "ntablename": ntablename,
                               "floatsize": floatsize, "factordesc": r.newindicatorname,
                               "factortname": factor_code,
                           "fview": fview,    "searchkey": searchkey,})  # factordesc colsdt[dcol]
                add_factors.append(i_factors)
                ord+=1
                add_menufactorsconfig.append("INSERT INTO dsidmfactors.menufactorsconfig (menuid, factorno, ord) VALUES('%s', %s, %s);"%(menuID,selffactorno,ord))
            acs.append("tradeDate varchar(8)")
            cte = "\n);"
            ctables = ctb + ",\n".join(acs) + cte
            print("drop table if EXISTS %s;"%ntablename) #建表sql
            print(ctables) #建表sql

            create_tables.append("\n\n------------create table  %s----------------\n\n"%ntablename)
            create_tables.append("\n\ndrop table if EXISTS %s;\n\n"%ntablename)
            create_tables.append(ctables)
            create_tables.append("\n\n------------end table  %s----------------"%ntablename)



            # basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
            # print(basesql) #insert basesql

            create_factors.append("\n\n------------add factors  %s----------------\n\n"%ntablename)
            print("\n".join(add_factors)) #insert basesql
            create_factors.append("\n\n".join(add_factors))
            create_factors.append("\n\n------------end factors   %s----------------"%ntablename)

            #factorno
            print("\n".join(add_menufactorsconfig)) #insert basesql
            create_menucfg.append("\n\n------------add menufactorsconfig  %s----------------\n\n"%ntablename)
            create_menucfg.append("\n\n".join(add_menufactorsconfig))
            create_menucfg.append("\n\n------------end menufactorsconfig   %s----------------"%ntablename)
            #factorno

        with open (r"D:\demand\mys\starmine\outputapend\createtable.sql",'w',encoding='utf8') as wf:
            wf.write("".join(create_tables))
        with open (r"D:\demand\mys\starmine\outputapend\addfactor.sql",'w',encoding='utf8') as wf:
            wf.write("".join(create_factors))
        with open (r"D:\demand\mys\starmine\outputapend\addmenucfg.sql",'w',encoding='utf8') as wf:
            wf.write("".join(create_menucfg))

        return

    def newfactors_desc2(self):
        xlsxpath = r"D:\demand\mys\starmine\outputapend\factor_names.xlsx"
        factorsDF = pd.read_excel(xlsxpath)
        print("factorsDF:", factorsDF)
        baseColumns = factorsDF.columns.tolist()
        newColumns = [str(x).replace(" ", "") for x in baseColumns]
        factorsDF.columns = newColumns
        print("factorsDF.colunns", factorsDF.columns)
        fmtfactor_dt = {}
        fmtfactor_list = []
        for i, r in factorsDF.iterrows():
            indicator_name = r.factor_names
            factor_codes = r.factor_codes
            # 查找括弧中的内容
            matches = re.findall(r'\((.*?)\)', factor_codes)
            print("matches:", matches)
            if len(matches) > 0:
                params = str(matches[0]).split(",")

                # for p in params:
                #     p.split("=")[-1]
            str(factor_codes)
            fmt_indicator_name = self.make_valid_filename(indicator_name)

            # 字段超长处理
            # if len(fmt_indicator_name)>40:
            #     fmt_indicator_name = re.sub(r'_+', '_', fmt_indicator_name)
            #     fmt_indicator_name = str(fmt_indicator_name).replace("_","")

            vbc = re.sub(r'_+', '_', fmt_indicator_name)
            vbc = str(vbc).replace("_", "")

            fmtfactor_dt[fmt_indicator_name] = [fmt_indicator_name, vbc, indicator_name]
            fmtfactor_list.append([fmt_indicator_name, vbc, indicator_name])

        return fmtfactor_dt, fmtfactor_list

    def copyDataToGpt(self, vtradedate):
        areasInfo=[
            {"area":'us',"exchange":"United States of America"},
            {"area":'hk',"exchange":"Hong Kong"},
            {"area":'jp',"exchange":"Japan"},
            {"area":'ch',"exchange":"China"} ]
        factortypes = ["basic", "dividend", "ownership", "debt", "earnings", "mom", "profitability", "valuation",
                       "growth", "trading", "starminecore", "analyst"]

        con, cur = getConnect()
        for aif in areasInfo:
            area=aif['area']
            exchange=aif['exchange']
            for cptable in factortypes:
                d_sql = "delete from newdata.%s_reuters_%s where tradedate='%s'" % (area,cptable, vtradedate)
                cp_sql = "insert into newdata.%s_reuters_%s  select  * from  newdata.a_reuters_%s" \
                         "  where ric in (select ric from  newdata.a_reuters_basic nrg where exchangecountry='%s') and tradedate='%s'" % (
                         area, cptable,cptable,exchange, vtradedate)
                cur.execute(d_sql)
                # print(d_sql+";")
                print("%s：d_sql rowcount:%s"%(d_sql,cur.rowcount))
                # print(cp_sql+";")
                cur.execute(cp_sql)
                print("%s：cp_sql rowcount:%s"%(cptable,cur.rowcount))
                con.commit()
        cur.close()
        con.close()

    def modifyLastDay(self,vtradedate):
        factortypes = ["basic", "dividend", "ownership", "debt", "earnings", "mom", "profitability", "valuation",
                       "growth", "trading", "starminecore", "analyst"]
        con, cur = dbutils.getConnect()
        for cptable in factortypes:
            tablename = "newdata.a_reuters_%s" % cptable
            up_date="update %s set tradedate='%s' "%(tablename,vtradedate)
            cur.execute(up_date)
            con.commit()
        cur.close()
        con.close()

    def copyDataLastDay(self, vtradedate):
        factortypes = ["basic", "dividend", "ownership", "debt", "earnings", "mom", "profitability", "valuation",
                       "growth", "trading", "starminecore", "analyst"]
        con, cur = getConnect()
        for cptable in factortypes:
            #qcolumns
            table_name = "a_reuters_%s" % cptable
            tablename = "newdata.a_reuters_%s" % cptable
            print("table_name:",table_name)
            q_columns=" SELECT column_name FROM information_schema.columns WHERE table_name = %s and table_schema ='newdata'"
            print("q_columns:",q_columns)
            columnsDF=pd.read_sql(q_columns,con,params=[table_name])
            tbcolumns=columnsDF['COLUMN_NAME'].values.tolist()
            print("tbcolumns:",tbcolumns)
            stbcolumns=",".join(tbcolumns)
            ccolumns=stbcolumns.replace("tradedate","'%s'"%vtradedate)
            # d_sql = "delete from newdata.t_reuters_%s where tradedate='%s'" % (cptable, vtradedate)
            cp_sql = "insert into {tablename} ({icolumns}) select {ccolumns} from  {tablename}" \
                     "  where  tradedate=(select max(tradedate) from {tablename})" \
                .format(**{"tablename":tablename,"icolumns":stbcolumns,
                           "ccolumns":ccolumns})
            cp_d="delete from %s  where tradedate='%s'"%(tablename,vtradedate)
            print("cp_d:",cp_d)
            print("cp_sql:",cp_sql)
            cur.execute(cp_d)
            print("del tablename:%s,num:%s"%(tablename,cur.rowcount))
            cur.execute(cp_sql)
            print("copy tablename:%s,num:%s"%(tablename,cur.rowcount))
            con.commit()
            continue
            cp_sql = "insert into newdata.t_reuters_%s (%s) select %s from  newdata.n_reuters_%s" \
                     "  where ric in (select ric from  newdata.n_reuters_gics nrg where gCountryofExchange='United States of America') and tradedate='%s'" % (
                     cptable, cptable, vtradedate)
            cur.execute(d_sql)
            # print(d_sql+";")
            # print(cp_sql+";")
            print("%s：d_sql rowcount:%s"%(d_sql,cur.rowcount))
            cur.execute(cp_sql)
            print("%s：cp_sql rowcount:%s"%(cptable,cur.rowcount))
        # con.commit()
        cur.close()
        con.close()


    def copyNewToHis(self, vtradedate):
        factortypes = ["basic", "dividend", "ownership", "debt", "earnings", "mom", "profitability", "valuation",
                       "growth", "trading", "starminecore", "analyst"]
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


    def copyDataLastDay(self, vtradedate):
        factortypes = ["basic", "dividend", "ownership", "debt", "earnings", "mom", "profitability", "valuation",
                       "growth", "trading", "starminecore", "analyst"]
        con, cur = getConnect()
        for cptable in factortypes:
            #qcolumns
            table_name = "a_reuters_%s" % cptable
            tablename = "newdata.a_reuters_%s" % cptable
            print("table_name:",table_name)
            q_columns=" SELECT column_name FROM information_schema.columns WHERE table_name = %s and table_schema ='newdata'"
            print("q_columns:",q_columns)
            columnsDF=pd.read_sql(q_columns,con,params=[table_name])
            columnsDF=fmt_df_column_upper(columnsDF)
            tbcolumns=columnsDF['COLUMN_NAME'].values.tolist()
            print("tbcolumns:",tbcolumns)
            stbcolumns=",".join(tbcolumns)
            ccolumns=stbcolumns.replace("tradedate","'%s'"%vtradedate)
            # d_sql = "delete from newdata.t_reuters_%s where tradedate='%s'" % (cptable, vtradedate)
            cp_sql = "insert into {tablename} ({icolumns}) select {ccolumns} from  {tablename}" \
                     "  where  tradedate=(select max(tradedate) from {tablename})" \
                .format(**{"tablename":tablename,"icolumns":stbcolumns,
                           "ccolumns":ccolumns})
            cp_d="delete from %s  where tradedate='%s'"%(tablename,vtradedate)
            print("cp_d:",cp_d)
            print("cp_sql:",cp_sql)
            cur.execute(cp_d)
            print("del tablename:%s,num:%s"%(tablename,cur.rowcount))
            cur.execute(cp_sql)
            print("copy tablename:%s,num:%s"%(tablename,cur.rowcount))
            con.commit()
            continue
            cp_sql = "insert into newdata.t_reuters_%s (%s) select %s from  newdata.n_reuters_%s" \
                     "  where ric in (select ric from  newdata.n_reuters_gics nrg where gCountryofExchange='United States of America') and tradedate='%s'" % (
                     cptable, cptable, vtradedate)
            cur.execute(d_sql)
            # print(d_sql+";")
            # print(cp_sql+";")
            print("%s：d_sql rowcount:%s"%(d_sql,cur.rowcount))
            cur.execute(cp_sql)
            print("%s：cp_sql rowcount:%s"%(cptable,cur.rowcount))
        # con.commit()
        cur.close()
        con.close()


    def newfactorsdataparserByArea(self, vtradeDate,area):
        fdname = (datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")
        # fdname = datetime.datetime.now().strftime("%Y-%m-%d")
        # basicDF = pd.read_excel(r"D:\demand\mys\starmine\outputapend\%s\v4\%s.xlsx" % (fdname, sheet_name))
        # basicDF = pd.read_excel(r"D:\demand\mys\starmine\outputapend\all_factors_x_%s.xlsx"%vtradeDate)
        # basicDF = pd.read_excel(r"D:\demand\mys\starmine\outputapend\%s\v8\all_factors.xlsx"%fdname)
        if isAly:
            source_path=r"Y:\current_factors\%s\%s\all_factors.xlsx"%(fdname,area)
        else:
            source_path=r"D:\demand\mys\starmine\outputapend\%s\%s\all_factors.xlsx"%(fdname,area)
        if not os.path.exists(source_path):
            return
        print("source_path:",source_path)
        basicDF = pd.read_excel(source_path)
        basicDF.replace([np.inf, -np.inf],np.NAN, inplace=True)
        basicDF=basicDF.replace({np.NAN:None})
        # basicDF.replace([np.inf, -np.inf,np.NaN],None, inplace=True)
        # basicDF.replace([np.NaN], None, inplace=True)
        vcolumns = basicDF.columns.values.tolist()
        ncolumns=[self.make_valid_filename(str(x).replace("TR.","").replace("*100","").replace("Period","").replace("=","").strip()) for x in vcolumns]
        basicDF.columns=ncolumns
        print("ncolumns:",ncolumns)
        # basicDF.to_excel("d:/work/temp/basicDFxxxx.xlsx")
        q_data="select * from factorcell f where factortable  like '%newdata.a_reuters_%'"
        con, cur = dbutils.getConnect()
        factorcellDF=pd.read_sql(q_data,con)
        factorcellDF=fmt_df_column_upper(factorcellDF)
        fillTables = set(factorcellDF['FACTORTABLE'].values.tolist())
        # fillTables=["newdata.a_reuters_basic"]
        # 所有美股
        amCodes = {}
        with dbmongo.Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        area_dt = {"hk":"Hong Kong","jp":"Japan","us":'United States of America','cn':"China"}
        basicDF=basicDF[basicDF["ExchangeCountry"]==area_dt[area]]
        for tableName in fillTables:
            tableColumnDF = factorcellDF[factorcellDF['FACTORTABLE']==tableName]
            ntablename=[]
            ntablename = ["RIC","windCode"]
            grabCol=["Instrument","TickerSymbol","ExchangeCountry"]
            for tc in tableColumnDF.itertuples():
                factortname = tc.FACTORTNAME
                if factortname not in grabCol:
                    grabCol.append(factortname)
                ntablename.append(factortname)
            ntablename.append("stockcode")
            ntablename.append("tradeDate")
            basesql = "insert into %s (%s) values(%s)" % (tableName, ",".join(ntablename), ",".join(["%s"] * len(ntablename)))
            grabDF = basicDF[grabCol]
            add_rows=[]
            # print("ntablename:",tableName,ntablename)
            for _i,r in grabDF.iterrows():
                atr = r.values.tolist()
                if "ExchangeCountry" not in ntablename:
                    didx=grabCol.index("ExchangeCountry")
                    atr.pop(didx)
                symbol=r.TickerSymbol
                ExchangeCountry=r.ExchangeCountry
                if ExchangeCountry in ['United States of America']:
                    if symbol in amCodes:#AM
                        windCode = amCodes[symbol]['windCode']
                        stockCode = str(windCode).split(".")[0]
                        atr[1]=windCode#windCode
                        atr.append(stockCode)
                        atr.append(vtradeDate)
                    else:
                        # print("not find....", symbol, r)
                        continue
                elif ExchangeCountry in ['Japan']:
                    atr[1]=r.Instrument #windCode
                    atr.append(r.Instrument)
                    atr.append(vtradeDate)
                elif ExchangeCountry in ['Hong Kong']:
                    atr[1]=r.Instrument #windCode
                    atr.append(r.Instrument)
                    atr.append(vtradeDate)
                elif ExchangeCountry in ["China"]:
                    atr[1]=str(r.Instrument).replace(".SS",".SH") #windCode
                    atr.append(str(r.Instrument).replace(".SS",".SH"))
                    atr.append(vtradeDate)
                else:
                    continue
                # print("basesql:",basesql)
                # print("atr:",atr)
                add_rows.append(atr)
            if len(add_rows) > 0:
                d_sql="delete from %s where tradedate='%s' and windcode in (select windcode  from newdata.a_reuters_basic " \
                      "  where exchangecountry ='%s' and tradedate =(select max(tradedate) from newdata.a_reuters_basic ) ) " % (tableName, vtradeDate,area_dt[area])
                cur.execute(d_sql)
                # print("d_sql:", d_sql)
                print("del %s rowcount:%s" %(tableName, cur.rowcount))
                cur.executemany(basesql, add_rows)
                print("add %s  rowcount:%s" %(tableName, cur.rowcount))
                # wx_send.send_wx_msg_operation("%s add %s (条)[%s]" % (ntablename, cur.rowcount, vtradeDate))
                con.commit()
            # grabDF.to_excel("d:/work/temp/_%ss.xlsx"%(str(tableName).replace(".","_")))
        return #5434
        amCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["stocklist"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                amCodes[s['symbol']] = s
        jpCodes = {}
        with Mongo("StockPool") as md:
            mydb = md.db
            reutersSet = mydb["jpStocks"]
            sStocks = reutersSet.find({})
            for s in sStocks:
                jpCodes[s['windCode']] = s
        # print("amCodes:", amCodes)
        print("jpCodes:", len(jpCodes))
        bcols = basicDF.columns.tolist()
        oldbcols = bcols.copy()
        bcols = [self.make_valid_filename(str(x)) for x in bcols]
        colsdt = dict(zip(bcols, oldbcols))
        basicDF.columns = bcols
        print("bcols:", basicDF.columns)
        ntablename = "newdata.n_reuters_%s" % sheet_name
        ctb = "create table %s \n(" % ntablename
        acs = ["windCode varchar(20)"]
        acs.append("stockcode varchar(20)")
        acs.append("RIC varchar(20)")
        ncols = ["windCode", "stockCode", "RIC"]
        dcols = []
        for bc in bcols:
            if bc in ['name', "Instrument", 'Ticker_Symbol', 'nan', 'GICS_Industry_Name', 'GICS_Sub_Industry_Name',
                      'GICS_Sector_Name', 'Country_of_Exchange']:
                continue
            dcols.append(bc)
            bc = re.sub(r'_+', '_', bc)
            bc = str(bc).replace("_", "")
            ncols.append(bc)
            acs.append("%s numeric(22,6)" % (bc))

        ncols.append('tradeDate')
        acs.append("tradeDate varchar(8)")
        cte = "\n);"
        ctables = ctb + ",\n".join(acs) + cte
        # print(ctables)
        basesql = "insert into %s (%s) values(%s)" % (ntablename, ",".join(ncols), ",".join(["%s"] * len(ncols)))
        # print("bcols:", bcols)
        ric_dt = {}
        for r in basicDF.itertuples():
            Country_of_Exchange = str(r.Country_of_Exchange)
            if Country_of_Exchange in ['United States of America']:
                Ticker_Symbol = str(r.Ticker_Symbol)
                symbol = Ticker_Symbol.replace(".", "_")
                if symbol in amCodes:
                    # print("amCodes:",amCodes[symbol])
                    ric_dt[r.Instrument] = {"windCode": amCodes[symbol]['windCode'], 'area': "US"}
                else:
                    # print("not find....", Ticker_Symbol, r)
                    pass
            elif Country_of_Exchange in ['Japan']:
                Instrument = str(r.Instrument)
                if Instrument in jpCodes:
                    ric_dt[Instrument] = {"windCode": jpCodes[Instrument]['windCode'], 'area': "JP"}
                else:
                    # print("not find....", jpCodes, r)
                    pass
            pass
        # ric_dt
        basic_rows = []
        # for r in basicDF.itertuples():
        #     Ticker_Symbol=str(r.Ticker_Symbol)
        #     if Ticker_Symbol in ric_dt:
        #         windCode = ric_dt[Ticker_Symbol] # 'Price_Close', 'Volume', 'Average_Daily_Volume___10_Days'
        #         Price_Close=float(r.Price_Close)
        #         Volume=float(r.Volume)
        #         Average_Daily_Volume___10_Days=float(r.Average_Daily_Volume___10_Days)
        #         Price____Intrinsic_Value_Country_Rank=float(r.Price____Intrinsic_Value_Country_Rank)
        #         basic_rows.append([windCode,str(windCode).split(".")[0],str(r.Instrument),Price_Close,Volume,Average_Daily_Volume___10_Days,Price____Intrinsic_Value_Country_Rank,vtradeDate])

        for i, r in basicDF.iterrows():
            ndr = dict(r)
            Instrument = str(ndr[bcols[0]])
            if Instrument in ric_dt:
                ric_IF = ric_dt[Instrument]
                windCode = ric_IF["windCode"]
                if ric_IF["area"] in ['US']:
                    stockCode = str(windCode).split(".")[0]
                else:
                    stockCode = windCode
                drw = [windCode, stockCode, str(ndr[bcols[0]])]
                for gd in dcols:
                    if pd.isnull(ndr[gd]):
                        gdvalue = None
                    else:
                        gdvalue = float(ndr[gd])
                    drw.append(gdvalue)
                drw.append(vtradeDate)
                basic_rows.append(drw)
        t_gpt_json = {"windCode": "Wind code is used to identify a company."}
        for dcol in dcols:
            i_factors = "INSERT INTO dsidmfactors.factorcell(factorno, factorclass, factortype, factordesc, factortable, factortname, factororder, regxnumber, regxtype, floatsize, enable, qmode, fdesc) " \
                        "VALUES((select nextval('seq_factor')), NULL, '{factortype}', '{factordesc}', '{ntablename}', '{factortname}', NULL, NULL, NULL, NULL, NULL, NULL, NULL);" \
                .format(**{"factortype": str(ntablename).replace("newdata.", ""), "ntablename": ntablename,
                           "factordesc": self.fmtfactor_dt[dcol][1],
                           "factortname": self.fmtfactor_dt[dcol][1]})  # factordesc colsdt[dcol]
            # print(i_factors)
            #"update factorcell set searchkey ='Price / Intrinsic Value Country Rank', factordesc ='Price / Intrinsic Value Country Rank' where factorno  =28702 and factortname='%s' "
            searchkey= self.fmtfactor_dt[dcol][4] + " " +self.fmtfactor_dt[dcol][5]
            fview = self.fmtfactor_dt[dcol][1]
            if self.fmtfactor_dt[dcol][4] == self.fmtfactor_dt[dcol][5]:
                searchkey = self.fmtfactor_dt[dcol][4]
            if self.fmtfactor_dt[dcol][-1] !="":
                searchkey=searchkey + " " +self.fmtfactor_dt[dcol][-1]
                fview=self.fmtfactor_dt[dcol][-1]

            u_factors="update factorcell set searchkey ='{searchkey}', factordesc ='{factordesc}' , FVIEW ='{fview}' where factortable='{ntablename}'  and factortname='{factortname}'; "\
                .format(**{"factortname": self.fmtfactor_dt[dcol][1],
                       "searchkey": searchkey,
                           "ntablename": ntablename,
                           "fview": fview,
                       "factordesc": self.fmtfactor_dt[dcol][4] })  # factordesc colsdt[dcol]
            print(u_factors)

            t_gpt_json[self.fmtfactor_dt[dcol][1]] = self.fmtfactor_dt[dcol][-1]
        t_gpt_json['tradedate'] = "The 'Tradedate' is the date that the financial indicator is recorded."
        sgtp_json = self.gtp_json
        sgtp_json[ntablename] = t_gpt_json
        self.gtp_json = sgtp_json
        # select nextval('seqtest') as seqtest
        # print("basesql:", basesql)
        return
        if len(basic_rows) > 0:
            con, cur = getConnect()
            # cur.execute("delete from %s "%ntablename)
            cur.execute("delete from %s where tradedate='%s'" % (ntablename, vtradeDate))
            cur.executemany(basesql, basic_rows)
            print("rowcount cur:", cur.rowcount)
            wx_send.send_wx_msg_operation("%s add %s (条)[%s]" % (ntablename, cur.rowcount, vtradeDate))
            con.commit()
            cur.close()
            con.close()


    def buildMktRank(self,area):
        area_dt = {"hk": "Hong Kong", "jp": "Japan", "us": 'United States of America', 'cn': "China"}
        exchangecountry = area_dt[area]
        con, cur = dbutils.getConnect()
        d_his = "delete from newdata.mkt_rank where exchangecountry=%s"
        cur.execute(d_his, [exchangecountry])
        copy_d = "insert into  newdata.mkt_rank select  ROW_NUMBER() OVER (ORDER BY x.CompanyMarketCapitalization desc  ) AS rank,x.*" \
                 "  from (select  arb.windcode ,arb.priceclose ,mv.CompanyMarketCapitalization,arb.tradedate,arb.exchangecountry" \
                 "  from newdata.a_reuters_basic arb,newdata.a_reuters_valuation mv " \
                 "where  arb.windcode =mv.windcode and arb.exchangecountry =%s and mv.companymarketcapitalization >0  )  as x "
        cur.execute(copy_d, [exchangecountry])
        con.commit()
        cur.close()
        con.close()

    def buildFactorToCacheTable(self):
        #将所有指标数据合并放入缓存表
        bt = datetime.datetime.now()
        lcon, lcur = dbutils.getConnect()
        q_bs_sql = "select * from factorcell f where upper(f.factortable)  = upper('newdata.a_reuters_basic')"
        fbaseDF = pd.read_sql(q_bs_sql, lcon)
        fbaseDF = fmt_df_column_upper(fbaseDF)
        fbaseDF["asName"] = fbaseDF['FACTORTNAME'] + fbaseDF['FACTORNO'].astype(str)
        fbaseDF["asFactor"] = fbaseDF['FACTORTNAME'] + " " + fbaseDF["asName"]
        qcolumns = fbaseDF['asName'].values.tolist()
        bfactors = ",".join(fbaseDF['asFactor'].values.tolist())
        exe_sqls = [
            "select {qcolumns} from (select windcode ,stockcode, ric,tradedate, windcode as  windcode0,  %s from newdata.a_reuters_basic) as a0 " % bfactors]
        q_sql = "select * from factorcell f where f.factortable like '%newdata.a_reuters_%' and f.factortable not in ('newdata.a_reuters_basic')"
        fDF = pd.read_sql(q_sql, lcon)
        fDF = fmt_df_column_upper(fDF)
        factortables = fDF['FACTORTABLE'].values.tolist()
        factortables = list(set(factortables))
        qtb_dt = {}
        for qtable in factortables:
            qfdDF = fDF[fDF['FACTORTABLE'] == qtable]
            qfdDF = qfdDF.sort_values(["FACTORNO"])
            factortnames = qfdDF['FACTORTNAME'].values.tolist()
            factornos = qfdDF['FACTORNO'].values.tolist()
            qtb_dt[qtable] = {'factortnames': factortnames, "factornos": factornos}
        "select * from newdata.a_reuters_basic where a"
        exesqls = []
        for q_id, qtable in enumerate(factortables):
            qsql = "select windcode windcode%s, %s from %s "
            qt_dt = qtb_dt[qtable]
            factortnames = qt_dt['factortnames']
            factornos = qt_dt['factornos']
            asname = [str(x) + str(y) for x, y in dict(zip(factortnames, factornos)).items()]
            as_name = []
            for _idx, fname in enumerate(factortnames):
                as_name.append("%s %s" % (fname, asname[_idx]))
                qcolumns.append(asname[_idx])
            exesql = qsql % (
            q_id + 1, ",".join(as_name), qtable)  # +  'a%s on a%s.windcode=a%s.windcode \n'%(q_id+1,q_id+1,q_id)
            # exesqls.append(exesql)
            exesqls.append("(%s) as a%s\n" % (exesql, q_id + 1))
        for asidx, qs in enumerate(exesqls):
            exe_sqls.append("left join %s" % (qs))
            exe_sqls.append(" on a%s.windcode%s=a%s.windcode%s\n" % (0, 0, asidx + 1, asidx + 1))
            if "windcode%s" % asidx in qcolumns:
                qcolumns.pop(qcolumns.find("windcode%s" % asidx))
        exe_sqls = "".join(exe_sqls)
        rtqcolumns = ["windCode", "stockCode", "ric", "tradeDate"] + qcolumns
        exe_sqls = exe_sqls.format(**{"qcolumns": ",".join(rtqcolumns)})
        print("*" * 100)
        print(exe_sqls)
        dhis = "delete from  newdata.a_reuters_factors"
        copy = "insert into newdata.a_reuters_factors (%s) %s " % (",".join(rtqcolumns), exe_sqls)
        lcur.execute(dhis)
        lcur.execute(copy)
        #刷新缓存
        #-- 7(重要,存入热缓存的操作)     **把newdata.a_reuters_factors存储在pg_prewarm中，返回所占块数**
        # flushcache_sql="SELECT pg_prewarm('newdata.a_reuters_factors',mode := 'buffer',fork := 'main',first_block := null,last_block := null)"
        # lcur.execute(flushcache_sql)
        lcon.commit()
        # aly_cur.close()
        # aly_con.close()
        #
        # q_data = "select %s from newdata.a_reuters_factors" % (",".join(rtqcolumns))
        # qfsDF = pd.read_sql(q_data, lcon)
        # #
        # isql = "INSERT INTO newdata.a_reuters_factors (%s) VALUES (%s)" % (
        # ",".join(rtqcolumns), ",".join(["%s"] * len(rtqcolumns)))
        # qfsDF = qfsDF.replace({np.nan: None, np.NAN: None})
        # lcur.execute("delete from  newdata.a_reuters_factors")
        # lcur.executemany(isql, qfsDF.values.tolist())
        # lcon.commit()
        lcur.close()
        lcon.close()
        et = datetime.datetime.now()
        print(bt)
        print(et)
    def sync_financial_report (self,vtradeDate,period='year',area='us'):
        fdname = (datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")
        #同步财务报告
        if period=="year":
            sctx=r"Y:\pannel_factors\%s\%s"%(fdname,area)
        elif period=="quarter":
            sctx=r"Y:\pannel_factors_qtr\%s\%s"%(fdname,area)
        else:
            return
        pfs = os.listdir(sctx)
        cp_pfs=[str(x).strip() for x in pfs]
        con,cur = dbutils.getConnect()
        isql="""
        INSERT INTO newdata.reuters_financial_report 
        (windcode, stockcode, ric, revenuemm, revenuegrowth, grossprofitmm, grossmargin, ebitdamm, ebitdamargin, netincomemm, netmargin, eps, epsgrowth, cashfromoperationsmm, capitalexpendituresmm, freecashflowmm, perioddate, periodid, tradedate) 
        VALUES(%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        if area in ['hk']:
            qcodes = "select  WINDCODE,WINDCODE AS RIC,STOCKCODE AS TICKER  from config.STOCKINFO u where AREA = 'HK'"
        else:
            qcodes = "select * from config.reuters_usstocks u where windcode is not null"
        dsql="delete from newdata.reuters_financial_report where windcode=%s and periodid=%s"
        codeDF=pd.read_sql(qcodes,con)
        codeDF=fmt_df_column_upper(codeDF)
        for cdt in codeDF.itertuples():
            ric = str(cdt.RIC)
            windCode = cdt.WINDCODE
            stockCode = cdt.TICKER
            fname  = str(ric).replace(".","_")+".xlsx"
            isfindfile=False
            if fname in cp_pfs:
                isfindfile = True
            if not isfindfile:
                if str(ric).endswith(".O"):
                    fname = str(ric).replace(".O",".OQ").replace(".", "_") + ".xlsx"
            if fname in cp_pfs:
                fileidx=cp_pfs.index(fname)
                xlsx_filename=os.path.join(sctx,pfs[fileidx])
                ddf=pd.read_excel(xlsx_filename)
                ddf.replace([np.inf, -np.inf],np.NAN, inplace=True)
                ddf=ddf.replace({np.NAN:None})
                reportIds = ddf.columns.values.tolist()
                ddf.index=ddf["item"].values.tolist()
                ddf=ddf.T
                cols = ddf.columns.values #item	FY2	FY1	FY0	FY-1	FY-2
                items = ddf.values.tolist()
                items.pop(0)
                # for itm in cols:
                #     scn  = self.make_valid_filename(
                #         str(itm).replace("TR.", "").replace("*100", "").replace("Period", "").replace("=", "").strip())
                    # print(scn)
                ndts=[]
                #FY2	FY1	FY0	FY-1	FY-2
                periodid={"FY0":"20221231","FY1":"20231231","FY2":"20241231",
                          "FY-1":"20211231","FY-2":"20201231",}
                periodidx=reportIds[1:]
                for qidx,its in enumerate(items):
                    ndt=[windCode,stockCode,ric]
                    ndt.extend(its)
                    ndt.append(periodidx[qidx])
                    ndt.append(period)
                    ndt.append(vtradeDate)
                    ndts.append(ndt)
                cur.execute(dsql,[windCode,period])
                cur.executemany(isql,ndts)
                print("add num:",windCode,cur.rowcount)
                con.commit()
        con.commit()
        cur.close()
        con.close()
    def test_(self):
        #删除历史
        con, cur = dbutils.getConnect()
        q = "select  * from factorcell f where factortable  like '%newdata.a_reuters%' order by factorno"
        qdf = pd.read_sql(q, con)
        qdf = fmt_df_column_upper(qdf)
        q_clos = []
        for _idx, qd in enumerate(qdf.itertuples()):
            fno = str(qd.FACTORNO)
            fdesc = qd.FACTORDESC + " %s" % _idx
            # fdesc = qd.FVIEW
            fname = qd.FACTORTNAME
            q_clos.append('s.%s as "%s"' % (fname + fno, fdesc))
        qstr = ",".join(q_clos)
        qsql = "select s.windcode,s.stockcode,%s,s.tradedate from  newdata.a_reuters_factors s,portfolio.user_portfolio_list b where s.windcode=b.windcode and b.portfolioid ='self_23544' " \
               "   order by b.seqno desc  " % qstr
        print(qsql)
        dt = pd.read_sql(qsql, con)
        dt.to_excel("d:/work/temp/allfactors_1204.xlsx")
if __name__ == '__main__':
    fu = fmtutils()
    # fu.test_()
    vtradeDate = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime("%Y%m%d")
    print("vtradeDate:", vtradeDate)
    bt1=datetime.datetime.now()
    fu.modifyLastDay(vtradeDate)  # 更新最新表数据日期
    bt2=datetime.datetime.now()
    print("modifyLastDay:%s(s)"%(bt2-bt1).total_seconds())
    fu.newfactorsdataparserByArea(vtradeDate,"us")
    fu.buildMktRank("us")
    bt3=datetime.datetime.now()
    print("us:%s(s)"%(bt3-bt2).total_seconds())
    fu.newfactorsdataparserByArea(vtradeDate,"hk")
    fu.buildMktRank("hk")
    bt4=datetime.datetime.now()
    print("hk:%s(s)"%(bt4-bt3).total_seconds())
    fu.newfactorsdataparserByArea(vtradeDate,"jp")
    fu.buildMktRank("jp")
    bt5=datetime.datetime.now()
    print("jp:%s(s)"%(bt5-bt4).total_seconds())
    fu.newfactorsdataparserByArea(vtradeDate,"cn")
    fu.buildMktRank("cn")
    bt6=datetime.datetime.now()
    print("cn:%s(s)"%(bt6-bt5).total_seconds())
    fu.buildFactorToCacheTable()
    bt7 = datetime.datetime.now()
    print("buildFactorToCacheTable:%s(s)"%(bt7-bt6).total_seconds())
    #将最新日期数据同步到历史表
    fu.copyNewToHis(vtradeDate)
    bt8=datetime.datetime.now()
    print("copyNewToHis:%s(s)"%(bt8-bt7).total_seconds())
    print("end...")
    """
    select * from factorcell f  where lower(factortname) =lower('TotRevenuepctPrdtoPrdFY0');
    update factorcell set factortname ='TotRevenuepctPrdtoPrdFY0' where factorno ='28774'; 
     SELECT  'alter table  '||table_schema||'.'||table_name || ' rename column ' || column_name || ' to TotRevenuepctPrdtoPrdFY0'  ||  ';'   table_name, column_name FROM information_schema.columns where column_name=lower('TotRevenuepctPrdtoPrdFQ0'); 
     SELECT  'alter table  '||table_schema||'.'||table_name || ' rename column ' || column_name || ' to TotRevenuepctPrdtoPrdFY028774'  ||  ';'   table_name, column_name FROM information_schema.columns where column_name=lower('TotRevenuepctPrdtoPrdFQ028774'); 
       """