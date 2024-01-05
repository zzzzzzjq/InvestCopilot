import pandas as pd
import datetime
import traceback
import sys
sys.path.append('../../../..')
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.toolsutils import dbmongo
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
def expNewsImport():
    #新闻重要性标记sql
    q_sql="select s.sid,s.slike ,s.updatetime , u.username ,c2.companyname  from business.summary_like s ,tusers u , companyuser c,company c2 " \
          "where s.userid=u.userid and u.userid =c.userid  and c.companyid =c2.companyid" \
          "  and slike  in ('1','2','3','4','5') and s.userid not in ('1967','73','72')  order by updatetime desc "
    q_sql="""
      select b.sid, STRING_AGG(b.slike, ', ') AS slike, STRING_AGG(b.unc, ', ') AS usercompany  from (
                   select s.sid,s.slike, u.userrealname || '/' || c2.companyname as unc   from business.summary_like s ,tusers u , companyuser c,company c2
          where s.userid=u.userid and u.userid =c.userid  and c.companyid =c2.companyid
            and slike  in ('1','2','3','4','5') and s.userid not in ('1967','73','72')  ) b  
				GROUP BY
				b.sid;
    """
    try:
        con,cur = dbutils.getConnect()
        newdf=pd.read_sql(q_sql,con)
        slike_dt={s.sid :s for s in newdf.itertuples()}
        newids = newdf['sid'].values.tolist()
        import collections
        s = collections.Counter(newids)
        print("ss:",s)
        page=1
        pageSize=100000
        dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
        if not dbRst.errorFlag:
            dbRst.vsummaryDatas = []
            return dbRst
        infomationDBs = dbRst.information_datas
        rtdata_d = []
        # 按集合进行分类
        qsets = {}
        for dbs in infomationDBs:  # 按数据库 对集合进行合并
            dataBase = dbs['website']
            dbName = dbs['dbset']
            datatype = dbs['datatype']
            if datatype not in ['news']:
                continue
            if dataBase in qsets:
                fqsets = qsets[dataBase]
                dbsets = fqsets["dbsets"]
                dbsets.append(dbName)
            else:
                qsets[dataBase] = {"website": dataBase, "dbsets": [dbName]}
        # print("tickers:",tickers)
        # print("qsets:",qsets)
        translation="zh"
        querys = {"publishOn": {"$gt": "2023-12-15 00:00:00"}, "tickers": "ORCL.N"}
        querys = {"publishOn": {"$gt": "2023-12-15 22:53:00"}}
        querys = {"id": {"$in": newids}}
        projection = {"_id": 0, "id": 1, "title": 1, "title_zh": 1, "title_en": 1, "publishOn": 1, "source": 1,
                      "core_reason": 1,
                      "total_score": 1,
                      # "updateTime": 1, "show_total_score": 1, "title_cores.%s" % titleTag: 1, "news_class": 1}
                      "insertTime": 1, "updateTime": 1, "show_total_score": 1, "title_display": 1, "title_cores": 1,
                      "news_class": 1, "summaryText": 1}

        # 定义排序条件
        sort = {"updateTime": -1}  # 重要性总分
        skip = (page - 1) * pageSize
        for website, website_dt in qsets.items():
            dbsets = website_dt['dbsets']
            # print("dataBase:",dataBase)
            dblist = [{"$unionWith": dbs} for dbs in dbsets]
            dblist.append({"$match": querys})  # 过滤符合条件的文档
            dblist.append({"$sort": sort})  # 根据排序条件排序
            dblist.append({"$skip": skip})  # 跳过文档
            dblist.append({"$limit": pageSize})  # 限制结果数量
            dblist.append({"$project": projection})  # 筛选要显示的字段

            with dbmongo.Mongo(website) as md:
                mydb = md.db
                contentsSets = list(mydb.collection_name.aggregate(dblist))
                for s in contentsSets:
                    if translation in ['zh']:
                        if 'title_zh' in s:  # 中文标题
                            s['title'] = s['title_zh']
                    elif translation in ['en']:
                        if 'title_en' in s:  # 英文标题
                            s['title'] = s['title_en']
                    if  "news_class" not in s:
                        continue
                    # nrow = [s['id'], s['title'], s['summaryText'], s['news_class'], s['total_score']]
                    #
                    # nrow.append(s['title_cores'])
                    # nrow.append(s['title_display'])
                    # nrow.append(s['updateTime'])
                    s["收集重要性"] = slike_dt[s['id']].slike
                    s["用户公司信息"] = slike_dt[s['id']].usercompany
                    rtdata_d.append(s)
                    # for x in rtdata_d:
                    #     print("x:",x)
                    # pd.DataFrame(rtdata_d).to_excel("d:/work/temp/news_score_%s.xlsx"%("ORCL"))
                print("nrow", len(rtdata_d[0]))
                print("nrow", rtdata_d[0])

                outdf = pd.DataFrame(rtdata_d)
                outdf = outdf.replace({0: None})
                outdf.to_excel("d:/work/temp/新闻重要性_采集_%s.xlsx" % (datetime.datetime.now().strftime("%Y-%m-%d")))
    except:
        print(traceback.format_exc())
def expNewsImport2():
    #新闻重要性标记sql
    q_sql="select s.sid,s.slike ,s.updatetime , u.username ,c2.companyname  from business.summary_like s ,tusers u , companyuser c,company c2 " \
          "where s.userid=u.userid and u.userid =c.userid  and c.companyid =c2.companyid  and slike  in ('1','2','3','4','5') order by updatetime desc "
    try:
        con,cur = dbutils.getConnect()
        newdf=pd.read_sql(q_sql,con)

        newids = newdf['sid'].values.tolist()
        page=1
        pageSize=100000
        dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
        if not dbRst.errorFlag:
            dbRst.vsummaryDatas = []
            return dbRst
        infomationDBs = dbRst.information_datas
        rtdata_d = []
        # 按集合进行分类
        qsets = {}
        for dbs in infomationDBs:  # 按数据库 对集合进行合并
            dataBase = dbs['website']
            dbName = dbs['dbset']
            datatype = dbs['datatype']
            if datatype not in ['news']:
                continue
            if dataBase in qsets:
                fqsets = qsets[dataBase]
                dbsets = fqsets["dbsets"]
                dbsets.append(dbName)
            else:
                qsets[dataBase] = {"website": dataBase, "dbsets": [dbName]}
        # print("tickers:",tickers)
        # print("qsets:",qsets)
        translation="zh"
        querys = {"publishOn": {"$gt": "2023-12-15 00:00:00"}, "tickers": "ORCL.N"}
        querys = {"publishOn": {"$gt": "2023-12-15 22:53:00"}}
        querys = {"id": {"$in": newids}}
        projection = {"_id": 0, "id": 1, "title": 1, "title_zh": 1, "title_en": 1, "publishOn": 1, "source": 1,
                      "core_reason": 1,
                      "total_score": 1,
                      # "updateTime": 1, "show_total_score": 1, "title_cores.%s" % titleTag: 1, "news_class": 1}
                      "insertTime": 1, "updateTime": 1, "show_total_score": 1, "title_display": 1, "title_cores": 1,
                      "news_class": 1, "summaryText": 1}
        projection = {"_id": 0, "id": 1,  "title_zh": 1,  "publishOn": 1, "source": 1,
                      "core_reason": 1,
                      "total_score": 1, "updateTime": 1,  "title_display": 1, "title_cores": 1,
                      "news_class": 1, "summaryText": 1}

        # 定义排序条件
        sort = {"updateTime": -1}  # 重要性总分
        skip = (page - 1) * pageSize
        news_dt={}
        for website, website_dt in qsets.items():
            dbsets = website_dt['dbsets']
            # print("dataBase:",dataBase)
            dblist = [{"$unionWith": dbs} for dbs in dbsets]
            dblist.append({"$match": querys})  # 过滤符合条件的文档
            dblist.append({"$sort": sort})  # 根据排序条件排序
            dblist.append({"$skip": skip})  # 跳过文档
            dblist.append({"$limit": pageSize})  # 限制结果数量
            dblist.append({"$project": projection})  # 筛选要显示的字段

            with dbmongo.Mongo(website) as md:
                mydb = md.db
                contentsSets = list(mydb.collection_name.aggregate(dblist))
                for s in contentsSets:
                    news_dt[s["id"]]=s
        rtdata_d=[]
        for _i,r in newdf.iterrows():
            rs = dict(r)
            rs.update(news_dt[r.sid])
            rtdata_d.append(rs)

        outdf = pd.DataFrame(rtdata_d )
        outdf = outdf.replace({0: None})
        outdf.to_excel("d:/work/temp/新闻重要性采集_%s.xlsx" % (datetime.datetime.now().strftime("%Y-%m-%d")))
    except:
        print(traceback.format_exc())

def fillStockName():
    from InvestCopilot_App.models.toolsutils import dbmongo

    con, cur = dbutils.getConnect()
    codeDF = pd.read_sql("select * from config.stockinfo where area='AM'",con)
    # codeDF = pd.read_sql("select * from config.reuters_usstocks ru where windcode  is not null",con)
    codedt = {x.windcode: x for x in codeDF.itertuples()}
    cur.close()
    con.close()
    with dbmongo.Mongo("StockPool") as db:
        mydb = db.db
        sset =mydb['stocklist']
        flst = sset.find({"bbgName":{"$exists":False}})
        for fin in flst:
            fwindcode=fin['windCode']
            if fwindcode in codedt:
                ct = codedt[fwindcode]
                symbol=(str(fwindcode).split(".")[0] )
                rts=sset.update_one({'windCode':fwindcode},{"$set":{"bbgcode": symbol+" US EQUITY",'bbgName':ct.stockname }})
                print(rts.modified_count)
            else:
                print(fin)
if __name__ == '__main__':
    expNewsImport()
    # expNewsImport2()
    # fillStockName()
    pass