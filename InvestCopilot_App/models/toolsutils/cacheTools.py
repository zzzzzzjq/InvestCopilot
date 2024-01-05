
import redis
import datetime
import pickle

#缓存有效期
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10
#缓存链接串
HOST="127.0.0.1"
PORT=6379
DB=1

class cacheTools():
    def __init__(self):
        r=redis.Redis(host=HOST,port=PORT,db=DB,password='Gfjj0.159753+')
        self.redis=r

    def getAllKeys(self):
        #所有key
        keys = self.redis.keys()
        return keys

    def getExpiredKeys(self):
        #过期key
        rd=self.redis
        rs = self.getAllKeys()
        now = datetime.datetime.now()
        expiredKeys=[]
        for key in rs :
            valiTime = rd.ttl(key)
            if valiTime is None:
                #永久有效
                nowT =now + datetime.timedelta(seconds=100000000)
            else:
                #当前时间加有效期时间，单位秒
                nowT =now + datetime.timedelta(seconds=valiTime)

            afterT=now+datetime.timedelta(seconds=CACHEUTILS_UPDATE_SECENDS)
            nowD=nowT.strftime("%Y%m%d")
            afterD=afterT.strftime("%Y%m%d")
            if nowD < afterD:
                expiredKeys.append("key[%s]已过期，失效时间[%s]，key失效时间[%s]" % (key,afterT,nowT))

        return expiredKeys


import pandas as pd
if __name__ == '__main__':
    # con=db.connect()
    # print(con)
    cache=cacheTools()
    # SQL_StockMarketDF=cache.redis.get('SQL_StockMarketDF')
    # SQL_StockMarketDF = pickle.loads(SQL_StockMarketDF)
    # # print("SQL_StockMarketDF:",SQL_StockMarketDF)
    # rs =SQL_StockMarketDF[SQL_StockMarketDF['STOCKCODE']=='600519.SH'].values.tolist()
    # print(rs)
    rs = cache.redis.keys("*SQL_StockMarketDF")
    print("key:",rs)
    SQL_StockMarketDF=cache.redis.get(rs[0])
    SQL_StockMarketDF = pickle.loads(SQL_StockMarketDF)
    rs =SQL_StockMarketDF[SQL_StockMarketDF['STOCKCODE']=='600519.SH'].values.tolist()
    # cache.redis.delete(*rs)
    # print(SQL_StockMarketDF)
    print(rs)
    # rs=cache.getExpiredKeys()
    # for x in rs :
    #     print(x)
    # rs = cache.getAllKeys()
    # print(rs)
    # q_s="select * from workday"
    # # import
    # pd.read_sql(q_s,)
    # dump = pickle.dumps("data")
    # cache.redis.set("",dump)
    pass
