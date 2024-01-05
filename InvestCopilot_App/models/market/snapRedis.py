# coding=utf-8
import redis
import datetime
import traceback
import pickle
import time

#缓存有效期
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10
#缓存链接串
HOST="127.0.0.1"
PORT=6379
DB=3
class cacheTools():
    """
    redis缓存连接处
    """
    __pool=None

    def __new__(cls,decode_responses=True,*args, **kw):
        if not cls.__pool:
            #https://www.jianshu.com/p/b9b9282103dd
            # r=redis.Redis(host=HOST,port=PORT,db=DB,password='daoheRedis6379')
            #多个数据源
            pool=redis.ConnectionPool(host=HOST,port=PORT,db=DB,max_connections=1000,
                                      password='daoheRedis6379',decode_responses=decode_responses)#,decode_responses=True
            cls.__pool=redis.StrictRedis(connection_pool=pool)
        return cls.__pool

import pandas as pd
if __name__ == '__main__':
    r=cacheTools(decode_responses=False)
    print("redis:",r)
    # print(redis.get("xxx"))

    keys = r.keys()
    print(keys)

    print(r.get("xxx"))

    df_bytes_from_redis = r.get("east_cn_realMarketDF")
    baseDF = pickle.loads(df_bytes_from_redis)
    print(len(baseDF),baseDF.columns.tolist())
    print(baseDF[baseDF['STOCKCODE']=='600519.SH'])


    time.sleep(100000)
    time.sleep(100000)
    #字符串
    #哈希
    r.hset(name='my',key='age',value=10)
    r.hset(name='my',key='sex',value='男')
    r.hset(name='my',key='name',value='张三')

    #列表 list lpush ，rpush ， blpop lpop
    r.delete("site-list")
    r.lpush("site-list",'a')
    r.lpush("site-list",'b')
    r.lpush("site-list",'c')
    l=r.lrange("site-list",0,-1)
    print("l:",l)
    x=r.lpop("site-list")#移出并获取列表的第一个元素
    print(x)
    x=r.rpop("site-list")#移出并获取列表的最后一个元素
    print(x)
    l=r.lrange("site-list",0,-1)

    #set
    r.delete("site-set*")
    r.sadd("site-set1",'a')
    r.sadd("site-set1",'b')
    r.sadd("site-set1",'b')
    r.sadd("site-set1",'c')
    l=r.smembers("site-set1")#返回集合中的所有成员
    print("set1:",l)

    #删除集合
    print("smembers:",r.delete("site-set"))

    r.sadd("site-set2",'a')
    r.sadd("site-set2",'f')
    r.sadd("site-set2",'g')
    l=r.smembers("site-set2")#返回集合中的所有成员
    print("set2:",l)

    #交集 sinter，差集：sdiff(x,y)，并集:sunion
    sdiff=r.sdiff("site-set1","site-set2")
    print("sdiff:",sdiff)
    #并集:sunion
    sunion=r.sunion("site-set1","site-set2")
    print("sunion:",sunion)

    #交集:sinter
    sinter=r.sinter("site-set1","site-set2")
    print("sinter:",sinter)

    print(r.hget("my",'age'))
    print(r.hgetall('my'))
    print(r.hkeys('my'))
    print(r.hvals('my'))

    # for x in r.lrange("site-list",0,-1):
    #     print(x)

    for x,v in r.hgetall('my').items():
        print(x,v)
    # for x in r.hvals('my'):
    #     print(x.decode('utf-8'))

    #事务
    multi = r.pipeline()
    multi.set('k1','v1')
    multi.execute()
    # 悲观锁
    with r.pipeline() as pipe:
        while 1:
            try:
                # 关注一个key 如果标记的键在事务处理前如果被别人修改过，那事务会处理失败，需再次重试！
                pipe.watch('stock_count')
                # time.sleep(10)
                count = int(r.get('stock_count'))
                if count > 0:  # 有库存
                # 事务开始
                    pipe.multi()
                    pipe.set('stock_count', count - 1)
                    # 事务结束
                    pipe.execute()
                    # 把命令推送过去
                    stock_count=r.get("stock_count")
                    print("stock_count:",stock_count)
                else:
                    #放弃修改
                    pipe.unwatch()

                break
            except Exception:
                traceback.print_exc()

                continue

    #主从复制： 主写，从读
    # SQL_StockMarketDF=cache.redis.get('SQL_StockMarketDF')
    # SQL_StockMarketDF = pickle.loads(SQL_StockMarketDF)
    # # print("SQL_StockMarketDF:",SQL_StockMarketDF)
    # rs =SQL_StockMarketDF[SQL_StockMarketDF['STOCKCODE']=='600519.SH'].values.tolist()
    # print(rs)
    # rs = cache.redis.keys("*SQL_StockMarketDF")
    # print("key:",rs)
    # SQL_StockMarketDF=cache.redis.get(rs[0])
    # SQL_StockMarketDF = pickle.loads(SQL_StockMarketDF)
    # rs =SQL_StockMarketDF[SQL_StockMarketDF['STOCKCODE']=='600519.SH'].values.tolist()
    # # cache.redis.delete(*rs)
    # # print(SQL_StockMarketDF)
    # print(rs)
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
