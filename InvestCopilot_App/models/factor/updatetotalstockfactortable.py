import psycopg2,datetime
import random
import time
import InvestCopilot_App.models.toolsutils.dbutils as dbutils
from InvestCopilot_App.models.factor.stockfactortable import *
# 源postgresql信息
ip_1 = 'www.daohequant.com'
listener_1 = '5432'
database_1 = 'Inari'
user_1 = 'dh_query'
password_1 = 'dh_query'

# 用字典存储指标表的列，可使用下面自动获取，但是为节省时间，已经固定的列就挑出来，节省一次查询时间
# source_cur.execute(f"SELECT * FROM {source_table_name} LIMIT 0")
# columns = [desc[0] for desc in source_cur.description]
# 下面dict字典，匹配列，也固定了列的位置们
dict_factor_columns = dict()
dict_factor_columns['newdata.a_reuters_analyst']= ['windcode', 'stockcode', 'ric', 'numestrevisingup', 'numestrevisingdown', 'numberofanalysts', 'meanpctchgestimatemeasurerevfy2wp60d', 'meanpctchgfy2wp60d', 'recmean', 'tradedate']
dict_factor_columns['newdata.a_reuters_ownership']=['windcode', 'stockcode', 'ric', 'ttlcmnsharesout', 'freefloatpct', 'siinstitutionalown', 'tradedate']
dict_factor_columns['newdata.a_reuters_debt']=['windcode', 'stockcode', 'ric', 'totaldebtoutstanding', 'ttldebttottlequitypct', 'cashandequivalents', 'currentratio', 'tradedate']
dict_factor_columns['newdata.a_reuters_dividend']=['windcode', 'stockcode', 'ric', 'dividendyield', 'dpssmartestntm', 'dividendpayoutratiopct', 'dividendpayoutratiopct5yravg', 'dividendpayoutratiopctttm', 'tradedate']
dict_factor_columns['newdata.a_reuters_starminecore']=['windcode', 'stockcode', 'ric', 'fwdptoepssmartestntm', 'armintracountryscore', 'eqctryrank', 'ivpricetointrinsicvaluecountryrank', 'pricemocountryrank', 'tradedate']
dict_factor_columns['newdata.a_reuters_earnings']=['windcode', 'stockcode', 'ric', 'revenueltm', 'revenuemeanfy1', 'revenuemeanfy2', 'revenueactvaluefy0', 'revenueactsurprisefq0', 'revenuefq0', 'revenuefq1', 'operatingincomeltm', 'operatingincomefy0', 'oprmeanfy1', 'oprmeanfy2', 'ebitltm', 'ebitfy0', 'ebitmeanfy1', 'ebitmeanfy2', 'ebitdaltm', 'ebitdafy0', 'ebitdameanfy1', 'ebitdameanfy2', 'netprofitactvalueltm', 'netprofitmeanfy1', 'netprofitmeanfy2', 'netprofitactvaluefy0', 'epsactvalueltm', 'epsmeanfy1', 'epsmeanfy2', 'epsactvaluefy0', 'epsactsurprisefq0', 'epsexclextradilfq0', 'epsexclextradilfq1', 'cashflowfromoperationsactvalueltm', 'capexmeanfy1', 'capexmeanfy2', 'tradedate']
dict_factor_columns['newdata.a_reuters_profitability']=['windcode', 'stockcode', 'ric', 'gpmmeanfy0', 'gpmmeanfy1', 'gpmmeanfy2', 'pcoperatingmarginpctfy0', 'oprmeanfy1revenuemeanfy1', 'oprmeanfy2revenuemeanfy2', 'ebitmarginpercentfy0', 'ebitmeanfy1revenuemeanfy1', 'ebitmeanfy2revenuemeanfy2', 'ebitdamarginpercentfy0', 'ebitdameanfy1revenuemeanfy1', 'ebitdameanfy2revenuemeanfy2', 'netprofitmarginfy0', 'netprofitmeanfy1revenuemeanfy1', 'netprofitmeanfy2revenuemeanfy2', 'roaactvaluefy0', 'roameanfy1', 'roameanfy2', 'roeactvaluefy0', 'roemeanfy1', 'roemeanfy2', 'assetturnoverfy0', 'ltdebttottleqtypctfy0', 'pcaccountsreceivableturnoverfy0', 'tradedate']
dict_factor_columns['newdata.a_reuters_growth']=['windcode', 'stockcode', 'ric', 'revenuemeanfy2revenuemeanfy11', 'revenuemeanfy1revenueactvaluefy01', 'revenuefq0revenuefq11', 'totrevenuepctprdtoprdfq0', 'totrevenue3yrcagrfy0', 'totrevenue5yrcagrfy0', 'ebitdameanfy2ebitdameanfy11', 'ebitdameanfy1ebitdafy01', 'netprofitmeanfy2netprofitmeanfy11', 'netprofitmeanfy1netprofitactvaluefy01', 'epsmeanestfwdyrgrowth', 'epsmeanestlastyrgrowth', 'epsexclextradilfq0epsexclextradilfq11', 'epsexclextradilpctprdtoprdfy0', 'ltgmean', 'capexsmartestfwdyrgrowth', 'capexsmartestlastyrgrowth', 'tradedate']
dict_factor_columns['newdata.a_reuters_trading']=['windcode', 'stockcode', 'ric', 'pricepctchg1d', 'pricepctchg1y', 'pricepctchg3y', 'pricepctchg4w', 'pricepctchgytd', 'pricepctchg10y', 'pricepctchg5y', 'price52weekhigh', 'price52weeklow', 'betathreeyearweekly', 'betafiveyear', 'tradedate']
dict_factor_columns['newdata.a_reuters_mom']=['windcode', 'stockcode', 'ric', 'totalreturn1wk', 'totalreturn1mo', 'totalreturn3mo', 'totalreturn6mo', 'totalreturn52wk', 'totalreturnytd', 'priceavg5d', 'priceavg20d', 'priceavg60d', 'price150dayaverage', 'price200dayaverage', 'priceavgnetdiff50d', 'pricerelsmapctchg200d', 'turnover', 'tradedate']
dict_factor_columns['newdata.a_reuters_basic']=['stockcode','exchangecountry','gicsindustry','gicssubindustry','gicssector','companyname','priceclose','volume','avgdailyvolume10day','tradedate']
dict_factor_columns['newdata.a_reuters_valuation']=['stockcode','ev','companymarketcapitalization','marketcapds','fwdptoepssmartestfy0','pe','fwdpentm','ptoepsmeanestfy1','ptoepsmeanestfy2','histpeg','fwdpegntm','pegfy1','pegfy2','histenterprisevaluerevenuefy0','histpricetorevenuepershareavgdilutedsharesoutltm','ptorevmeanestntm','ptorevmeanestfy1','ptorevmeanestfy2','ptobvpsactvaluefy0','pricetobvpershare','ptobpsmeanestfy1','ptobpsmeanestfy2','evtoebitda','fwdevtoebitda','fwdevtoebitdafy1','pricetocfpershare']


def insert_factorindextable(table_name,cal_time):
    if (cal_time==1):
        start_time = time.process_time()
        start_time2 =time.perf_counter()
    target_conn, target_cur = dbutils.getConnect()
    # 连接源数据库
    source_conn = psycopg2.connect(host=ip_1,database=database_1,user=user_1,password=password_1,port=listener_1)
    source_cur = source_conn.cursor()
    # 获取源表结构
    target_table_name = "totalstockfactor"  # 源表名
    source_table_name = table_name
    if table_name =='newdata.a_reuters_basic':
        # 12828条
        # windcode,stockcode,ric,exchangecountry,gicsindustry,gicssubindustry,gicssector,priceclose,volume,avgdailyvolume10day,tradedate,companyname
        columns = ['stockcode','exchangecountry','gicsindustry','gicssubindustry','gicssector','companyname','priceclose','volume','avgdailyvolume10day','tradedate']
        len_1 = len(columns)-1
        colums_sql = ','.join(columns)
        query_2 = ','.join([f'{column}=%s' for column in columns[1:len_1]])
        # 从源数据库读取表数据
        source_cur.execute(f"select {colums_sql} from {source_table_name}")
        rows = source_cur.fetchall()
        insert_values = [list(row) for row in rows]
        print(len(insert_values))
        target_cur.execute("BEGIN")
        temp_time_1 = time.perf_counter()
        target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
        for row in rows:
            stockcode = row[0]
            update_a = f"update {target_table_name} set {query_2} where windcode like '{stockcode}' ;"
            target_cur.execute(update_a , row[1:len_1])
        target_cur.execute("COMMIT")
        target_conn.commit()
        print(f"更新{table_name}表数据的锁表时间",time.perf_counter() - temp_time_1)
    else:
        if table_name in dict_factor_columns:
            # columns = dict_factor_columns[table_name]
            source_cur.execute(f"SELECT * FROM {source_table_name} LIMIT 0")
            columns = [desc[0] for desc in source_cur.description]
            len_1 = len(columns)-1
            query_2 = ','.join([f'{column}=%s' for column in columns[3:len_1]])
            colums_sql = columns[1]+','+','.join(columns[3:len_1])
            # print(colums_sql)
            # # 从源数据库读取表数据
            source_cur.execute(f"select {colums_sql} from {source_table_name}")
            rows = source_cur.fetchall()
            insert_values = [list(row) for row in rows]
            print(len(insert_values))
            # print([stock[0].split('.')[0] for stock in insert_values])
            target_cur.execute("BEGIN")
            temp_time_1 = time.perf_counter()
            target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
            for row in rows:
                target_cur.execute(f"select * from  {target_table_name} where windcode like '{row[0]}'")
                len_temp = target_cur.fetchall()
                len_temp_1 = len([list(row_ii) for row_ii in len_temp])
                if(len_temp_1>1):
                    print([list(row_ii) for row_ii in len_temp])
                update_a = f"update {target_table_name} set {query_2} where windcode like '{row[0]}';"
                target_cur.execute(update_a , row[1:len_1])
            target_cur.execute("COMMIT")
            target_conn.commit()
            print(f"更新{source_table_name}表数据的锁表时间",time.perf_counter() - temp_time_1)
    source_cur.close()
    target_cur.close()
    source_conn.close()
    target_conn.close()
    if (cal_time==1):
        print("CPU执行时间：", time.process_time()- start_time, "秒")
        print("系统显示时间：", time.perf_counter()-start_time2, "秒")

def insert_factors_totaltable():
    start_time = time.process_time()
    start_time2 =time.perf_counter()
    target_conn, target_cur = dbutils.getConnect()
    # 连接源数据库
    source_conn = psycopg2.connect(host=ip_1,database=database_1,user=user_1,password=password_1,port=listener_1)
    source_cur = source_conn.cursor()
    # 获取源表结构
    target_table_name = "totalstockfactor"  # 源表名
    target_cur.execute(f"select count(*) from pg_class where relname = '{target_table_name}';")
    if_exists_table = target_cur.fetchall()
    if if_exists_table[0][0]==0:
        source_cur.close()
        target_cur.close()
        source_conn.close()
        target_conn.close()
        insert_stockinfotable()
    else:
        # target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
        # 获取全部指标表名称
        source_cur.execute(f"select distinct factortable from factorcell f where  factortable  like '%newdata.a_reuters%';")
        factor_table_names = source_cur.fetchall()
        source_cur.close()
        target_cur.close()
        source_conn.close()
        target_conn.close()
        factor_table_names = [names[0] for names in factor_table_names]
        lens_factor_table = len(factor_table_names)
        for name in factor_table_names:
            # print(name)
            insert_factorindextable(name,0)
    print("CPU执行时间：", time.process_time()- start_time, "秒")
    print("系统显示时间：", time.perf_counter()-start_time2, "秒")

def get_null_factor_null_sql():
    target_conn, target_cur = dbutils.getConnect()
    target_cur.execute("""select column_name from information_schema.columns where table_name = 'totalstockfactor'
    and column_name not in ('windcode','stockcode','eastcode','stockname','stocktype','area',
                            'updateday','pinyin','relationcode','disabled','insertdate');""")
    factor_table_names = target_cur.fetchall()
    factor_table_names = [names[0] for names in factor_table_names]
    s = 'select count(*) from totalstockfactor where ' + ' is null and '.join(factor_table_names) + ' is null'+";"
    print(s)
    target_cur.execute(s)
    count_1 = target_cur.fetchone()[0]
    print("目前完全没有任何指标数据的股票数为:", count_1, "条")
    target_cur.close()
    target_conn.close()

def update_factorindextable(table_name,cal_time,update_totoday):
    if (cal_time==1):
        start_time = time.process_time()
        start_time2 =time.perf_counter()
    target_conn, target_cur = dbutils.getConnect()
    # 连接源数据库
    source_conn = psycopg2.connect(host=ip_1,database=database_1,user=user_1,password=password_1,port=listener_1)
    source_cur = source_conn.cursor()
    # 获取源表结构
    target_table_name = "totalstockfactor"  # 源表名
    source_table_name = table_name
    columns=list()
    len_1=0
    columns_sql=''
    query_2=''
    bool_temp = False
    if table_name in dict_factor_columns:
        columns = dict_factor_columns[table_name]
        len_1 = len(columns)-1
        query_2 = ','.join([f'{column}=%s' for column in columns[3:len_1]])
        colums_sql = columns[1]+','+','.join(columns[3:len_1])   #'stockcode'
        bool_temp = True;
    elif table_name =='newdata.a_reuters_basic':
        columns = dict_factor_columns['newdata.a_reuters_basic']
        len_1 = len(columns)-1
        colums_sql = ','.join(columns)
        query_2 = ','.join([f'{column}=%s' for column in columns[1:len_1]])
        bool_temp = True;
    if bool_temp == True:
        source_cur.execute(f"select {colums_sql} from {source_table_name} where( to_date(tradedate,'YYYYMMDD') >=(CURRENT_DATE - integer '{update_totoday}'));")
        rows = source_cur.fetchall()
        insert_values = [list(row) for row in rows]
        print("更新",len(insert_values),f"条{table_name}指标")
        target_cur.execute("BEGIN")
        temp_time_1 = time.perf_counter()
        target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
        for row in rows:
            update_a = f"update {target_table_name} set {query_2} where windcode like '{row[0]}';"
            target_cur.execute(update_a , row[1:len_1])
            target_conn.commit()
        target_cur.execute("COMMIT")
        print(f"更新{source_table_name}表数据的锁表时间",time.perf_counter() - temp_time_1)
    else:
        print(f"{source_table_name}表这{update_totoday+1}天没有更新")
    source_cur.close()
    target_cur.close()
    source_conn.close()
    target_conn.close()
    if (cal_time==1):
        print("CPU执行时间：", time.process_time()- start_time, "秒")
        print("系统显示时间：", time.perf_counter()-start_time2, "秒")

def update_factortotal_table():
    start_time = time.process_time()
    start_time2 =time.perf_counter()
    target_conn, target_cur = dbutils.getConnect()
    # 连接源数据库
    source_conn = psycopg2.connect(host=ip_1,database=database_1,user=user_1,password=password_1,port=listener_1)
    source_cur = source_conn.cursor()
    # 获取源表结构
    target_table_name = "totalstockfactor"  # 源表名
    target_cur.execute(f"select count(*) from pg_class where relname = '{target_table_name}';")
    if_exists_table = target_cur.fetchall()
    if if_exists_table[0][0]==0:
        source_cur.close()
        target_cur.close()
        source_conn.close()
        target_conn.close()
        insert_factors_totaltable()
    else:
        # target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
        # 获取全部指标表名称
        source_cur.execute(f"select distinct factortable from factorcell f where  factortable  like '%newdata.a_reuters%';")
        factor_table_names = source_cur.fetchall()
        factor_table_names = [names[0] for names in factor_table_names]
        lens_factor_table = len(factor_table_names)
        source_cur.close()
        target_cur.close()
        source_conn.close()
        target_conn.close()
        for name in factor_table_names:
            # print(name)
            update_factorindextable('newdata.a_reuters_trading',1,0)
    print("CPU执行时间：", time.process_time()- start_time, "秒")
    print("系统显示时间：", time.perf_counter()-start_time2, "秒")

if __name__ == '__main__':
    choice_for_factor_test=4
    # 初次插入指标数据使用insert_factors_totaltable()
    # 每日更新使用update_factortotal_table()

    # cal_time参数为1显示，时间，为0不显示时间

    # 1.单指标表更新指标测试/非根据指标更新时间的日常更新,用于单指标表测试
    if choice_for_factor_test==1:
        # 下面均可单独运行
        # insert_factorindextable('newdata.a_reuters_basic', 1)
        # insert_factorindextable('newdata.a_reuters_mom', 1)
        # insert_factorindextable('newdata.a_reuters_debt', 0)
        insert_factorindextable('newdata.a_reuters_valuation', 0)
    #     输出： 第一行，该指标表共多少数据能和stockinfo匹配
    #           第二行更新的锁表时间，实际时间

    # 2.初次更新全部指标表的测试，12张表的
    elif choice_for_factor_test==2:
        insert_factors_totaltable()

    # 数据完全没有任何一个指标数据的股票的总数
    elif choice_for_factor_test==3:
        get_null_factor_null_sql()

    # 日常单指标表更新数据，update_totoday参数用于表明，要更新最近几天的指标，
    # 等于0，更新今日更新的指标，等于1，更新近两天的，以此类推
    elif choice_for_factor_test==4:
        update_factorindextable('newdata.a_reuters_trading', 1, 0)

    elif choice_for_factor_test==5:
        update_factortotal_table()


    # choice_for_factor_test==4
    # 输出如：
    # 更新    12835
    # 条newdata.a_reuters_trading指标
    # 更新newdata.a_reuters_trading表数据的锁表时间    1018.9148347000009
    # CPU执行时间： 2.3125    秒
    # 系统显示时间： 1055.6830718999845    秒







