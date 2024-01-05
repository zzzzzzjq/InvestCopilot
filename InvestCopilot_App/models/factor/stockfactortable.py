import psycopg2,datetime
import random
import time
import InvestCopilot_App.models.toolsutils.dbutils as dbutils

# 源postgresql信息
ip_1 = 'www.daohequant.com'
listener_1 = '5432'
database_1 = 'Inari'
user_1 = 'dh_query'
password_1 = 'dh_query'

def create_totalstockfactortable():
    # 或者在python文件执行，即执行下面
    conn, cur = dbutils.getConnect()
    create_stockfactor_table_query = """
    CREATE TABLE if not exists TOTALSTOCKFACTOR(
        STOCKCODE     VARCHAR(50),             -- 股票代码
        windcode      varchar(20),
        eastcode      varchar(20),
        STOCKNAME     VARCHAR(100),            -- 股票名称
        stocktype     varchar(10),
        area          varchar(10),
        updateday     varchar(8),
        pinyin        varchar(200),
        relationcode  VARCHAR(50),  
        disabled      bpchar(1),
        INSERTDATE    TIMESTAMP,               -- 添加日期
        PRIMARY KEY(STOCKCODE)    -- 主键(组合ID，股票代码)
    );"""
    cur.execute(create_stockfactor_table_query)
    conn.commit()

    delete_factor_redundant_name = True

    cur.execute(
        "SELECT * FROM information_schema.columns  WHERE table_name = 'totalstockfactor' and column_name not in ('pinyin', 'eastcode', 'updateday', 'disabled', 'windcode', 'stockcode', 'stocktype', 'insertdate', 'relationcode', 'area', 'stockname');")
    factor_names_1 = cur.fetchall()
    factor_names_list_1 = [names_i[3].lower() for names_i in factor_names_1]
    before_total_factor_list = set(factor_names_list_1)
    # 建表不一样，inari的指标表都是小写的，我需要换成小写字母建表，才能查询
    cur.execute("select distinct factortname from factorcell;")
    factor_names = cur.fetchall()
    factor_names_list = [names_i[0].lower() for names_i in factor_names]
    after_total_factor_list = set(factor_names_list)
    delete_fator_columns = before_total_factor_list - after_total_factor_list
    add_factor_columns = after_total_factor_list - before_total_factor_list
    # # change1106更改指标名称后和源表设置一致，删除本表中多余的指标
    if (delete_factor_redundant_name and len(delete_fator_columns) > 0):
        for column_name in delete_fator_columns:
            cur.execute(f"ALTER TABLE TOTALSTOCKFACTOR DROP COLUMN IF EXISTS {column_name} CASCADE ;")
    if (len(add_factor_columns) > 0):
        for column_name in add_factor_columns:
            cur.execute(f"ALTER TABLE TOTALSTOCKFACTOR ADD COLUMN IF NOT EXISTS {column_name} NUMERIC(50,6);")
        str_temp = """alter table totalstockfactor alter column exchangecountry type varchar(100);
                    alter table totalstockfactor alter column gicsindustry type varchar(100);
                    alter table totalstockfactor alter column gicssubindustry type varchar(100);
                    alter table totalstockfactor alter column exchangecountry type varchar(150);
                    alter table totalstockfactor alter column gicssector type varchar(50);
                    alter table totalstockfactor alter column companyname type varchar(150);"""
        for str_i in str_temp.split(";")[:6]:
            cur.execute(str_i)
    select_1_query = """
        SELECT COUNT(*) FROM information_schema.columns  WHERE table_name = 'totalstockfactor';
        """
    cur.execute(select_1_query)
    conn.commit()
    column_count = cur.fetchone()[0]
    print("目前该表共", str(column_count), "列")
    cur.close()
    conn.close()


# insert_stockinfotable先把股票信息都存入，表不存在会自动创建表插入数据，
# 表存在会更新数据，新增插入，删除的删除，修改的更新
def insert_stockinfotable():
    start_time = time.process_time()
    start_time2 =time.perf_counter()
    target_conn, target_cur = dbutils.getConnect()
    # 连接源数据库
    source_conn = psycopg2.connect(host=ip_1,database=database_1,user=user_1,password=password_1,port=listener_1)
    source_cur = source_conn.cursor()
    # 获取源表结构
    target_table_name = "totalstockfactor"  # 源表名
    source_table_name = "stockinfo"  # 目标表名
    target_cur.execute(f"select count(*) from pg_class where relname = '{target_table_name}';")
    if_exists_table = target_cur.fetchall()
    if if_exists_table[0][0]==0:
        create_totalstockfactortable()
        time_insert = [datetime.datetime.now()]
        source_cur.execute(f"SELECT * FROM {source_table_name} LIMIT 0")
        columns = [desc[0] for desc in source_cur.description]
        # 从源数据库读取表数据
        source_cur.execute(f"select * from {source_table_name}")
        rows = source_cur.fetchall()
        insert_values = [list(row)+time_insert for row in rows]
        # print(insert_values[0])
        target_cur.execute("BEGIN")
        temp_time_1 = time.perf_counter()
        target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
        target_insert_sql = f"INSERT INTO {target_table_name} VALUES ({', '.join(['%s'] * (len(columns)+1))})"
        target_cur.executemany(target_insert_sql, insert_values)
        target_cur.execute("COMMIT")
        target_conn.commit()
        print("插入全部stockinfo数据锁表时间",time.perf_counter() - temp_time_1)
    else :
        source_cur.execute(f"SELECT * FROM {source_table_name} LIMIT 0")
        columns = [desc[0] for desc in source_cur.description]
        query_1 = ', '.join(['%s'] * len(columns))
        colums_sql = ','.join(columns)
        source_cur.execute(f"select * from {source_table_name};")
        rows_s1 = source_cur.fetchall()
        rows_s1 = set(rows_s1)
        target_cur.execute(f"SELECT stockcode,windcode,eastcode,stockname,stocktype,area,updateday,pinyin,relationcode,disabled from {target_table_name}")
        rows_t1 = target_cur.fetchall()
        rows_t1 = set(rows_t1)
        # diff_rows = rows_t1.symmetric_difference(rows_s1)
        delete_rows = rows_t1-rows_s1
        add_rows = rows_s1-rows_t1
        if(len(delete_rows)>0 or len(add_rows)>0):
            try:
                temp_time_1 =time.perf_counter()
                # 开始事务
                target_cur.execute("BEGIN")
                # 锁定目标表
                target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
                # 删除目标表与源表不一致的地方，包括多余的和修改的
                if len(delete_rows)>0:
                    print(f"删除{len(delete_rows)}条")
                    if(len(delete_rows)==1):
                        delete_id = delete_rows.pop()[0]
                        print(delete_id)
                        target_cur.execute(f"delete from {target_table_name} where stockcode = {delete_id}::varchar;")
                    else:
                        delete_id = tuple(rows[0] for rows in delete_rows)
                        print(delete_id)
                        target_cur.execute(f"delete from {target_table_name} where stockcode in {delete_id};")
                if len(add_rows)>0:
                    print(f"新增{len(add_rows)}条")
                    # addorupdate_id = [rows[0] for rows in add_rows]
                    for row in add_rows:
                        target_cur.execute(f"insert into {target_table_name} ({colums_sql}) values ({query_1}) ;", row)
            except Exception as e:
                # 回滚事务
                target_conn.rollback()
                print("更新失败:", str(e))
            finally:
                # 释放加锁
                target_cur.execute("COMMIT")
                target_conn.commit()
                print("更新stockinfo数据锁表时间",time.perf_counter() - temp_time_1)
    source_cur.close()
    target_cur.close()
    source_conn.close()
    target_conn.close()
    print("CPU执行时间：", time.process_time()- start_time, "秒")
    print("系统显示时间：", time.perf_counter()-start_time2, "秒")

def insert_stockinfotable2():
    start_time = time.process_time()
    start_time2 =time.perf_counter()
    target_conn, target_cur = dbutils.getConnect()
    # 连接源数据库
    source_conn = psycopg2.connect(host=ip_1, database=database_1, user=user_1, password=password_1,port=listener_1)
    source_cur = source_conn.cursor()
    # 获取源表结构
    target_table_name = "totalstockfactor"  # 源表名
    source_table_name = "stockinfo"  # 目标表名
    target_cur.execute(f"select count(*) from pg_class where relname = '{target_table_name}';")
    if_exists_table = target_cur.fetchall()
    if if_exists_table[0][0]==0:
        create_totalstockfactortable()
        time_insert = [datetime.datetime.now()]
        source_cur.execute(f"SELECT * FROM {source_table_name} LIMIT 0")
        columns = [desc[0] for desc in source_cur.description]
        # 从源数据库读取表数据
        source_cur.execute(f"select * from {source_table_name}")
        rows = source_cur.fetchall()
        insert_values = [list(row)+time_insert for row in rows]
        # print(insert_values[0])
        target_cur.execute("BEGIN")
        temp_time_1 = time.perf_counter()
        target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
        target_insert_sql = f"INSERT INTO {target_table_name} VALUES ({', '.join(['%s'] * (len(columns)+1))})"
        target_cur.executemany(target_insert_sql, insert_values)
        target_cur.execute("COMMIT")
        target_conn.commit()
        print("插入全部stockinfo数据锁表时间",time.perf_counter() - temp_time_1)
    else :
        try:
            source_cur.execute(f"SELECT * FROM {source_table_name} LIMIT 0")
            columns = [desc[0] for desc in source_cur.description]
            query_1 = ', '.join(['%s'] * len(columns))
            colums_sql = ','.join(columns)
            source_cur.execute(f"select * from {source_table_name} where( to_date(updateday,'YYYYMMDD') >=(CURRENT_DATE - integer '1'));")
            rows = source_cur.fetchall()
            insert_values = [list(row) for row in rows]
            print("这两天更新了股票信息：",len(insert_values),"条")
            # update_query = update_query%rows[0][1:]
            target_cur.execute("BEGIN")
            temp_time_1 = time.perf_counter()
            target_cur.execute(f"LOCK TABLE {target_table_name} IN EXCLUSIVE MODE")
            for row in rows:
                update_query =''
                for column,row1 in zip(columns[1:],row[1:]):
                    if row1!=None :
                        update_query +=f"""{column}='{row1}',"""
                    else :
                        update_query +=f"{column}=null,"
                update_query = update_query[:len(update_query)-1]
                target_cur.execute(f"insert into {target_table_name} ({colums_sql}) values ({query_1}) on conflict(stockcode) do update SET {update_query} ;", row)
            target_cur.execute("COMMIT")
            target_conn.commit()
            print("更新最近两天的stockinfo数据的锁表时间",time.perf_counter() - temp_time_1)
        except Exception as e:
            # 回滚事务
            target_conn.rollback()
            print("更新失败:", str(e))
        finally:
            # 释放加锁
            target_cur.execute("COMMIT")
    source_cur.close()
    target_cur.close()
    source_conn.close()
    target_conn.close()
    print("CPU执行时间：", time.process_time()- start_time, "秒")
    print("系统显示时间：", time.perf_counter()-start_time2, "秒")


if __name__ == '__main__':
    try_test_choice=3
    if try_test_choice==1:
        # 创建股票指标表表格，并输出表格列数，
        # 初始这步没有数据的，如果表存在也没关系，会跳过。
        create_totalstockfactortable()
        time.sleep(3)
    elif try_test_choice==2:
        # first_work
        # 初始使用insert_stockinfotable()插入全部stockinfo表
        insert_stockinfotable() #会自动调用create_totalstockfactortable()
        time.sleep(3)
    elif try_test_choice==3:
        # daily_work
        # 日常使用insert_stockinfotable2()更新最近两天的数据，记得去调用指标插入函数，不然只有股票信息，没有指标信息
        insert_stockinfotable2()
        time.sleep(1)
    elif try_test_choice==4:
        # # weekly_work
        # 每周运行一次insert_stockinfotable()删除不一致的数据
        insert_stockinfotable()
        time.sleep(1)



