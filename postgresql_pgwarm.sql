-- 缓存newdata.a_reuters_factors表,最好用postgres超级管理员账号
-- 下面是postgresql缓存表设置的相关代码

-- 1 查询表的大小
-- VACUUM FULL public.userportfoliostock;
SELECT count(*) FROM newdata.a_reuters_factors;
SELECT pg_size_pretty(pg_total_relation_size('newdata.a_reuters_factors')); --25MB

-- 2 引入缓存表pg_prewarm
create extension if not exists pg_prewarm;
-- 当一个扩展被标记为 extrelocatable 时，意味着它可以以二进制的形式被复制到其他目录中，而无需重新编译该扩展。
SELECT * FROM pg_extension WHERE extname = 'pg_prewarm';
-- 创建其他扩展
create extension if not exists pg_buffercache; -- # 提供了一种查看缓冲区缓存的内容的方法。
-- 查看其他扩展
SELECT * FROM pg_extension;
SELECT count(*) FROM pg_buffercache;  -- 16384 ,每块是8kb 524288
SELECT * FROM pg_buffercache;
-- bufid：缓冲区中数据块的标识符。
-- relfilenode：与数据块关联的表的文件节点标识符。
-- reltablespace：包含表的表空间的标识符。
-- relforknumber：存储数据块的关系分支（如主数据分支、索引分支）的标识符。
-- relblocknumber：数据块在关系分支中的块编号。
-- isdirty：指示数据块是否被修改（脏数据）。
-- usagecount：数据块的使用计数。
-- refcount：数据块的引用计数。
-- pincount：数据块的固定计数。
-- hits：只读情况下，数据块被访问的次数。
-- reads：从磁盘读取数据块的次数。
-- writes：将数据块写入磁盘的次数'

-- 3 获取整个共享缓冲区的大小,
-- 一般postgresql shared_buffers为128MB，共16384 个8kb的块
SHOW shared_buffers; --128MB windows服务器有用范围是64MB到512MB，默认128MB
-- 运行下面这句看 shared_buffers;加载的位置，是configure file可以不alter system，
select * from pg_settings where name='shared_buffers';

-- 4 下面已设置了.
-- **修改postgresql缓存表大小**
-- 1. 打开 `postgresql.conf` 文件，PostgreSQL 安装目录下的 `data` 文件夹中。
-- 2. 搜索 `shared_buffers` 参数，并将其设置为你想要的值。不加"MB,GB"单位会当作块数
--         effective_cache_size也改一下
-- 3. 保存并关闭 `postgresql.conf` 文件
-- postgresql.auto.conf 文件中的更改优先于 postgresql.conf 文件中的设置
ALTER SYSTEM SET shared_buffers = '4GB';
-- ALTER SYSTEM SET shared_buffers = '512MB';
SELECT pg_reload_conf();
-- 5. 重启postgresql， 运行SELECT pg_reload_conf();但最好是重启postgresql，很快的
-- 6. 查看shared_buffers是否是自己想要的
SHOW shared_buffers; --128MB windows服务器有用范围是64MB到512MB，默认128MB
-- 运行下面这句看 shared_buffers加载的位置，是configure file可以不alter system，最好都运行一下
select * from pg_settings where name='shared_buffers';

-- 5 查看其他的设置
-- effective_cache_size 也在 postgresql.conf 文件，改大一点
SHOW effective_cache_size; --4GB 提示查询优化器估计系统缓存的大小,设置值大点，可以让优化器更倾向使用索引扫描而不是顺序扫描
SHOW maintenance_work_mem; --64MB 设置的用于维护操作（如索引创建、整理等）的工作内存的大小
SHOW work_mem; --4mb 设置的每个查询运行时可用于操作内存的大小
show wal_buffers; --4MB,还没写入磁盘的WAL(预写入日志)数据的共享内存量
-- SET work_mem='4MB';  --work_mem是每单个连接用户使用的内存，也就是实际需要的内存为max_connections * work_mem

-- 6 查看newdata.a_reuters_factors表在缓存中的块数
select count(*) from pg_buffercache where relfilenode = (select relfilenode from pg_class where relname = 'a_reuters_factors');
-- 查看a_reuters_factors在那些具体的块
select * from pg_class;
select * from pg_buffercache where relfilenode = (select relfilenode from pg_class where relname = 'a_reuters_factors');
-- 重启postgresql，清除了缓存，下面设置了重启postgresql不会再清除缓存

-- 7(重要,存入热缓存的操作)     **把newdata.a_reuters_factors存储在pg_prewarm中，返回所占块数**
SELECT pg_prewarm('newdata.a_reuters_factors',mode := 'buffer',fork := 'main',first_block := null,last_block := null);
-- 想存什么表和索引都可以.注意索引膨胀问题

-- 8 设置pgprewarm在postgresql重启时缓存也不丢失
-- pg_stat_statements 需要在C:/Program Files/PostgreSQL/15/data/postgresql.conf中设置一下
-- 流程：
-- 1 运行下面语句，找sourcefile列，一般在C:/Program Files/PostgreSQL/15/data/postgresql.conf
select * from pg_settings where name='shared_buffers';
-- 2 在postgresql.conf文件中搜索shared_preload_libraries 把前面的注释取消,可以按4操作
-- 3 可能要修改C:/Program Files/PostgreSQL/15/data/postgresql.auto.conf文件，这个文件优先的
-- 4 在postgresql.conf，postgresql.auto.conf添加shared_preload_libraries = 'pg_stat_statements, pg_prewarm'
-- 重启数据库
-- 5 按照2，3，4或者运行下面两行，然后重启数据库
show shared_preload_libraries;
-- 数据库启动时加载，配置shared_preload_libraries参数，必须重启数据库
alter system set shared_preload_libraries=pg_stat_statements,pg_prewarm;
-- 6 运行下面,创建扩展
CREATE  EXTENSION if not exists pg_stat_statements;
-- 跟踪并统计PostgreSQL数据库中语句执行性能的扩展插件
SELECT * FROM pg_stat_statements;
-- 配置好后,下面语句返回pg_stat_statements,pg_prewarm.
show shared_preload_libraries;





-- 9 其他
--查看数据在缓存中占比
SELECT c.relname,pg_size_pretty(count(*) * 8192) as buffered,
round(100.0 * count(*) /(SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1)AS buffers_percent,
round(100.0 * count(*) * 8192 /pg_relation_size(c.oid),1)AS percent_of_relation
FROM pg_class c
INNER JOIN pg_buffercache b ON b.relfilenode = c.relfilenode
INNER JOIN pg_database d ON (b.reldatabase = d.oid AND d.datname = current_database())
GROUP BY c.oid,c.relname
ORDER BY 3 DESC LIMIT 10;