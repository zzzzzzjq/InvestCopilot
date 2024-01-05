
CREATE USER copilot WITH PASSWORD 'cpwd@#45%';
CREATE DATABASE  copilot owner copilot;

CREATE SCHEMA config    AUTHORIZATION copilot;
CREATE SCHEMA portfolio    AUTHORIZATION copilot;
GRANT ALL ON ALL TABLES IN SCHEMA portfolio  TO copilot;
GRANT ALL ON ALL TABLES IN SCHEMA public  TO copilot;


ALTER ROLE copilot SET search_path TO public;

alter user copilot with password 'cpwd@#45%';



--创建用户表
create table  TUSERS (
USERID      VARCHAR(10)     PRIMARY KEY,
USERNAME              VARCHAR(50)  ,
USERPASSWORD          VARCHAR(100) ,
ROLEID                INTEGER     ,
PRIVILEGESET          VARCHAR(512) ,
USERSTATUS            INTEGER     ,
USERREALNAME          VARCHAR(40)  ,
USERNICKNAME          VARCHAR(40)  ,
DEPARTMENT            VARCHAR(50)  );

--添加基本管理员
Insert into  TUSERS (USERID,USERNAME,USERPASSWORD,ROLEID,PRIVILEGESET,USERSTATUS,USERREALNAME,USERNICKNAME,DEPARTMENT)
values ('001','rocky','M0DYFbMeOR1sfy+D551tNw==',1,'0,1,5,6,7,8,10,12',1,'Rocky.zhou','Rocky.zhou',null);

--创建用户ID序列
 CREATE SEQUENCE  SEQ_USERID
    START WITH 2     --从1开始
    INCREMENT BY 1   --每次递增1
    NO MINVALUE      --无最小值
    NO MAXVALUE      --无最大值
    CACHE 1;         --设置缓存中序列的个数，建议设置  PG有BUG这个CACHE设置为其他会导致不同的会话序列混乱

--创建用户角色表
create table  USERROLE(
USERID     VARCHAR(10) PRIMARY KEY,
ROLEID     VARCHAR(10) 
);
select * from portfolio_template;
--创建用户日志表
create table  USERLOG(
USERID        VARCHAR(10)  ,
SESSIONID     VARCHAR(100) ,
BUSINCODE     VARCHAR(50)  ,
OPERADATE     TIMESTAMP   ,
STATE         CHAR(1)       ,
PLATFORM      VARCHAR(20)  ,
BROWSER       VARCHAR(50)  ,
REMOTEIP      VARCHAR(20)  ,
CONTENT       VARCHAR(200) );

select * from  USERLOG;

--创建系统菜单表
create table MENU(
MENUID             VARCHAR(20)  PRIMARY KEY,
MENUNAME           VARCHAR(100) ,
MENUURL            VARCHAR(255) ,
PARENTMENUNAME     VARCHAR(100) ,
PARENTMENUICON     VARCHAR(100) ,
PARENTORDERID      VARCHAR(20)  ,
SUBORDERID         INTEGER) ;

--创建缓存日志表
create table CACHELOG(
CACHEKEY       VARCHAR(50) ,
CACHESIZE      VARCHAR(20) ,
VALIDTIME      TIMESTAMP         ,
CREATETIME     TIMESTAMP        );

--创建角色权限表
create table PRIVROLE(
ROLEID         VARCHAR(10)   PRIMARY KEY,
ROLENAME       VARCHAR(100)  ,
MENUIDLIST     VARCHAR(2000) );

--创建菜单URL表
create table URLS(
MENUID        VARCHAR(20)  not null,
ACCESSURL     VARCHAR(255) );


--添加基本用户菜单数据表
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submenu00','欢迎','welcome.html','欢迎','linecons-star','menu00',1);
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submanager01','用户管理','manageuser.html','后台管理','fa-briefcase','submanager01',1);
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submanager02','角色管理','managemenu.html','后台管理','fa-briefcase','submanager01',2);
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submanager03','缓存管理','managecache.html','后台管理','fa-briefcase','submanager01',3);

--添加基本url信息表
Insert into URLS (MENUID,ACCESSURL) values ('submenu00','welcome.html');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','delUser');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','restUserPwd');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','manageuser.html');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','getAllUserData');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','addUser');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','editeUserRole');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','stopUser');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','startUser');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','flushUserCache');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','dataTableLanguage');
Insert into URLS (MENUID,ACCESSURL) values ('submanager02','managemenu.html');
Insert into URLS (MENUID,ACCESSURL) values ('submanager02','amendMenuPriv');
Insert into URLS (MENUID,ACCESSURL) values ('submanager02','addMenuPriv');
Insert into URLS (MENUID,ACCESSURL) values ('submanager02','removeMenuPriv');
Insert into URLS (MENUID,ACCESSURL) values ('submanager03','managecache.html');
Insert into URLS (MENUID,ACCESSURL) values ('submanager03','getSysCacheList');
Insert into URLS (MENUID,ACCESSURL) values ('submanager03','flushSysCacheKey');
Insert into URLS (MENUID,ACCESSURL) values ('submanager03','searchSysCache');
Insert into URLS (MENUID,ACCESSURL) values ('submanager03','dataTableLanguage');
Insert into URLS (MENUID,ACCESSURL) values ('nomenu','Changepassword.html');
Insert into URLS (MENUID,ACCESSURL) values ('nomenu','PasswordChange');
Insert into URLS (MENUID,ACCESSURL) values ('nomenu','dataTableLanguage');

--添加基本用户角色（管理员权限角色）
insert into userrole values('001','01');


--添加基本的用户菜单权限
Insert into PRIVROLE (ROLEID,ROLENAME,MENUIDLIST) values ('01','所有权限','''nomenu'',''submenu00'',''submanager01'',''submanager02'',''submanager03'',''submenu04''');

--修改菜单图标
UPDATE MENU T
SET
   PARENTMENUICON ='fa fa-tachometer fa-fw"'
WHERE
  PARENTORDERID ='menu00';

UPDATE MENU T
SET
  PARENTMENUICON ='fa fa-gears fa-fw"'
WHERE
  PARENTORDERID ='submanager01';


--创建用户主页表
create table tusermainpage(
USERID  VARCHAR(10) NOT NULL, 
MENUID          VARCHAR(20) );

--添加用户主页表基本数据
Insert into TUSERMAINPAGE (USERID,MENUID) values ('001','submenu00');


--一些和oracle兼容的函数
--instr函数
-- 实现1
CREATE FUNCTION INSTR(
  VARCHAR,
  VARCHAR
) RETURNS INTEGER AS
  $$ DECLARE POS INTEGER;
BEGIN
  POS := INSTR($1, $2, 1);
  RETURN POS;
END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;
 -- 实现2
 CREATE FUNCTION INSTR(STRING VARCHAR, STRING_TO_SEARCH VARCHAR, BEG_INDEX INTEGER) RETURNS INTEGER AS
  $$        DECLARE POS INTEGER NOT NULL DEFAULT 0;
  TEMP_STR  VARCHAR;
  BEG       INTEGER;
  LENGTH    INTEGER;
  SS_LENGTH INTEGER;
BEGIN
  IF BEG_INDEX > 0 THEN
    TEMP_STR := SUBSTRING(STRING FROM BEG_INDEX);
    POS := POSITION(STRING_TO_SEARCH IN TEMP_STR);
    IF POS = 0 THEN
      RETURN 0;
    ELSE
      RETURN POS + BEG_INDEX - 1;
    END IF;
  ELSIF BEG_INDEX < 0 THEN
    SS_LENGTH := CHAR_LENGTH(STRING_TO_SEARCH);
    LENGTH := CHAR_LENGTH(STRING);
    BEG := LENGTH + BEG_INDEX - SS_LENGTH + 2;
    WHILE BEG > 0 LOOP
      TEMP_STR := SUBSTRING(STRING FROM BEG FOR SS_LENGTH);
      POS := POSITION(STRING_TO_SEARCH IN TEMP_STR);
      IF POS > 0 THEN
        RETURN BEG;
      END IF;
      BEG := BEG - 1;
    END LOOP;
    RETURN 0;
  ELSE
    RETURN 0;
  END IF;
END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;
 -- 实现3
 CREATE FUNCTION INSTR(STRING VARCHAR, STRING_TO_SEARCH VARCHAR, BEG_INDEX INTEGER, OCCUR_INDEX INTEGER) RETURNS INTEGER AS
  $$           DECLARE POS INTEGER NOT NULL DEFAULT 0;
  OCCUR_NUMBER INTEGER NOT NULL DEFAULT 0;
  TEMP_STR     VARCHAR;
  BEG          INTEGER;
  I            INTEGER;
  LENGTH       INTEGER;
  SS_LENGTH    INTEGER;
BEGIN
  IF BEG_INDEX > 0 THEN
    BEG := BEG_INDEX;
    TEMP_STR := SUBSTRING(STRING FROM BEG_INDEX);
    FOR I IN 1 .. OCCUR_INDEX LOOP
      POS := POSITION(STRING_TO_SEARCH IN TEMP_STR);
      IF I = 1 THEN
        BEG := BEG + POS - 1;
      ELSE
        BEG := BEG + POS;
      END IF;
      TEMP_STR := SUBSTRING(STRING FROM BEG + 1);
    END LOOP;
    IF POS = 0 THEN
      RETURN 0;
    ELSE
      RETURN BEG;
    END IF;
  ELSIF BEG_INDEX < 0 THEN
    SS_LENGTH := CHAR_LENGTH(STRING_TO_SEARCH);
    LENGTH := CHAR_LENGTH(STRING);
    BEG := LENGTH + BEG_INDEX - SS_LENGTH + 2;
    WHILE BEG > 0 LOOP
      TEMP_STR := SUBSTRING(STRING FROM BEG FOR SS_LENGTH);
      POS := POSITION(STRING_TO_SEARCH IN TEMP_STR);
      IF POS > 0 THEN
        OCCUR_NUMBER := OCCUR_NUMBER + 1;
        IF OCCUR_NUMBER = OCCUR_INDEX THEN
          RETURN BEG;
        END IF;
      END IF;
      BEG := BEG - 1;
    END LOOP;
    RETURN 0;
  ELSE
    RETURN 0;
  END IF;
END;
$$ LANGUAGE PLPGSQL STRICT IMMUTABLE;

  

--一些测试的SQL可以忽略不执行
select * from menu;
select * from userrole;
select * from privrole;


update menu t set parentmenuicon ='fa fa-tachometer fa-fw"' where parentorderid ='menu00';
update menu t set parentmenuicon ='fa fa-gears fa-fw"' where parentorderid ='submanager01';

desc tusers;

alter table tusers alter column userpassword  TYPE VARCHAR(100);

--------------------------------------


--20231001 菜单设置
--新的菜单
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submenu02','美国','usa.html','美国','fa region-usa fa-fw','menu05',1);
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submenu03','中国','china.html','中国','fa region-china fa-fw','menu08',1);



Insert into URLS (MENUID,ACCESSURL) values ('submenu02','usa.html');
Insert into URLS (MENUID,ACCESSURL) values ('submenu03','china.html');


--自选股
INSERT INTO MENU (
  MENUID,
  MENUNAME,
  MENUURL,
  PARENTMENUNAME,
  PARENTMENUICON,
  PARENTORDERID,
  SUBORDERID
) VALUES (
  'submenu04',
  '自选股',
  'userportfolio.html',
  '自选股',
  'fa fa-bar-chart-o fa-fw',
  'menu09',
  1
);

INSERT INTO URLS (
  MENUID,
  ACCESSURL
) VALUES (
  'submenu04',
  'userportfolio.html'
);

--新建用户自选股组合表
CREATE TABLE portfolio.USERPORTFOLIO(
    PORTFOLIOID   INTEGER    PRIMARY KEY,   -- 组合ID
    USERID        VARCHAR(10),              -- 用户ID
    PORTFOLIONAME VARCHAR(50),              -- 组合名称
    STOCKNUM INTEGER                       -- 股票数量
);

--新建用户自选股股票表
CREATE TABLE portfolio.USERPORTFOLIOSTOCK(
    PORTFOLIOID   INTEGER,                 -- 组合ID
    STOCKCODE     VARCHAR(10),             -- 股票代码    
    STOCKNAME     VARCHAR(200),            -- 股票名称
    INSERTDATE    TIMESTAMP,               -- 添加日期
    ORDERID      INTEGER,                  -- 排序号
    PRIMARY KEY(PORTFOLIOID, STOCKCODE)    -- 主键(组合ID，股票代码)
);


--创建组合ID序列
CREATE SEQUENCE portfolio.SEQ_PORTFOLIOID
    START WITH 1 --从1开始
    INCREMENT BY 1 --每次递增1
    NO MINVALUE --无最小值
    NO MAXVALUE --无最大值
    CACHE 1;


GRANT USAGE, SELECT ON SEQUENCE portfolio.seq_portfolioid TO copilot;

/* 测试代码 */
--insert into userportfolio values(nextval('SEQ_PORTFOLIOID'),'001','test1',0,'');

--delete from userportfolio ;

--select * from userportfolio;
select * from urls where MENUID='submenu03';

select * from stockinfo where stockcode ='NVDA';

insert into portfolio.USERPORTFOLIOSTOCK values(10,'AAPL','苹果',now(),1);
insert into portfolio.USERPORTFOLIOSTOCK values(10,'MSFT','微软',now(),2);
insert into portfolio.USERPORTFOLIOSTOCK values(10,'NVDA','英伟达',now(),3);

update portfolio.USERPORTFOLIO set stocknum =3 where portfolioid =10;

-- update urls set menuid ='submenu04' where accessurl ='china.html';

/* 测试代码 */



/*add*/



CREATE TABLE dsidmfactors.sysdictionary (
	keyno varchar(20) NULL,
	keyvalue varchar(100) NULL,
	keydesc varchar(200) NULL,
	keyenbale bpchar(1) NULL,
	keyorder int4 NULL
);

\db;


CREATE SCHEMA config     AUTHORIZATION copilot;
GRANT ALL ON ALL TABLES IN SCHEMA config  TO copilot;
create table config.platename(
	id varchar(100),
	name varchar(200),
	area varchar(50),
	ord integer
);


create table config.plateCodes(
	id varchar(100),
	windCode varchar(50),
	insertTime timestamp,
	status char(1)
)





CREATE TABLE factorrluser (
	userid varchar(10) NULL,
	templateno varchar(10) NULL,
	templatename varchar(50) NULL,
	display bpchar(1) NULL,
	templatetype varchar(20) NULL,
	templateorder int4 NULL,
	templatedesc varchar(100) NULL,
	defaulttemplate bpchar(1) NULL
);
 COMMENT ON TABLE factorrluser IS '用户指标模板配置表。';
 COMMENT ON COLUMN factorrluser.display IS '';
 COMMENT ON COLUMN factorrluser.templatetype IS '模版分类 自选股、组合、报告查询等等 ';
-- add1115
alter table factorrluser ADD portfolioid varchar(10) NULL ;

select * from factorrluser;
INSERT INTO factorrluser
(userid, templateno, templatename, display, templatetype, templateorder, templatedesc, defaulttemplate)
VALUES('051', '8', 'Summary', '1', '100', NULL, '自选股系统默认模版', '1');

CREATE SEQUENCE IF NOT EXISTS SEQ_FACTORUSERTEMPLATEID
    START WITH 1 --从1开始
    INCREMENT BY 1 --每次递增1
    NO MINVALUE --无最小值
    NO MAXVALUE --无最大值
    CACHE 1;
GRANT USAGE, SELECT ON SEQUENCE SEQ_FACTORUSERTEMPLATEID TO copilot;

CREATE TABLE factortemplate (
	templateno varchar(10) NULL,
	factorno int4 NULL,
	orderno int4 NULL,
	statementtype varchar(20) NULL,
	reportperiod varchar(8) NULL
);

 COMMENT ON TABLE factortemplate IS '模板指标配置表。';
 COMMENT ON COLUMN factortemplate.factorno IS 'factorcell表中的factorno ';
 COMMENT ON COLUMN factortemplate.statementtype IS 'A股中的报告表类型';
COMMENT ON COLUMN factortemplate.reportperiod IS 'A股中的报告期';


CREATE TABLE factorcell (
	factorno int4 NULL,
	factorclass varchar(100) NULL,
	factortype varchar(50) NULL,
	factordesc varchar(100) NULL,
	factortable varchar(50) NULL,
	factortname varchar(100) NULL,
	factororder int4 NULL,
	regxnumber int4 NULL,
	regxtype bpchar(1) NULL,
	floatsize int4 NULL,
	"enable" bpchar(1) NULL,
	qmode bpchar(1) NULL,
	fdesc varchar(500) NULL,
	searchkey varchar(200) NULL,
	fview varchar(100) NULL
);

INSERT INTO factortemplate
(templateno, factorno, orderno, statementtype, reportperiod)
VALUES('8', 28747, 1, NULL, NULL);
INSERT INTO factortemplate
(templateno, factorno, orderno, statementtype, reportperiod)
VALUES('8', 28845, 3, NULL, NULL);
INSERT INTO factortemplate
(templateno, factorno, orderno, statementtype, reportperiod)
VALUES('8', 28846, 4, NULL, NULL);
INSERT INTO factortemplate
(templateno, factorno, orderno, statementtype, reportperiod)
VALUES('8', 28847, 5, NULL, NULL);
INSERT INTO factortemplate
(templateno, factorno, orderno, statementtype, reportperiod)
VALUES('8', 28848, 6, NULL, NULL);
INSERT INTO factortemplate
(templateno, factorno, orderno, statementtype, reportperiod)
VALUES('8', 28844, 2, NULL, NULL);



CREATE TABLE portfolio.user_portfolio (
	userid varchar(10) NOT NULL,
	portfolioid varchar(10) NOT NULL,
	portfoliotype varchar(20) NOT NULL,
	portfolioname varchar(50) NOT NULL,
	seqno int4 NOT NULL,
	createtime timestamp NULL,
	averageyield numeric(16, 4) NULL
);


alter table  portfolio.user_portfolio add stocknum integer;


CREATE TABLE portfolio.user_portfolio_list (
	userid varchar(10) NOT NULL,
	portfolioid varchar(10) NOT NULL,
	windcode varchar(20) NULL,
	seqno int4 NULL,
	createtime timestamp NULL
);

INSERT INTO portfolio.user_portfolio
(userid, portfolioid, portfoliotype, portfolioname, seqno, createtime, averageyield)
VALUES('051', 'self_23462', 'self', '我的自选股', 1, '2023-09-03 21:38:00.335', 0.0203);

INSERT INTO portfolio.user_portfolio
(userid, portfolioid, portfoliotype, portfolioname, seqno, createtime, averageyield)
VALUES('051', 'self_23463', 'self', '我的自选股2', 1, '2023-09-03 21:38:00.335', 0.0203);


INSERT INTO portfolio.user_portfolio_list
(userid, portfolioid, windcode, seqno, createtime)
VALUES('051', 'self_23462', 'NVDA.O', 1, '2023-09-03 21:38:24.554');
INSERT INTO portfolio.user_portfolio_list
(userid, portfolioid, windcode, seqno, createtime)
VALUES('051', 'self_23462', 'AMD.O', 2, '2023-09-03 21:38:30.012');
INSERT INTO portfolio.user_portfolio_list
(userid, portfolioid, windcode, seqno, createtime)
VALUES('051', 'self_23462', '0700.HK', 3, '2023-09-03 21:38:37.294');
INSERT INTO portfolio.user_portfolio_list
(userid, portfolioid, windcode, seqno, createtime)
VALUES('051', 'self_23463', 'AAPL.O', 1, '2023-09-03 22:06:26.993');
INSERT INTO portfolio.user_portfolio_list
(userid, portfolioid, windcode, seqno, createtime)
VALUES('051', 'self_23463', 'MSFT.O', 3, '2023-09-03 22:07:20.459');
INSERT INTO portfolio.user_portfolio_list
(userid, portfolioid, windcode, seqno, createtime)
VALUES('051', 'self_23463', 'NVDA.O', 4, '2023-09-03 22:07:27.244');
INSERT INTO portfolio.user_portfolio_list
(userid, portfolioid, windcode, seqno, createtime)
VALUES('051', 'self_23462', 'MSFT.O', 4, '2023-09-07 14:29:00.056');





--股票代码配置表
CREATE TABLE stockinfo (
	stockcode varchar(50) NULL,
	windcode varchar(20) NULL,
	eastcode varchar(20) NULL,
	stockname varchar(100) NULL,
	stocktype varchar(10) NULL,
	area varchar(10) NULL,
	updateday varchar(8) NULL,
	pinyin varchar(200) NULL,
	relationcode varchar(50) NULL,
	disabled bpchar(1) NULL
);

CREATE TABLE config.workday (
	seq int4 NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX config_workday ON config.workday USING btree (tradedate);

CREATE TABLE config.usaworkday (
	workday varchar(8) NULL
);

CREATE TABLE config.hkworkday (
	workday varchar(8) NULL
);


CREATE SCHEMA spider
    AUTHORIZATION copilot;

GRANT ALL ON ALL TABLES IN SCHEMA spider  TO copilot;


CREATE TABLE spider.emminhq_time (
	stockcode varchar(20) NULL,
	tradedate varchar(10) NULL,
	tradetime varchar(10) NULL,
	nowprice numeric(16, 2) NULL,
	pctchange numeric(22, 4) NULL,
	area varchar(20) NULL,
	status varchar(10) NULL
);
CREATE INDEX emminhq_time_dt ON spider.emminhq_time USING btree (tradedate);
CREATE INDEX emminhq_time_tt ON spider.emminhq_time USING btree (tradetime);



CREATE TABLE spider.emminhq_index (
	stockcode varchar(20) NULL,
	tradedate varchar(10) NULL,
	tradetime varchar(10) NULL,
	nowprice numeric(16, 2) NULL,
	pctchange numeric(22, 4) NULL,
	high numeric(16, 2) NULL,
	low numeric(16, 2) NULL,
	"open" numeric(16, 2) NULL,
	preclose numeric(16, 2) NULL,
	roundlot numeric(16, 2) NULL,
	"change" numeric(16, 2) NULL,
	volume numeric(16, 2) NULL,
	amount numeric(16, 2) NULL,
	area varchar(20) NULL
);
-- drop table if exists portfolio_template cascade ;
-- create table if not exists portfolio_template(
--     userid varchar(10) NULL,
--     portfolioid varchar(10) NOT NULL,
--     templateno varchar(10) NULL,
--     updatetime timestamp NULL
--
-- );
-- comment on table  portfolio_template is '股票组合与指标模版的关系表';
-- GRANT ALL ON ALL TABLES IN SCHEMA public  TO copilot;
-- select * from portfolio_template;


CREATE TABLE spider.usa_stockcode (
	stockcode varchar(50) NULL,
	eastcode varchar(20) NULL,
	stockname varchar(200) NULL,
	area varchar(10) NULL
);



CREATE SCHEMA newdata
    AUTHORIZATION copilot;

GRANT ALL ON ALL TABLES IN SCHEMA newdata  TO copilot;


CREATE TABLE newdata.asharedescription (
	object_id varchar(100) NULL,
	s_info_windcode varchar(40) NULL,
	s_info_code varchar(40) NULL,
	s_info_name varchar(50) NULL,
	s_info_compname varchar(100) NULL,
	s_info_compnameeng varchar(100) NULL,
	s_info_isincode varchar(40) NULL,
	s_info_exchmarket varchar(40) NULL,
	s_info_listboard varchar(10) NULL,
	s_info_listdate varchar(8) NULL,
	s_info_delistdate varchar(8) NULL,
	s_info_sedolcode varchar(40) NULL,
	crncy_code varchar(10) NULL,
	s_info_pinyin varchar(10) NULL,
	s_info_listboardname varchar(10) NULL,
	is_shsc int4 NULL,
	opdate date NULL,
	opmode varchar(1) NULL
);



CREATE TABLE spider.eaststockcode (
	eastcode varchar(20) NULL,
	enable bpchar(1) NULL,
	stocktype varchar(10) NULL
);


alter table company add updateTime timestamp;
update company set updateTime=current_timestamp;



CREATE TABLE portfolio.summarys_Comments (
	rid varchar(10) not NULL,
	sid varchar(100) not  NULL,
	cuserid varchar(10) not  NULL,
	slable  text NULL,
	scomments  text NULL,
	publishtime timestamp not null,
	parentid varchar(10)
);
--rid,sid,cuserid,slable,comments,publishtime,parentid

--创建评论
 CREATE sequence business.seq_comment
    START WITH 1     --从1开始
    INCREMENT BY 1   --每次递增1
    NO MINVALUE      --无最小值
    NO MAXVALUE      --无最大值
    CACHE 1;         --设置缓存中序列的个数，建议设置  PG有BUG这个CACHE设置为其他会导致不同的会话序列混乱


GRANT USAGE, SELECT ON SEQUENCE  business.seq_comment TO copilot;




--路透社美股配置表
create table config.reuters_usstocks(
Id	 varchar(20),
windcode  varchar(20),
SecCode  varchar(20),
Cusip  varchar(20),
ISIN   varchar(20),
stockName  varchar(200),
issuename  varchar(100),
RIC  varchar(20),
Ticker  varchar(20),
Sedol  varchar(20),
ExchCode  varchar(20),
CntryCode  varchar(20),
gicssectorname   varchar(200),
gicsindustryname  varchar(200),
gicssubindustrynamme  varchar(200)
)

--路透社美股价格
CREATE TABLE newdata.reuters_us_stockprice (
	windcode varchar(15) NULL,
	stockcode varchar(15) NULL,
	ric varchar(15) NULL,
	stockname varchar(200) NULL,
	s_dq_close numeric(20, 4) NULL,
	s_dq_change numeric(20, 4) NULL,
	s_dq_pctchange numeric(20, 4) NULL,
	s_dq_volume numeric(20, 4) NULL,
	s_dq_amount numeric(20, 4) NULL,
	s_dq_open numeric(20, 4) NULL,
	s_dq_high numeric(20, 4) NULL,
	s_dq_low numeric(20, 4) NULL,
	s_dq_preclose numeric(20, 4) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX r_usa_mk_wdcode ON newdata.reuters_us_stockprice USING btree (windcode);
CREATE INDEX r_usa_mk_td ON newdata.reuters_us_stockprice USING btree (tradedate);

--点赞操作
CREATE SCHEMA business    AUTHORIZATION copilot;

create table business.summary_like(
sid varchar(100),
userid varchar(10),
slike char(1),
updatetime timestamp
)
--点评操作

drop table business.summary_comments;
create table business.summary_comments(
cid varchar(10),
sid varchar(100),
userid varchar(10),
comments varchar(1000),
updatetime timestamp,
parentid varchar(10)
)


alter  table business.summary_comments add flag char(1);
alter  table business.summary_like add flag char(1);

--指标数据：

-- DROP TABLE newdata.a_reuters_analyst;

CREATE TABLE newdata.a_reuters_analyst (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	numestrevisingup numeric(22, 6) NULL,
	numestrevisingdown numeric(22, 6) NULL,
	numberofanalysts numeric(22, 6) NULL,
	meanpctchgestimatemeasurerevfy2wp60d numeric(22, 6) NULL,
	meanpctchgfy2wp60d numeric(22, 6) NULL,
	recmean numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_analyst_wdcode ON newdata.a_reuters_analyst USING btree (windcode);
CREATE INDEX a_reuters_analyst_td ON newdata.a_reuters_analyst USING btree (tradedate);


-- newdata.a_reuters_basic definition

-- Drop table

-- DROP TABLE newdata.a_reuters_basic;

CREATE TABLE newdata.a_reuters_basic (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	exchangecountry varchar(100) NULL,
	gicsindustry varchar(100) NULL,
	gicssubindustry varchar(100) NULL,
	gicssector varchar(100) NULL,
	priceclose numeric(22, 6) NULL,
	volume numeric(22, 6) NULL,
	avgdailyvolume10day numeric(22, 6) NULL,
	tradedate varchar(8) NULL,
	companyname varchar(200) NULL
);
CREATE INDEX a_reuters_basic_wdcode ON newdata.a_reuters_basic USING btree (windcode);
CREATE INDEX a_reuters_basic_td ON newdata.a_reuters_basic USING btree (tradedate);




-- newdata.a_reuters_debt definition

-- Drop table

-- DROP TABLE newdata.a_reuters_debt;

CREATE TABLE newdata.a_reuters_debt (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	totaldebtoutstanding numeric(22, 6) NULL,
	ttldebttottlequitypct numeric(22, 6) NULL,
	cashandequivalents numeric(22, 6) NULL,
	currentratio numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_debt_wdcode ON newdata.a_reuters_debt USING btree (windcode);
CREATE INDEX a_reuters_debt_td ON newdata.a_reuters_debt USING btree (tradedate);




-- newdata.a_reuters_dividend definition

-- Drop table

-- DROP TABLE newdata.a_reuters_dividend;

CREATE TABLE newdata.a_reuters_dividend (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	dividendyield numeric(22, 6) NULL,
	dpssmartestntm numeric(22, 6) NULL,
	dividendpayoutratiopct numeric(22, 6) NULL,
	dividendpayoutratiopct5yravg numeric(22, 6) NULL,
	dividendpayoutratiopctttm numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_dividend_wdcode ON newdata.a_reuters_dividend USING btree (windcode);
CREATE INDEX a_reuters_dividend_td ON newdata.a_reuters_dividend USING btree (tradedate);



-- newdata.a_reuters_earnings definition

-- Drop table

-- DROP TABLE newdata.a_reuters_earnings;

CREATE TABLE newdata.a_reuters_earnings (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	revenueltm numeric(22, 6) NULL,
	revenuemeanfy1 numeric(22, 6) NULL,
	revenuemeanfy2 numeric(22, 6) NULL,
	revenueactvaluefy0 numeric(22, 6) NULL,
	revenueactsurprisefq0 numeric(22, 6) NULL,
	revenuefq0 numeric(22, 6) NULL,
	revenuefq1 numeric(22, 6) NULL,
	operatingincomeltm numeric(22, 6) NULL,
	operatingincomefy0 numeric(22, 6) NULL,
	oprmeanfy1 numeric(22, 6) NULL,
	oprmeanfy2 numeric(22, 6) NULL,
	ebitltm numeric(22, 6) NULL,
	ebitfy0 numeric(22, 6) NULL,
	ebitmeanfy1 numeric(22, 6) NULL,
	ebitmeanfy2 numeric(22, 6) NULL,
	ebitdaltm numeric(22, 6) NULL,
	ebitdafy0 numeric(22, 6) NULL,
	ebitdameanfy1 numeric(22, 6) NULL,
	ebitdameanfy2 numeric(22, 6) NULL,
	netprofitactvalueltm numeric(22, 6) NULL,
	netprofitmeanfy1 numeric(22, 6) NULL,
	netprofitmeanfy2 numeric(22, 6) NULL,
	netprofitactvaluefy0 numeric(22, 6) NULL,
	epsactvalueltm numeric(22, 6) NULL,
	epsmeanfy1 numeric(22, 6) NULL,
	epsmeanfy2 numeric(22, 6) NULL,
	epsactvaluefy0 numeric(22, 6) NULL,
	epsactsurprisefq0 numeric(22, 6) NULL,
	epsexclextradilfq0 numeric(22, 6) NULL,
	epsexclextradilfq1 numeric(22, 6) NULL,
	cashflowfromoperationsactvalueltm numeric(22, 6) NULL,
	capexmeanfy1 numeric(22, 6) NULL,
	capexmeanfy2 numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_earnings_wdcode ON newdata.a_reuters_earnings USING btree (windcode);
CREATE INDEX a_reuters_earnings_td ON newdata.a_reuters_earnings USING btree (tradedate);







-- newdata.a_reuters_growth definition

-- Drop table

-- DROP TABLE newdata.a_reuters_growth;

CREATE TABLE newdata.a_reuters_growth (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	revenuemeanfy2revenuemeanfy11 numeric(22, 6) NULL,
	revenuemeanfy1revenueactvaluefy01 numeric(22, 6) NULL,
	revenuefq0revenuefq11 numeric(22, 6) NULL,
	totrevenuepctprdtoprdfq0 numeric(22, 6) NULL,
	totrevenue3yrcagrfy0 numeric(22, 6) NULL,
	totrevenue5yrcagrfy0 numeric(22, 6) NULL,
	ebitdameanfy2ebitdameanfy11 numeric(22, 6) NULL,
	ebitdameanfy1ebitdafy01 numeric(22, 6) NULL,
	netprofitmeanfy2netprofitmeanfy11 numeric(22, 6) NULL,
	netprofitmeanfy1netprofitactvaluefy01 numeric(22, 6) NULL,
	epsmeanestfwdyrgrowth numeric(22, 6) NULL,
	epsmeanestlastyrgrowth numeric(22, 6) NULL,
	epsexclextradilfq0epsexclextradilfq11 numeric(22, 6) NULL,
	epsexclextradilpctprdtoprdfy0 numeric(22, 6) NULL,
	ltgmean numeric(22, 6) NULL,
	capexsmartestfwdyrgrowth numeric(22, 6) NULL,
	capexsmartestlastyrgrowth numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_growth_wdcode ON newdata.a_reuters_growth USING btree (windcode);
CREATE INDEX a_reuters_growth_td ON newdata.a_reuters_growth USING btree (tradedate);

-- newdata.a_reuters_mom definition

-- Drop table

-- DROP TABLE newdata.a_reuters_mom;

CREATE TABLE newdata.a_reuters_mom (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	totalreturn1wk numeric(22, 6) NULL,
	totalreturn1mo numeric(22, 6) NULL,
	totalreturn3mo numeric(22, 6) NULL,
	totalreturn6mo numeric(22, 6) NULL,
	totalreturn52wk numeric(22, 6) NULL,
	totalreturnytd numeric(22, 6) NULL,
	priceavg5d numeric(22, 6) NULL,
	priceavg20d numeric(22, 6) NULL,
	priceavg60d numeric(22, 6) NULL,
	price150dayaverage numeric(22, 6) NULL,
	price200dayaverage numeric(22, 6) NULL,
	priceavgnetdiff50d numeric(22, 6) NULL,
	pricerelsmapctchg200d numeric(22, 6) NULL,
	turnover numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_mom_wdcode ON newdata.a_reuters_mom USING btree (windcode);
CREATE INDEX a_reuters_mom_td ON newdata.a_reuters_mom USING btree (tradedate);


-- newdata.a_reuters_ownership definition

-- Drop table

-- DROP TABLE newdata.a_reuters_ownership;

CREATE TABLE newdata.a_reuters_ownership (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	ttlcmnsharesout numeric(22, 6) NULL,
	freefloatpct numeric(22, 6) NULL,
	siinstitutionalown numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_ownership_wdcode ON newdata.a_reuters_ownership USING btree (windcode);
CREATE INDEX a_reuters_ownership_td ON newdata.a_reuters_ownership USING btree (tradedate);





-- newdata.a_reuters_profitability definition

-- Drop table

-- DROP TABLE newdata.a_reuters_profitability;

CREATE TABLE newdata.a_reuters_profitability (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	gpmmeanfy0 numeric(22, 6) NULL,
	gpmmeanfy1 numeric(22, 6) NULL,
	gpmmeanfy2 numeric(22, 6) NULL,
	pcoperatingmarginpctfy0 numeric(22, 6) NULL,
	oprmeanfy1revenuemeanfy1 numeric(22, 6) NULL,
	oprmeanfy2revenuemeanfy2 numeric(22, 6) NULL,
	ebitmarginpercentfy0 numeric(22, 6) NULL,
	ebitmeanfy1revenuemeanfy1 numeric(22, 6) NULL,
	ebitmeanfy2revenuemeanfy2 numeric(22, 6) NULL,
	ebitdamarginpercentfy0 numeric(22, 6) NULL,
	ebitdameanfy1revenuemeanfy1 numeric(22, 6) NULL,
	ebitdameanfy2revenuemeanfy2 numeric(22, 6) NULL,
	netprofitmarginfy0 numeric(22, 6) NULL,
	netprofitmeanfy1revenuemeanfy1 numeric(22, 6) NULL,
	netprofitmeanfy2revenuemeanfy2 numeric(22, 6) NULL,
	roaactvaluefy0 numeric(22, 6) NULL,
	roameanfy1 numeric(22, 6) NULL,
	roameanfy2 numeric(22, 6) NULL,
	roeactvaluefy0 numeric(22, 6) NULL,
	roemeanfy1 numeric(22, 6) NULL,
	roemeanfy2 numeric(22, 6) NULL,
	assetturnoverfy0 numeric(22, 6) NULL,
	ltdebttottleqtypctfy0 numeric(22, 6) NULL,
	pcaccountsreceivableturnoverfy0 numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_profitability_wdcode ON newdata.a_reuters_profitability USING btree (windcode);
CREATE INDEX a_reuters_profitability_td ON newdata.a_reuters_profitability USING btree (tradedate);





-- newdata.a_reuters_starminecore definition

-- Drop table

-- DROP TABLE newdata.a_reuters_starminecore;

CREATE TABLE newdata.a_reuters_starminecore (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	fwdptoepssmartestntm numeric(22, 6) NULL,
	armintracountryscore numeric(22, 6) NULL,
	eqctryrank numeric(22, 6) NULL,
	ivpricetointrinsicvaluecountryrank numeric(22, 6) NULL,
	pricemocountryrank numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_starminecore_wdcode ON newdata.a_reuters_starminecore USING btree (windcode);
CREATE INDEX a_reuters_starminecore_td ON newdata.a_reuters_starminecore USING btree (tradedate);





-- newdata.a_reuters_trading definition

-- Drop table

-- DROP TABLE newdata.a_reuters_trading;

CREATE TABLE newdata.a_reuters_trading (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	pricepctchg1d numeric(22, 6) NULL,
	pricepctchg1y numeric(22, 6) NULL,
	pricepctchg3y numeric(22, 6) NULL,
	pricepctchg4w numeric(22, 6) NULL,
	pricepctchgytd numeric(22, 6) NULL,
	pricepctchg10y numeric(22, 6) NULL,
	pricepctchg5y numeric(22, 6) NULL,
	price52weekhigh numeric(22, 6) NULL,
	price52weeklow numeric(22, 6) NULL,
	betathreeyearweekly numeric(22, 6) NULL,
	betafiveyear numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_trading_wdcode ON newdata.a_reuters_trading USING btree (windcode);
CREATE INDEX a_reuters_trading_td ON newdata.a_reuters_trading USING btree (tradedate);



-- newdata.a_reuters_valuation definition

-- Drop table

-- DROP TABLE newdata.a_reuters_valuation;

CREATE TABLE newdata.a_reuters_valuation (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	ev numeric(22, 6) NULL,
	companymarketcapitalization numeric(22, 6) NULL,
	marketcapds numeric(22, 6) NULL,
	fwdptoepssmartestfy0 numeric(22, 6) NULL,
	pe numeric(22, 6) NULL,
	fwdpentm numeric(22, 6) NULL,
	ptoepsmeanestfy1 numeric(22, 6) NULL,
	ptoepsmeanestfy2 numeric(22, 6) NULL,
	histpeg numeric(22, 6) NULL,
	fwdpegntm numeric(22, 6) NULL,
	pegfy1 numeric(22, 6) NULL,
	pegfy2 numeric(22, 6) NULL,
	histenterprisevaluerevenuefy0 numeric(22, 6) NULL,
	histpricetorevenuepershareavgdilutedsharesoutltm numeric(22, 6) NULL,
	ptorevmeanestntm numeric(22, 6) NULL,
	ptorevmeanestfy1 numeric(22, 6) NULL,
	ptorevmeanestfy2 numeric(22, 6) NULL,
	ptobvpsactvaluefy0 numeric(22, 6) NULL,
	pricetobvpershare numeric(22, 6) NULL,
	ptobpsmeanestfy1 numeric(22, 6) NULL,
	ptobpsmeanestfy2 numeric(22, 6) NULL,
	evtoebitda numeric(22, 6) NULL,
	fwdevtoebitda numeric(22, 6) NULL,
	fwdevtoebitdafy1 numeric(22, 6) NULL,
	pricetocfpershare numeric(22, 6) NULL,
	tradedate varchar(8) NULL
);
CREATE INDEX a_reuters_valuation_wdcode ON newdata.a_reuters_valuation USING btree (windcode);
CREATE INDEX a_reuters_valuation_td ON newdata.a_reuters_valuation USING btree (tradedate);


create table newdata.a_reuters_analyst_his as select * from newdata.a_reuters_analyst where 1=2;
create table newdata.a_reuters_basic_his as select * from newdata.a_reuters_basic where 1=2;
create table newdata.a_reuters_debt_his as select * from newdata.a_reuters_debt where 1=2;
create table newdata.a_reuters_dividend_his as select * from newdata.a_reuters_dividend where 1=2;
create table newdata.a_reuters_earnings_his as select * from newdata.a_reuters_earnings where 1=2;
create table newdata.a_reuters_growth_his as select * from newdata.a_reuters_growth where 1=2;
create table newdata.a_reuters_mom_his as select * from newdata.a_reuters_mom where 1=2;
create table newdata.a_reuters_ownership_his as select * from newdata.a_reuters_ownership where 1=2;
create table newdata.a_reuters_profitability_his as select * from newdata.a_reuters_profitability where 1=2;
create table newdata.a_reuters_starminecore_his as select * from newdata.a_reuters_starminecore where 1=2;
create table newdata.a_reuters_trading_his as select * from newdata.a_reuters_trading where 1=2;
create table newdata.a_reuters_valuation_his as select * from newdata.a_reuters_valuation where 1=2;


CREATE INDEX a_reuters_analyst_his_wdcode ON newdata.a_reuters_analyst_his  USING btree (windcode);
CREATE INDEX a_reuters_analyst_his_td ON newdata.a_reuters_analyst_his  USING btree (tradedate);
CREATE INDEX a_reuters_basic_his_wdcode ON newdata.a_reuters_basic_his  USING btree (windcode);
CREATE INDEX a_reuters_basic_his_td ON newdata.a_reuters_basic_his  USING btree (tradedate);
CREATE INDEX a_reuters_debt_his_wdcode ON newdata.a_reuters_debt_his  USING btree (windcode);
CREATE INDEX a_reuters_debt_his_td ON newdata.a_reuters_debt_his  USING btree (tradedate);
CREATE INDEX a_reuters_dividend_his_wdcode ON newdata.a_reuters_dividend_his  USING btree (windcode);
CREATE INDEX a_reuters_dividend_his_td ON newdata.a_reuters_dividend_his  USING btree (tradedate);
CREATE INDEX a_reuters_earnings_his_wdcode ON newdata.a_reuters_earnings_his  USING btree (windcode);
CREATE INDEX a_reuters_earnings_his_td ON newdata.a_reuters_earnings_his  USING btree (tradedate);
CREATE INDEX a_reuters_growth_his_wdcode ON newdata.a_reuters_growth_his  USING btree (windcode);
CREATE INDEX a_reuters_growth_his_td ON newdata.a_reuters_growth_his  USING btree (tradedate);
CREATE INDEX a_reuters_mom_his_wdcode ON newdata.a_reuters_mom_his  USING btree (windcode);
CREATE INDEX a_reuters_mom_his_td ON newdata.a_reuters_mom_his  USING btree (tradedate);
CREATE INDEX a_reuters_ownership_his_wdcode ON newdata.a_reuters_ownership_his  USING btree (windcode);
CREATE INDEX a_reuters_ownership_his_td ON newdata.a_reuters_ownership_his  USING btree (tradedate);
CREATE INDEX a_reuters_profitability_his_wdcode ON newdata.a_reuters_profitability_his  USING btree (windcode);
CREATE INDEX a_reuters_profitability_his_td ON newdata.a_reuters_profitability_his  USING btree (tradedate);
CREATE INDEX a_reuters_starminecore_his_wdcode ON newdata.a_reuters_starminecore_his  USING btree (windcode);
CREATE INDEX a_reuters_starminecore_his_td ON newdata.a_reuters_starminecore_his  USING btree (tradedate);
CREATE INDEX a_reuters_trading_his_wdcode ON newdata.a_reuters_trading_his  USING btree (windcode);
CREATE INDEX a_reuters_trading_his_td ON newdata.a_reuters_trading_his  USING btree (tradedate);
CREATE INDEX a_reuters_valuation_his_wdcode ON newdata.a_reuters_valuation_his  USING btree (windcode);
CREATE INDEX a_reuters_valuation_his_td ON newdata.a_reuters_valuation_his  USING btree (tradedate);


--财务报表：

CREATE TABLE newdata.reuters_financial_report (
	windcode varchar(20) NULL,
	stockcode varchar(20) NULL,
	ric varchar(20) NULL,
	revenuemm numeric(24, 4) NULL,
	revenuegrowth numeric(24, 4) NULL,
	grossprofitmm numeric(24, 4) NULL,
	grossmargin numeric(24, 4) NULL,
	ebitdamm numeric(24, 4) NULL,
	ebitdamargin numeric(24, 4) NULL,
	netincomemm numeric(24, 4) NULL,
	netmargin numeric(24, 4) NULL,
	eps numeric(24, 4) NULL,
	epsgrowth numeric(24, 4) NULL,
	cashfromoperationsmm numeric(24, 4) NULL,
	capitalexpendituresmm numeric(24, 4) NULL,
	freecashflowmm numeric(24, 4) NULL,
	perioddate varchar(10) NULL,
	periodid varchar(10) NULL,
	tradedate varchar(10) NULL
);

CREATE INDEX reuters_financial_report_wdcode ON newdata.reuters_financial_report  USING btree (windcode);



------清库脚本：---
delete  from business.summarys_Comments;
delete  from business.summary_like;
alter sequence business.seq_comment restart with 1;





CREATE TABLE business.summarys_edit_status (
	sid varchar(100) not NULL,
	cuserid varchar(10) not NULL,
	edittime timestamp not NULL,
	endtime timestamp NULL,
	status varchar(10) NULL
);

CREATE TABLE business.summarys_edit_content (
	rid varchar(100) NULL,
	sid varchar(100) NULL,
	cuserid varchar(10) NULL,
	ccontent text NULL,
	languages varchar(10) NULL,
	edittime timestamp NULL,
	dtype varchar(50) NULL,
	dsource varchar(50) NULL
);


create index summary_like_sid on business.summary_like(sid);
create index summary_comments_sid on business.summary_comments (sid);

alter table business.summary_like add Marktype varchar(20);
alter table business.summary_like alter COLUMN   slike type varchar(10);

--用户登录日志记录
create table userrequestlog (
userid varchar(20),
requesturl  varchar(200),
requestmethod  varchar(100),
requestpara  varchar(2000),
remoteAddr  varchar(50),
Platform  varchar(100),
Browser  varchar(100),
requesttime timestamp

);


SELECT schemaname,sequencename,last_value FROM pg_sequences;

--最大序列矫正
SELECT sequence_schema ,sequence_name FROM information_schema.sequences;
--portfolio	seq_portfolioid
select max( to_number(replace(portfolioid,'self_',''),'fm9999')) from portfolio.user_portfolio up ;
alter sequence portfolio.seq_portfolioid restart with 2389;
--public	seq_userid
select max( to_number(replace(userid,'self_',''),'fm9999')) from tusers t ;
alter sequence public.seq_userid restart with 2067;
--public	seq_factorusertemplateid
select max( to_number(replace(templateno,'self_',''),'fm9999')) from factortemplate f  ; --pass
alter sequence public.seq_factorusertemplateid restart with 815;

--pubusinessblic	seq_comment
select max( to_number(replace(cid,'self_',''),'fm9999')) from business.summary_comments    f  ; --pass
alter sequence business.seq_comment restart with 433;

alter table factorrluser add column tcreatetime timestamp;
commit;


--主题：
create table  business.news_title_config(
userid   varchar(20),
title_id  varchar(50),
title_en  varchar(100),
title_zh  varchar(100),
ttype varchar(20), --self  sys
tsource varchar(20),--pool --strategy -- title
tord integer,
createtime timestamp
);

create table business.news_title_stocks(
userid   varchar(20),
title_id  varchar(50),
windcode   varchar(20)
);


delete from  business.news_title_config;
insert into  business.news_title_config values (
'80','ai_software','AI software','AI软件','sys','title',1,current_timestamp
);

insert into  business.news_title_config values (
'80','ai_hardware','AI hardware','AI硬件','sys','title',2,current_timestamp
);

insert into  business.news_title_config values (
'80','bitcoin','bitcoin','比特币','sys','title',3,current_timestamp
);

insert into  business.news_title_config values (
'80','saas','SAAS','SAAS','sys','title',4,current_timestamp
);

insert into  business.news_title_config values (
'80','us_consumer_discretionary','US Consumer Discretionary','美国可选消费','sys','title',5,current_timestamp
);

insert into  business.news_title_config values (
'80','us_internet','US Internet','美国互联网','sys','title',6,current_timestamp
);

insert into  business.news_title_config values (
'80','global_ev','global EV','全球电动车','sys','title',7,current_timestamp
);

insert into  business.news_title_config values (
'80','solar_energy','solar Energy','光伏新能源','sys','title',8,current_timestamp
);

insert into  business.news_title_config values (
'80','chinese_consumer','CH consumer','中国消费','sys','title',9,current_timestamp
);

insert into  business.news_title_config values (
'80','chinese_cost_effective_consumer','CH CostEffectiveConsumer','中国性价比消费','sys','title',10,current_timestamp
);

insert into  business.news_title_config values (
'80','chinese_internet','CH Internet','中国互联网','sys','title',11,current_timestamp
);

insert into  business.news_title_config values (
'80','chinese_travel','US Travel','中国出行','sys','title',12,current_timestamp
);

insert into  business.news_title_config values (
'80','weight_loss_drugs','weight LossRrugInfo','减肥药','sys','title',13,current_timestamp
);


insert into  business.news_title_config values (
'80','sg_SPV','Stock Price Volatility','股价波动','sys','pool',14,current_timestamp
);


insert into  business.news_title_config values (
'80','all_title','ALL','全部','sys','title',0,current_timestamp
);


select * from business.news_title_config order by tord ;


--所有覆盖的美股里面 前一天涨跌幅》5%or 《-5%的 所有股票


create table business.news_title_stocks(
userid   varchar(20),
title_id  varchar(50),
windcode   varchar(20)
);

select * from tusers where userid ='80';
select * from business.news_title_stocks where userid ='80';

commit;

delete from  business.news_title_stocks;
insert into  business.news_title_stocks
select '80','sg_SPV',windcode from spider.east_us_stockprice ecs
where tradedate =( select max(tradedate)  from  spider.east_us_stockprice where tradedate < ( select max(tradedate) from  spider.east_us_stockprice  ) )
and (ecs.s_dq_pctchange >5 or ecs.s_dq_pctchange <-5)


select  *  from business.news_title_stocks;


 select title_id,title_zh,title_en,ttype,tsource,tord  from business.news_title_config where userid = '80' and ttype='self'
                     union  select  title_id,title_zh,title_en,ttype,tsource,tord  from business.news_title_config where (userid = '80' and userid!='80')



create table business.news_title_nav(
title_id   varchar(50),
title_nav  numeric,
ndx_nav   numeric,
tradedate varchar(10)
)


--创建字典ID序列
CREATE SEQUENCE public.seq_dictid
    START WITH 1 --从1开始
    INCREMENT BY 1 --每次递增1
    NO MINVALUE --无最小值
    NO MAXVALUE --无最大值
    CACHE 1;

GRANT USAGE, SELECT ON SEQUENCE public.seq_dictid TO copilot;
