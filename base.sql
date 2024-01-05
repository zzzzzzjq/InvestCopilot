
CREATE USER copilot WITH PASSWORD 'password01';
CREATE DATABASE  copilot owner copilot;

--创建用户表
create table TUSERS (
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
Insert into TUSERS (USERID,USERNAME,USERPASSWORD,ROLEID,PRIVILEGESET,USERSTATUS,USERREALNAME,USERNICKNAME,DEPARTMENT)
values ('001','rocky','M0DYFbMeOR1sfy+D551tNw==',1,'0,1,5,6,7,8,10,12',1,'Rocky.zhou','Rocky.zhou',null);

--创建用户ID序列
 CREATE SEQUENCE SEQ_USERID
    START WITH 2     --从1开始
    INCREMENT BY 1   --每次递增1
    NO MINVALUE      --无最小值
    NO MAXVALUE      --无最大值
    CACHE 1;         --设置缓存中序列的个数，建议设置  PG有BUG这个CACHE设置为其他会导致不同的会话序列混乱

--创建用户角色表
create table USERROLE(
USERID     VARCHAR(10) PRIMARY KEY,
ROLEID     VARCHAR(10) 
);



--创建用户日志表
create table USERLOG(
USERID        VARCHAR(10)  ,
SESSIONID     VARCHAR(100) ,
BUSINCODE     VARCHAR(50)  ,
OPERADATE     TIMESTAMP   ,
STATE         CHAR(1)       ,
PLATFORM      VARCHAR(20)  ,
BROWSER       VARCHAR(50)  ,
REMOTEIP      VARCHAR(20)  ,
CONTENT       VARCHAR(200) );

select * from USERLOG;

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
Insert into PRIVROLE (ROLEID,ROLENAME,MENUIDLIST) values ('01','所有权限','''nomenu'',''submenu00'',''submanager01'',''submanager02'',''submanager03''');

--修改菜单图标
UPDATE MENU T
SET
  T.PARENTMENUICON ='fa fa-tachometer fa-fw"'
WHERE
  PARENTORDERID ='menu00';

UPDATE MENU T
SET
  T.PARENTMENUICON ='fa fa-gears fa-fw"'
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


update menu t set t.parentmenuicon ='fa fa-tachometer fa-fw"' where parentorderid ='menu00';
update menu t set t.parentmenuicon ='fa fa-gears fa-fw"' where parentorderid ='submanager01';

desc tusers;

alter table tusers alter column userpassword  TYPE VARCHAR(100);

--------------------------------------


--20231001 菜单设置
--新的菜单
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submenu02','美国','usa.html','美国','fa region-usa fa-fw','menu05',1);
Insert into MENU (MENUID,MENUNAME,MENUURL,PARENTMENUNAME,PARENTMENUICON,PARENTORDERID,SUBORDERID) values ('submenu04','中国','china.html','中国','fa region-china fa-fw','menu08',1);



Insert into URLS (MENUID,ACCESSURL) values ('submenu02','usa.html');
Insert into URLS (MENUID,ACCESSURL) values ('submenu04','china.html');


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
  'submenu03',
  '自选股',
  'userportfolio.html',
  '自选股',
  'fa fa-bar-chart-o fa-fw',
  'menu10',
  1
);

INSERT INTO URLS (
  MENUID,
  ACCESSURL
) VALUES (
  'submenu03',
  'userportfolio.html'
);

INSERT INTO URLS(
    MENUID,
    ACCESSURL
) VALUES(
    'submenu03',
    'proc_userportfolio'
);



--新建用户自选股组合表
CREATE TABLE USERPORTFOLIO(
    PORTFOLIOID   INTEGER    PRIMARY KEY,   -- 组合ID
    USERID        VARCHAR(10),              -- 用户ID
    PORTFOLIONAME VARCHAR(50),              -- 组合名称
    STOCKNUM INTEGER                       -- 股票数量
    
);

--新建用户自选股股票表
CREATE TABLE USERPORTFOLIOSTOCK(
    PORTFOLIOID   INTEGER,                 -- 组合ID
    STOCKCODE     VARCHAR(10),             -- 股票代码    
    STOCKNAME     VARCHAR(200),            -- 股票名称
    INSERTDATE    TIMESTAMP,               -- 添加日期
    ORDERID      INTEGER,                  -- 排序号
    PRIMARY KEY(PORTFOLIOID, STOCKCODE)    -- 主键(组合ID，股票代码)
);


--创建组合ID序列
CREATE SEQUENCE SEQ_PORTFOLIOID
    START WITH 1 --从1开始
    INCREMENT BY 1 --每次递增1
    NO MINVALUE --无最小值
    NO MAXVALUE --无最大值
    CACHE 1;

/* 测试代码 */
--insert into userportfolio values(nextval('SEQ_PORTFOLIOID'),'001','test1',0,'');

--delete from userportfolio ;

--select * from userportfolio;
select * from urls where MENUID='submenu03';

select * from stockinfo where stockcode ='NVDA';

insert into USERPORTFOLIOSTOCK values(10,'AAPL','苹果',now(),1);
insert into USERPORTFOLIOSTOCK values(10,'MSFT','微软',now(),2);
insert into USERPORTFOLIOSTOCK values(10,'NVDA','英伟达',now(),3);

update USERPORTFOLIO set stocknum =3 where portfolioid =10;

update urls set menuid ='submenu04' where accessurl ='china.html';

/* 测试代码 */

/* 20231108 菜单API设置 */
--新的URL
select * from menu ;
select * from urls where menuid ='submanager01';
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','addUserBtn');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','getAllRoleData');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','restPwdBtn');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','updateUserBtn');
Insert into URLS (MENUID,ACCESSURL) values ('submanager01','refreshCacheBtn');

select * from menu;
select * from urls where menuid ='submanager02';
Insert into URLS (MENUID,ACCESSURL) values ('submanager02','getAllRoleData2');


update menu set parentorderid = 's_manager01' where parentorderid ='submanager01';  


ALTER TABLE USERROLE DROP CONSTRAINT userrole_pkey;

select * from tusers;
select * from userrole where userid ='001';

select * from company c ;
select * from companyuser c  ;

select t.userrealname, tt4.companyname from tusers t left join (
select cc.userid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4 
on t.userid = tt4.userid ;


/* 20231125 新添加菜单 openai任务管理 */
select * from tusers;
select * from menu;
insert into menu values(
  'submanager04',
  'openai任务管理',
  'manageopenaitask.html',
  '后台管理',
  'fa fa-gears fa-fw',
  's_manager01',
    4
);

insert into urls values('submanager04','manageopenaitask.html');
insert into urls values('submanager04','procOpenAITaskInfo');

select * from topenaitask order by task_id desc;
select * from topenaiqueue order by id;

select * from privrole ;
select * from urls where menuid ='submanager02';
insert into urls values('submanager02','CorrectionMenuPriv');


