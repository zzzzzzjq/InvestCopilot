# coding:utf-8
__author__ = 'Rocky'
import os
import cx_Oracle
import base64
from pyDes import *
import configparser

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.conf import settings as settings


def getURL():
    """
    获取数据源连接串
    """
    try:
        cf = configparser.ConfigParser()
        cf.sections()
        file_abs_dir = os.path.dirname(os.path.abspath('.'))
        cffilename = file_abs_dir +'/InvestCopilot/config/config.ini'
        cf.read(cffilename)        
        key= settings.SYSCRYPTKEY           
        k = des(key, CBC, "\0\0\0\0\0\0\0\0")        
        ip = k.decrypt(base64.decodebytes(cf['CONNECT']['IP'].encode('ascii')), "*").decode('ascii')        
        listener = k.decrypt(base64.decodebytes(cf['CONNECT']['LISTENER'].encode('ascii')), "*").decode('ascii')
        sid = k.decrypt(base64.decodebytes(cf['CONNECT']['SID'].encode('ascii')), "*").decode('ascii')
        user = k.decrypt(base64.decodebytes(cf['CONNECT']['USER'].encode('ascii')), "*").decode('ascii')
        password = k.decrypt(base64.decodebytes(cf['CONNECT']['PASSWORD'].encode('ascii')), "*").decode('ascii')                
        return user, password, ip, listener, sid
    except Exception as err:
        print(err)
        print("加密数据库连接串失败[config.ini]")        
        try:
            user, password, ip, listener, sid = cf['CONNECT']['USER'], cf['CONNECT']['PASSWORD'], cf['CONNECT']['IP'], \
                cf['CONNECT']['LISTENER'], \
                cf['CONNECT']['SID']
            key= settings.SYSCRYPTKEY            
            k = des(key, CBC, "\0\0\0\0\0\0\0\0")
            cf.set('CONNECT', 'IP',
                   base64.encodebytes(k.encrypt(cf['CONNECT']['IP'].encode('ascii'), "*")).decode('ascii'))
            cf.set('CONNECT', 'LISTENER',
                   base64.encodebytes(k.encrypt(cf['CONNECT']['LISTENER'].encode('ascii'), "*")).decode('ascii'))
            cf.set('CONNECT', 'SID',
                   base64.encodebytes(k.encrypt(cf['CONNECT']['SID'].encode('ascii'), "*")).decode('ascii'))
            cf.set('CONNECT', 'USER',
                   base64.encodebytes(k.encrypt(cf['CONNECT']['USER'].encode('ascii'), "*")).decode('ascii'))
            cf.set('CONNECT', 'PASSWORD',
                   base64.encodebytes(k.encrypt(cf['CONNECT']['PASSWORD'].encode('ascii'), "*")).decode('ascii'))            
            fh = open(cffilename, 'w')
            cf.write(fh)                        
            return user, password, ip, listener, sid
        except Exception as err:
            print(err)            
            raise

def OraclePool_old():
    try:
        #无连接池的连接方式        
        user, password, ip, listener, sid = getURL()
        connection =cx_Oracle.connect(user,password,ip+':'+ listener + '/' + sid)
        return connection
    except Exception as err:
        print(err)
        raise


class OraclePool(object):
    __pool = None
    """连接池初始化"""

    def __new__(cls, *args, **kw):
        if not cls.__pool:
            user, password, ip, listener, sid = getURL()
            """
            mincached ：启动时开启的空连接数量
            maxcached ：连接池最大可用连接数量
            maxshared ：连接池最大可共享连接数量
            maxconnections ：最大允许连接数量
            blocking ：达到最大数量时是否阻塞
            maxusage ：单个连接最大复用次数
           """
            cls.__pool = PooledDB(creator=cx_Oracle, mincached=1, maxcached=40, user=user, password=password,
                                  dsn=ip + ":" + listener + "/" + sid)
        return cls.__pool.connection()

def connect():
    try:
        #20200814 经过测试连接池方式在程序自动更新重启过程中无法释放ORACLE connection 会导致数据库连接会话泄漏，修改不用。
     
        user, password, ip, listener, sid = getURL()
        connection =cx_Oracle.connect(user,password,ip+':'+ listener + '/' + sid)
        return connection
    except Exception as err:
        print(err)
        raise


if __name__ == '__main__':
    #print(getURL())
    con = connect()
    cursor = con.cursor()    
    sql_str = "select * from tcodeupdate"
    cursor.execute(sql_str)
    result = cursor.fetchall()
    print(result)
    