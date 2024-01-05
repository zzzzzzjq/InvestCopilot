__author__ = 'Rocky'
"""
pgdbpool.py
postgresql Connect 
"""
import traceback
import os
import socket
import psycopg2
import base64
from pyDes import *
from dbutils.pooled_db import PooledDB

import configparser

from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils

LOGGER = logger_utils.LoggerUtils()

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.conf import settings as settings


def getEncodeDBParameter():
    """
    获取数据源连接串
    """
    try:
        cf = configparser.ConfigParser()
        cf.sections()
        current_path = os.path.abspath(__file__)
        current_path = os.path.split(current_path)[0]
        parent_directory = os.path.dirname(current_path)
        parent_directory = os.path.dirname(parent_directory)
        parent_directory = os.path.dirname(parent_directory)
        cffilename = parent_directory + '/config/config.ini'
        cf.read(cffilename)
        key = settings.SYSCRYPTKEY
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
            key = settings.SYSCRYPTKEY
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


def getCfg():
    """
    获取数据源连接串
    """
    try:
        selfname = os.path.basename(__file__)
        section = selfname.split('.')[0]
        config = configparser.ConfigParser()
        config.read('.\config.ini', encoding='UTF-8')

        user = config.get(section, 'user')
        password = config.get(section, 'password')
        ip = config.get(section, 'ip')
        listener = config.get(section, 'listener')
        sid = config.get(section, 'sid')

        return user, password, ip, listener, sid
    except Exception as err:
        LOGGER.error("数据库参数错误！")
        LOGGER.error(traceback.format_exc())
        return 'error', 'error', 'error', 'error', 'error'


class pgsqlPool(object):
    __pool = None
    """连接池初始化"""

    def __new__(cls, *args, **kw):
        if not cls.__pool:
            user, password, ip, listener, sid = getEncodeDBParameter()
            """
[CONNECT]
ip = 120.78.94.82 
listener = 15433 
sid = copilot 
user = copilot 
password = copilot123456
            """
            #print("user, password, ip, listener, sid:",user, password, ip, listener, sid)
            if user == 'error':
                raise Exception("数据库参数获取错误！")
            cls.__pool = PooledDB(
                creator=psycopg2,  # 使用链接数据库的模块mincached
                maxconnections=50,  # 连接池允许的最大连接数，0和None表示不限制连接数
                mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                maxcached=4,  # 链接池中最多闲置的链接，0和None不限制
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                setsession=[],  # 开始会话前执行的命令列表。
                host=ip,
                port=listener,
                user=user,
                password=password,
                database=sid)
        return cls.__pool.connection()


# 获得数据源和游标
def getConnect():
    try:
        connection = pgsqlPool()
        cursor = connection.cursor()
    except Exception as err:
        LOGGER.error('DBConnect Error:')
        LOGGER.error(traceback.format_exc())
        return None, None
    return connection, cursor


# 获得数据源
def getDBConnect():
    try:
        connection = pgsqlPool()
    except Exception as err:
        LOGGER.error('DBConnect Error:')
        LOGGER.error(traceback.format_exc())
        return None
    return connection


# 获得游标
def getDBCursor():
    try:
        connection = pgsqlPool()
        cursor = connection.cursor()
    except Exception as err:
        LOGGER.error('DBConnect Error:')
        LOGGER.error(traceback.format_exc())
        return None
    return cursor


if __name__ == '__main__':
    s = getEncodeDBParameter()
    print(s)
    con, cursor = getConnect()
    if con != None:
        sql = "select count(1) from  config.reuters_usstocks "
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
        cursor.close()
        con.close()



