#This is FileCheckFile, no use! 
__author__ = 'Rocky'
import traceback
import cx_Oracle
import connect as conn
import os
import sys

def updateStatus():
    connection = conn.connect()
    cursor = connection.cursor()
    sqlstr = "update tcodeupdate set status='1' where status='0' and procid <>'000' "
    cursor.execute(sqlstr)
    connection.commit()

if __name__ == '__main__':
    updateStatus()