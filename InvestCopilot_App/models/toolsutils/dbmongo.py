# file:
# MongoDB

import socket
import pymongo

#/usr/local/mongodb/bin
#mongod -f mongodb.conf

class Mongo(object):
    def __init__(self, db_name):
        self.db_name = db_name
        self.db_host = "127.0.0.1"
        self.port = 27017
        if socket.gethostname() == 'iZ2vcc0k0a629n6e2al2udZ':
            self.db_host = "172.18.163.215"  # 阿里云内网
        elif socket.gethostname() == 'iZebwatodogxv8Z':
            self.db_host = "172.18.163.215"  #阿里云生产环境
        elif socket.gethostname() == 'WIN-BN8GBRE6EOE':
            self.db_host = "120.78.94.82"  #阿里云测试环境
            self.port=28099
        elif socket.gethostname() == 'iZtkd54y1pp0z4Z':
            self.db_host = "172.18.163.218"  #阿里云测试环境
            self.port=28099
        elif socket.gethostname() == 'robby.local':
            self.db_host = "192.168.2.6"  # robby 服务器
            self.db_host = "127.0.0.1" # robby mac 服务器
        # print("db_host:",self.db_host)
        self.client = pymongo.MongoClient('mongodb://' + self.db_host,self.port , #27017,#28039
                                          serverSelectionTimeoutMS=5000, socketTimeoutMS=5000)#,username="copilot",password="copilot@8800",
        # print('数据库已连接')
        self.db = self.client[self.db_name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print('关闭数据库连接')
        self.client.close()

    def hello(self):
        print('hello')

if __name__ == '__main__':
    with Mongo('website') as mongo:
        # some code here
        for cname in mongo.db.list_collection_names():
            print("cname:", cname)
            mycol = mongo.db[cname]
            fos = mycol.find()
            for o in fos:
                print(o)
