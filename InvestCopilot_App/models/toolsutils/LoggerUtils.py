__author__ = 'Robby'

import traceback
import logging

__metaclass__ = type


class LoggerUtils(logging.Logger):
    def __init__(self):
        self.logger = logging.getLogger('InvestCopilotLogger')

    def getLogger(self):
        return self.logger

    def info(self, msg, *args, **kwargs):
        self.getLogger().info(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.getLogger().debug(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.getLogger().error(msg, *args, **kwargs)


    # 发生异常错误行号
    def errLineNo(self,ex=Exception,msg=""):
        # s = sys.exc_info()
        # errclass = s[0]
        # errmsg = s[1]
        # errstack = s[2]
        # errlineno = ''
        # if errmsg is not None:
        #     errlineno = errstack.tb_lineno  # 发生异常错误行号
        print(traceback.format_exc())
        self.error("%s 错误堆栈信息:%s" % (msg+"》",traceback.format_exc()))


if __name__ == '__main__':
    nb = LoggerUtils()
    try:
        nb.getLogger().error('xxxxxxxxxxxxxx')
        strHello = "the length of (%s) is %d" % ('Hello World', len('Hello World'))

        nb.error("Error '%s' happened on 【line:%s】" % ('XXXXXXXXX', 'XXXXXXXXX'))
        print(strHello)
        int = 50 / 0
        # pass
    except Exception as ex:
        nb.errLineNo()
