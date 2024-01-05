#用户信息处理
import sys

sys.path.append("../..")
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils

import logging
from InvestCopilot_App.models.toolsutils import dbmongo

Logger = logger_utils.LoggerUtils()

class cuserMode:

    def getUserCompanyRF(self):
        #获取用户与公司的关联关系
        SqlStr = "select u.userid,u.userrealname,c.shortcompanyname,username,c.companyid  from tusers u, companyuser cw, company c where u.userid =cw.userid  and cw.companyid =c.companyid   "
        usDF =dbutils.getPDQuery(SqlStr)
        return usDF

