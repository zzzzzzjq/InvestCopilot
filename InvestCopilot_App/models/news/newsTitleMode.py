
import json
import math
import time
import traceback
import datetime
import re
import socket
import os
import sys
import requests
import pandas as pd
import math
import threading
from pymongo import UpdateOne,UpdateMany,ReplaceOne

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from bson.son import SON

sys.path.append("../..")
# import login.stock.stockutils as stock_utils
# import login.toolsutils.ToolsUtils as tool_utils
# import numpy as np
# import pandas as pd
# import decimal
# import login.cache.cacheDB as cache_db
# import login.cache.dataSynTools.cacheTools as redis_tools
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.user.userMode import cuserMode
from  InvestCopilot_App.models.portfolio.portfolioMode import cportfolioMode
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
from InvestCopilot_App.models.business.likeMode import likeMode

import logging
from InvestCopilot_App.models.toolsutils import dbmongo
import InvestCopilot_App.models.toolsutils.dbutils as dbutls

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()
class newsTitleMode():

    def getTitles(self,userId):
        q_title = """ select title_id,title_zh,title_en,ttype,tsource,tord  from business.news_title_config where userid = %s 
                     union  select  title_id,title_zh,title_en,ttype,tsource,tord  from business.news_title_config where (userid = %s and userid!=%s)  order by tord     """
        dataDF = dbutls.getPDQueryByParams(q_title,params=[tools_utils.globa_default_template_userId,userId,tools_utils.globa_default_template_userId])
        return dataDF

    def getNewsTitle(self,userId,translation='zh'):
        newsTitleDF=self.getTitles(userId)
        newsTitles=[]
        for nt in newsTitleDF.itertuples():
            title_name=nt.TITLE_ZH
            if translation=='en':
                title_name = nt.TITLE_EN
            newsTitles.append({'titleTag':nt.TITLE_ID,"titleName":title_name,"tType":nt.TTYPE,"tSource":nt.TSOURCE})
        return newsTitles
    def getTitleStocks(self,title_id):
        q_codes="select  windcode  from business.news_title_stocks where title_id=%s"
        dataDF = dbutls.getPDQueryByParams(q_codes, params=[title_id])
        return dataDF
if __name__ == '__main__':


    # s=newsTitleMode().getTitles("1769")
    # print(s)
    # s=newsTitleMode().getNewsTitle("80",translation='en')
    s=newsTitleMode().getTitleStocks("sg_SPV")
    print(s)

