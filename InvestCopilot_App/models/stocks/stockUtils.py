#股票代码基础信息

import os
import traceback
import pandas as pd
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'InvestCopilot.settings')
from django.core.cache import cache

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import InvestCopilot_App.models.cache.cacheDB as cache_db
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict

Logger = logger_utils.LoggerUtils()

# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10

# snap_redis = cacheTools(decode_responses=False)

KEYFIX="copilot_"
class stockUtils:
    def formateStockArea(self,value):
        if value == 'CH':
            return '1'
        elif value == 'HK':
            return '2'
        else:
            return '3'

    def stockSearch(self,searchStr,flagALL="A",vName="0", reload=False):
        """
        股票代码搜索
        flagAll: A ch股，H:港股 U:美股 ALL:所有
        vName: 1：显示公司名字；2：股票代码+公司名字 0：公司名字+搜索拼音简写
        """
        resultData = ResultData()
        try:
            # print("getWindStockDictData:",request.POST)
            # search = request.POST.get('search', '')
            # flagALL = request.POST.get('flagAll', 'A')  # 支持股票的标识  默认为A股
            # vName = request.POST.get('vName', '0')  # 支持转债的标识  默认为不支持
            dataList = []
            if searchStr is None or len(searchStr) == 0:
                resultData.data =dataList
                return resultData
            search = str(searchStr).lower().lstrip()
            allStockPdData = cache_db.getStockInfoCache(flage=flagALL, reload=reload)
            allStockPdData = allStockPdData[['STOCKCODE','WINDCODE', 'SEARCH', 'STOCKNAME']]
            # allStockPdData['AREA'] = allStockPdData['AREA'].apply(lambda x: self.formateStockArea(x))
            ##.contains('A|B|C') 多个匹配
            # 如果是存英文，匹配股票代码 #SequenceMatcher(None, baseStr, compStr).ratio()
            # 先完全匹配
            allStockPdData1 = allStockPdData[(allStockPdData['STOCKCODE'] == str(search).upper())]#|(allStockPdData['WINDCODE'] == str(search).upper())
            allStockPdData = allStockPdData[allStockPdData['SEARCH'].str.contains(search)]
            # 相识度排序
            allStockPdData['comp'] = allStockPdData['SEARCH'].apply(lambda x: tools_utils.strSimilar(search, x))
            allStockPdData = allStockPdData.sort_values(by='comp', ascending=False)
            # stockPdData = stockPdData[~stockPdData['NAME'].str.contains('退市')]  #剔除退市
            count = 0
            # 限制返回个数
            rtCodes = []
            for idx, row in allStockPdData1.iterrows():
                if row.WINDCODE in rtCodes:
                    continue
                else:
                    rtCodes.append(row.WINDCODE)
                if str(vName) == "1":
                    dataList.append({'id': row.WINDCODE, 'text': row.STOCKNAME})
                elif str(vName) == "2":
                    dataList.append({'id': row.WINDCODE, 'text': row.WINDCODE + " " + row.STOCKNAME})
                else:
                    dataList.append({'id': row.WINDCODE, 'text': row.STOCKNAME, 'search': row.SEARCH_RES})
            for idx, row in allStockPdData.iterrows():
                if row.WINDCODE in rtCodes:
                    continue
                if str(vName) == "1":
                    dataList.append({'id': row.WINDCODE, 'text': row.STOCKNAME})
                elif str(vName) == "2":
                    dataList.append({'id': row.WINDCODE, 'text': row.WINDCODE + " " + row.STOCKNAME})
                else:
                    dataList.append({'id': row.WINDCODE, 'text': row.STOCKNAME, 'search': row.SEARCH_RES})
                if count > 20:
                    break
                count += 1
            resultData.data = dataList
            return resultData
        except Exception as ex:
            Logger.errLineNo()
            resultData.errorData(errorMsg='抱歉，获取数据失败，请稍后重试。')
            return resultData

    def getStockInfo(self,windCode,flage='ALL', reload=False):
        stockInfoDT = cache_dict.getStockInfoDT(flage=flage, reload=reload)
        if windCode in stockInfoDT:
            return stockInfoDT[windCode]
        else:
            return {'Windcode':windCode }

if __name__ == '__main__':
    # s = stockUtils().getStockInfoCache(flage='ALL', reload=True)
    # s = stockUtils().stockSearch("AAPL",flagALL='ALL',vName='2',reload=True)
    # stockInfo_dt = cache_dict.getStockInfoDT()
    # print(stockInfo_dt)
    pass