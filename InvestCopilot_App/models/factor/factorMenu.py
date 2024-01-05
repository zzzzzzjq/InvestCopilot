#指标展示与搜索
import pandas as pd
import json
import os
import traceback
import pickle

import InvestCopilot_App.models.toolsutils.dbutils as dbutils
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as Logger_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
from django.http import JsonResponse

Logger = Logger_utils.LoggerUtils()

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10

KEYFIX="FACTOR"
class factorMenu():

    def getCategorySubIndex(self,parentId, indexType):
        #获取指标分类的子类
        #parentId 父ID，indexType：指标分类
        selectSql = 'SELECT t.menuid,t.menuname,t.ord,t.parentid,t.menutype  FROM MENUFACTOR t WHERE t.PARENTID =%s AND t.MENUTYPE =%s ORDER BY t.ORD ASC'
        data =dbutils.getPDQueryByParams(selectSql, params=[parentId, indexType])
        return data

    def getIndexTreeCache(self,indexType, reload=False):
        """
        获取指标树数据
        """
        key = KEYFIX + '_indexTree_' + str(indexType)
        allCategoryIndexList = cache.get(key)
        if allCategoryIndexList is None:
            reload = True
        if reload:
            topIndexCategoryPdData = self.getCategorySubIndex(parentId='#', indexType=indexType)
            print("topIndexCategoryPdData:",topIndexCategoryPdData)
            topIndexCategoryList = [{'id': row.MENUID, 'text': row.MENUNAME} for idx, row in
                                    topIndexCategoryPdData.iterrows()]
            print("topIndexCategoryList:",topIndexCategoryList)

            # allCategoryIndexList = manager_index_mode.getAllCategoryIndexList(topIndexCategoryList, indexType)
            # allCategoryIndexList = sorted(allCategoryIndexList, key=lambda x: x['id'])
            # cache.set(key, allCategoryIndexList, CACHEUTILS_UPDATE_SECENDS)
        return allCategoryIndexList

    def getMenuFactor(self,indexType,parentId="sm0", reload=False):
        #获取指标树数据
        rest=ResultData()
        try:
            menuPd = self.getCategorySubIndex(parentId=parentId, indexType=indexType)
            rtd = menuPd.rename(columns=lambda x: x.capitalize()).to_json(orient='records')
            # rtd =[]
            # for m in menuPd.itertuples():
            #     print(m)
            #     menuid = m.MENUID
            #     menuname =m.MENUNAME
            #     rtd.append({"menuId":menuid,'menuName':menuname,"factorNames":[{"factorNo":"","factorDesc":""}]})
            rest.data=json.loads(rtd)
            # 菜单下配置的指标
            # menuCfgDF = cache_db.getIndexMenuDF(reload=reload)
            # 指标明细信息
            # factorDF = cache_db.getFactorCellDF(reload=reload)
        except:
            msg = "抱歉，数据失败，请稍后再试！"
            Logger.errLineNo(msg=msg)
            rest.errorData(translateCode="searchFactorsError",errorMsg=msg)
            return rest
        return rest

    def factorCacheData(self,indexType, reload=False):
        qKey = 'FACTOR_CellDF_' + str(indexType)
        factorIndexDF = cache.get(qKey)
        isLock = False
        if factorIndexDF is None:
            isLock = True
            reload = True
            factorIndexDF = cache.get(qKey)
            if factorIndexDF is not None:
                reload = False
        if reload:
            factorDescData = cache_db.getFactorCellDF(reload)
            factorDescData = factorDescData[['FACTORNO', 'FACTORDESC', 'SEARCHKEY', 'FACTORTNAME', 'FDESC']]
            allIndexMenuPdData = cache_db.getIndexMenuDF(reload)
            indexMenuPdData = allIndexMenuPdData[allIndexMenuPdData['MENUTYPE'] == str(indexType)]
            indexMenuPdData['FACTORNO'] = indexMenuPdData['FACTORNO'].apply(lambda x: str(x))
            factorDescData = pd.merge(indexMenuPdData, factorDescData, how='inner', on='FACTORNO')
            # 括弧，英文转中文，兼容搜索
            factorDescData['SEARCHKEY'] = factorDescData['SEARCHKEY'].apply(
                lambda x: x.replace('(', '（').replace(')', '）'))
            # 中文 拼音 指标字段名（英文或别名）
            factorDescData['SEARCH'] = factorDescData[
                'SEARCHKEY']
            # 忽略大小写
            factorDescData['SEARCH'] = factorDescData['SEARCH'].str.upper()
            # 指标对应的菜单名称
            # factorDescData['MENUID']=factorDescData['FACTORNO'].apply(lambda x : menuFactorDict[x])
            # 删除重复记录
            factorDescData = factorDescData.drop_duplicates()

            # {'id': row.FACTORNO, 'text': row.FACTORDESC, 'search': row.SEARCH} for idx, row in
            # factorDescDF.iterrows()
            factorIndexDF = factorDescData
            cache.set(qKey, factorIndexDF, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
            if isLock:
                pass  # _lock.release()
        return factorIndexDF

    def getFactorByMenu(self,categoryId,indexType,searchKey=None,reload=False):
        #获取指标树数据
        rest=ResultData()
        try:
            # 指标分类大项
            indexPdData = self.factorCacheData(indexType,reload)
            # 指标分类菜单项
            indexPdData = indexPdData[indexPdData['MENUID'] == str(categoryId)]
            indexPdData = indexPdData.sort_values('ORD')
            if searchKey is not None:
                searchKey = str(searchKey).strip().upper()
                searchKey = searchKey.replace('(', '（').replace(')', '）')
                indexPdData = indexPdData[indexPdData['SEARCH'].str.contains(searchKey)]
                # indexPdData = indexPdData.loc[indexPdData['FACTORDESC'].str.contains(searchKey,case=True)]
            rtd = [{'id': row.FACTORNO, 'name': row.FACTORDESC, 'explain': row.FDESC} for idx, row in
                         indexPdData.iterrows()]
            rest.data=rtd
        except:
            msg = "抱歉，数据失败，请稍后再试！"
            Logger.errLineNo(msg=msg)
            rest.errorData(translateCode="searchFactorsError",errorMsg=msg)
            return rest
        return rest

    def searchFactors(self,searchKey,indexType,reload=False):
        #指标搜索
        rest=ResultData()
        try:
            searchKey = str(searchKey).strip().upper()
            searchKey = searchKey.replace('(', '（').replace(')', '）')
            fdt=[]
            allFactorDF = self.factorCacheData(indexType,reload)
            allFactorDF = allFactorDF[allFactorDF['SEARCH'].str.contains(searchKey)]
            allFactorDF['STR_SIMILAR'] = allFactorDF['FACTORDESC'].apply(
                lambda x: tools_utils.strSimilar(searchKey, x))
            allFactorDF = allFactorDF.sort_values('STR_SIMILAR', ascending=False)  # 按照字符串相似度从大到小排序
            allFactorDF = allFactorDF.drop(['STR_SIMILAR'], axis=1)
            menuIds={}
            for idx, row in allFactorDF.iterrows():
                fdt.append({'id': row.FACTORNO, 'name': row.FACTORDESC})
                menuIds[row.FACTORNO]=row.MENUID
            # 指标对应的目录
            # indexIdList = allFactorDF['FACTORNO'].tolist()
            # indexIdList = list(map(lambda x: str(x), indexIdList))
            # allFactorDF['FACTORNO'] = allFactorDF['FACTORNO'].apply(lambda x: str(x))
            # indexMenuPdData = allFactorDF.loc[allFactorDF['FACTORNO'].isin(indexIdList)]
            rest.factorData = fdt  # 搜索指标信息
            rest.factorMenu = menuIds  # 菜单目录编号（搜索的指标在哪些菜单目录下）
        except:
            msg = "抱歉，搜索失败，请稍后再试！"
            Logger.errLineNo(msg=msg)
            rest.errorData(translateCode="searchFactorsError",errorMsg=msg)
            return rest
        return rest

    def getFactorMenuCache(self,indexType,parentId="sm0", reload=False):
        """
        获取指标树数据
        """
        rest=ResultData()
        try:
            key = KEYFIX + '_indexTree_' + str(indexType)
            allCategoryIndexList = cache.get(key)
            if allCategoryIndexList is None:
                reload = True
            if reload:
                menuPdData = self.getCategorySubIndex(parentId=parentId, indexType=indexType)
                # 指标明细信息
                factorDF = cache_db.getFactorCellDF(reload=reload)
                factorDescData = factorDF[['FACTORNO', 'FACTORDESC', 'SEARCHKEY']]
                # 菜单下配置的指标
                menuCfgDF = cache_db.getIndexMenuDF(reload)
                indexMenuPdData = menuCfgDF[menuCfgDF['MENUTYPE'] == str(indexType)]
                indexMenuPdData['FACTORNO'] = indexMenuPdData['FACTORNO'].apply(lambda x: str(x))
                factorDescData = pd.merge(indexMenuPdData, factorDescData, how='inner', on='FACTORNO')
                allCategoryIndexList =[]
                for m in menuPdData.itertuples():
                    menuid = m.MENUID
                    menuname =m.MENUNAME
                    vfd = factorDescData[factorDescData['MENUID']==menuid].sort_values("ORD")
                    factorNames=vfd[['FACTORNO','FACTORDESC','SEARCHKEY']].rename(columns=lambda x: x.capitalize()).to_json(orient='records')
                    factorNames=json.loads(factorNames)
                    allCategoryIndexList.append({"menuId":menuid,'menuName':menuname,"factorNames":factorNames})
                cache.set(key, allCategoryIndexList, CACHEUTILS_UPDATE_SECENDS)
            rest.data= allCategoryIndexList
        except:
            msg = "抱歉，指标列表加载失败，请稍后再试！"
            Logger.errLineNo(msg=msg)
            rest.errorData(translateCode="getFactorMenuCacheError",errorMsg=msg)
            return rest
        return rest

if __name__ == '__main__':
    # factorMenu().getIndexTreeCache("4")
    # data=factorMenu().getMenuFactor("4")
    # data=factorMenu().getFactorByMenu("21031","4","Cash",reload=True)
    # data=factorMenu().searchFactors("Cash","4",reload=True)
    data=factorMenu().getFactorMenuCache( "4",reload=True)
    print("data:",data )
    # print((data.toDict()["data"]))