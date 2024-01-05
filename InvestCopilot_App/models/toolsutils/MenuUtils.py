__author__ = 'Robby'

from django.core.cache import cache
import InvestCopilot_App.models.cache.cacheDB as cache_db
from django.utils.safestring import mark_safe
from collections import OrderedDict
from InvestCopilot_App.models.toolsutils import dbutils

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

def initNewMenusList(request):
    """
    格式化菜单数据为html
    """
    userId = request.session.get('user_id')
    #获取用户所拥有的菜单id
    userMenuIdPdData = cache_db.getUserMenuIdDF()        
    userMenuIdPdData = userMenuIdPdData.loc[userMenuIdPdData['USERID'] == userId]
    userMenuIdList = userMenuIdPdData['MENUIDLIST'].values.tolist()
    menuIdList =[]
    if len(userMenuIdList) == 0:
        menuIdList = []
    else:
        #20221226 修改为多角色获取最大集合菜单
        for s_userMenuList in userMenuIdList:
            menuIdList1 = s_userMenuList.split(',')
            for s_menuId in menuIdList1:
                if s_menuId not in menuIdList :
                    menuIdList.append(s_menuId)
    menuIdList = list(map(lambda x:x.replace("'",''),menuIdList))
    #获取所有的一级菜单(按照parentorderid排序)
    menuPdData = cache_db.getMenuDF()
    parentMenuPdData = menuPdData.loc[menuPdData['MENUID'].isin(menuIdList)]
    parentMenuPdData = parentMenuPdData[['PARENTMENUNAME','PARENTORDERID']]
    parentMenuPdData = parentMenuPdData.drop_duplicates('PARENTMENUNAME')
    parentMenuPdData = parentMenuPdData.sort_values('PARENTORDERID',ascending=True)
    parentMenuList = parentMenuPdData['PARENTMENUNAME'].values.tolist()
    #获取所有的二级菜单（按照menuid和parentorderid排序）
    subMenuPdData = menuPdData.loc[menuPdData['MENUID'].isin(menuIdList)]
    subMenuPdData = subMenuPdData.sort_values(['PARENTORDERID','SUBORDERID'],ascending=True)
    subMenuList = [{"menuId":row.MENUID,"menuName":row.MENUNAME,"menuUrl":row.MENUURL,
                    "parentMenuName":row.PARENTMENUNAME,"parentMenuIcon":row.PARENTMENUICON,
                    "subOrderId":row.SUBORDERID,"parentOrderId":row.PARENTORDERID} for idx,row in subMenuPdData.iterrows()]
    menuTreeList = []  #菜单树
    for parentMenu in parentMenuList:
        parentMenuName = parentMenu
        tmpList = []
        for subMenu in subMenuList:
            if subMenu["parentMenuName"] == parentMenuName:
                tmpList.append(subMenu)
        tmpDict = {parentMenuName:tmpList}
        menuTreeList.append(tmpDict)

    return {
        'menuTreeList':menuTreeList
    }




if __name__ == '__main__':
    # Leve1Menu = 'menu02'  # 一级菜单
    # Leve2Menu = ''
    # user_privilegeset = '1'
    # data = menusListToHtml(Leve1Menu, Leve2Menu, user_privilegeset)
    # print(data)
    rs = cache_db.getUserMenuIdDF()
    print(rs)
    pass