__author__ = 'Robby'
"""
    缓存基础数据结构封装
"""
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.models.multiset import MultiSetAPIMode as api_mode
from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData

Logger = logger_utils.LoggerUtils()

# 菜单树结构处理
# def getChild(menuId, rootMenu, childMenusList):
def getChild(menuId, rootMenu, menuFactos, childMenusList):
    childMenus = []
    # 二级菜单
    for idx, row in rootMenu.iterrows():
        if row.PARENTID == menuId:
            childMenusDict = {}
            childMenusDict['menuId'] = row.MENUID
            childMenusDict['menuName'] = row.MENUNAME
            factorList = menuFactos.loc[menuFactos['MENUID'] == row.MENUID]
            childMenusDict['factorNoList'] = factorList['FACTORNO'].values.tolist()
            childMenusDict['factorDescList'] = factorList['FACTORDESC'].values.tolist()
            # 搜索（英文字母或中文）
            childMenusDict['factorSearchList'] = factorList['FACTORSEARCH'].values.tolist()

            childMenus.append(childMenusDict)

    # 三级+N级菜单数据结构处理
    if len(childMenusList) > 0:
        for c in childMenusList:
            if c['menuId'] == menuId:
                c['childMenus'] = childMenus
    else:
        # 二级菜单
        childMenusList = childMenus

    # 三级+N级菜单
    for meus in childMenus:
        meusT = meus.copy()
        for key, value in meusT.items():
            rootMenuT = rootMenu.loc[rootMenu['PARENTID'] == meusT['menuId']]
            if not rootMenuT.empty:
                # 递归调用
                # getChild(value, rootMenu, childMenusList)
                getChild(value, rootMenu, menuFactos, childMenusList)

    return childMenusList


def getFactorMenus(menuType):
    # 获取因子自定义分类菜单
    resultData = ResultData()
    try:
        # 菜单类型
        if menuType == '':
            menuType = '1'  # 指标类型：1：旧指标；2：新指标

        # 菜单数据结构处理
        rootMenu = api_mode.getFactorMenu(menuType)
        parentMenu = rootMenu.loc[rootMenu['PARENTID'] == '#']
        parentMenuDict = tools_utils.dfToListInnerDict(parentMenu)

        # 获取每项菜单对应的因子列表数据
        menuFactos = api_mode.getMenufactorsCfg()
        menusTreeList = []
        for idx, row in parentMenu.iterrows():
            menusListDict = {}
            menusListDict['menuId'] = row.MENUID
            menusListDict['menuName'] = row.MENUNAME
            # childMenus = getChild(row.MENUID, rootMenu, {})
            childMenus = getChild(row.MENUID, rootMenu, menuFactos, {})
            if len(childMenus) > 0:
                menusListDict['childMenus'] = childMenus

            menusTreeList.append(menusListDict)

        resultData.menusTreeList = menusTreeList
        resultData.parentMenuDict = parentMenuDict

    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='抱歉，获取因子分类数据失败，请稍后重试。')

    return resultData


def set_subMenus(id, menus):
    """
    根据传递过来的父菜单id，递归设置各层次父菜单的子菜单列表

    :param id: 父级id
    :param menus: 子菜单列表
    :return: 如果这个菜单没有子菜单，返回None;如果有子菜单，返回子菜单列表
    """
    # 记录子菜单列表
    subMenus = []
    # 遍历子菜单
    for idx ,row in menus.iterrows():
        if row.PARENTID==id:
            menusListDict = {}
            menusListDict['menuId'] = row.MENUID
            menusListDict['menuName'] = row.MENUNAME
            subMenus.append(menusListDict)

    # 把子菜单的子菜单再循环一遍

    for menusListDict in subMenus:
        menus2 = api_mode.getStockRangeMenu(menusListDict['menuId'])
        # 还有子菜单
        if not menus2.empty:
            menusListDict['childMenus'] = set_subMenus(menusListDict['menuId'], menus2)

    # 子菜单列表不为空
    if len(subMenus) >0 :
        return subMenus
    else:  # 没有子菜单了
        return None


def buildStockRangeMenu(parentId="#"):
    # 构建股票范围分类菜单
    resultData = ResultData()
    try:
        # 菜单数据结构处理
        rootMenu = api_mode.getStockRangeMenu(parentId)
        menusTreeList = []
        for idx, row in rootMenu.iterrows():
            menusListDict = {}
            menusListDict['menuId'] = row.MENUID
            menusListDict['menuName'] = row.MENUNAME

            #得到子菜单
            childMenusDF = api_mode.getStockRangeMenu(row.MENUID)
            subMenus = set_subMenus(row.MENUID,childMenusDF)
            menusListDict['childMenus'] = subMenus

            menusTreeList.append(menusListDict)

        # print('menusTreeList:', menusTreeList)
        resultData.menusTreeList = menusTreeList
        # resultData.parentMenuDict = parentMenuDict

    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='抱歉，获取股票范围分类数据失败，请稍后重试。')

    return resultData

def buildStockRangeMenuv2(parentId="#"):
    resultData = ResultData()
    try:
        topMenuInfoPdData = api_mode.getStockRangeMenu(parentId)  #顶级菜单
        topMenuList = [{'value':row.MENUID,'id':row.MENUID,'text':row.MENUNAME, "state": {"opened" : 'true'}} for idx,row in topMenuInfoPdData.iterrows()]
        menusTreeList = getAllMenuList(topMenuList)
        resultData.menusTreeList=menusTreeList
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='抱歉，获取股票范围分类数据失败，请稍后重试。')

    return resultData

allMenuList = []  #菜单列表全局变量
allSubMenuList = []  #所有子菜单列表的全局变量
def getAllMenuList(topMenuList):
    """
    获取所有的文件菜单
    """
    global allMenuList
    allMenuList.clear()
    allSubMenuList.clear()
    getAndSetAllFileSubMenu(topMenuList,isFirstTopMenu = 1)
    return allMenuList

def getAndSetAllFileSubMenu(topMenuList,isFirstTopMenu):
    """
    获取并设置所有的文件子菜单 递归调用
    isFirstTopMenu 为1表示是处理顶级菜单
    isFirstTopMenu 为0表示是处理子菜单
    """
    global allSubMenuList
    for menu in topMenuList:
        menuId = menu['value']
        subMenuPdData =  api_mode.getStockRangeMenu(parentId=menuId)  #获取子菜单
        if not subMenuPdData.empty:  #有子菜单
            subMenuList = [{'value':row.MENUID,'id':row.MENUID,'text':row.MENUNAME,"state": {"opened" : 'true'}} for idx,row in subMenuPdData.iterrows()]
            menu['children'] = subMenuList
            allSubMenuList.extend(subMenuList)
            if menu not in allSubMenuList:  #排除前面已经处理过的子菜单
                allMenuList.append(menu)
            getAndSetAllFileSubMenu(subMenuList,isFirstTopMenu = 0)
        else:
            if isFirstTopMenu == 1:
                allMenuList.append(menu)



if __name__ == '__main__':
    # data = buildStockRangeMenu()
    # print(data.menusTreeList)
    # print(data.parentMenuDict)

    df = ResultData()
    # data1 = getFactorMenus(menuType='1')
    # data2 = getFactorMenus(menuType='2')
    # df.oldFactors=data1.toDict()
    # df.newFactors=data2.toDict()
    # import json
    # print(df.toDict())
    # ks =json.dumps(df.toDict())
    # print(ks)
    data = buildStockRangeMenuv2('0102')
    print(data.toDict())