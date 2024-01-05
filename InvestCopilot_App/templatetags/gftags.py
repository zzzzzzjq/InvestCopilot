from InvestCopilot_App.models.cache.dict import dictCache as cache_dict
from django import template
from django.utils.safestring import mark_safe

#from InvestCopilot_App.models.cache.mode import stockBaseCache as cache_mode_base
from InvestCopilot_App.models.toolsutils import CacheUtils as cache_util
from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
#from InvestCopilot_App.stock import stockbase as stock_base

register = template.Library()


# 金额格式化，保留2位小数
def floatFormatTag(value, size=0):
    try:
        if 0 == size:
            return round(float(value))  # 取整，四舍五入
        return round(float(value), size)
    except Exception as ex:
        return value
"""
def formatDigit2PercentStyle(value):
    '''
    数据添加百分号
    '''
    value1 = str(value)
    if value1 == stock_utils.DEFAULT_VALUE:
        return value1
    return ''.join([value1,'%'])
"""

# 取模运算
def modTag(value, modevalue=4):
    try:
        return value % modevalue == 0
    except Exception as ex:
        return value
"""
# A 表示A股股票代码， ALL表示 A股+美股+港股代码
def stockoptionTag(value='A'):
    try:
        result = cache_mode_base.getStockCodeListCache(value)
        # return "".join(result)
        return mark_safe("".join(result))
    except Exception as ex:
        return ""

def staticPicTag(stockCode, imgType='FORWORD_PETTMFY12'):
    # 静态图片 base64串，入参：stockCode股票代码,imgType:因子图片类型。
    try:
        stockCode = stockCode[0:6]
        result = cache_util.getStaticPicSrc(stockCode, imgType)
        if result is None:
            return stock_utils.NOTHING_PIC  # 图片不存在
        return result
    except Exception as ex:
        return stock_utils.NOTHING_PIC  # 图片不存在

"""
# 获取数据字典 ，keyNo字典编号   sysdictionary表
def dictionaryTagList(keyNo):
    try:
        result = cache_dict.getDictionaryData(keyNo)
        return result
    except Exception as ex:
        return ""


# 获取数据字典 ，keyNo字典编号   sysdictionary表
def dictionaryTagOption(keyNo):
    try:
        result = cache_dict.getDictionaryData(keyNo, option=True)
        # return "".join(result)
        return mark_safe("".join(result))
    except Exception as ex:
        return ""

"""
# 获取自定义因子分数
def selfFactorScoreTag(stockCode):
    # 传入参数stockCode可以是单个股票点或list
    # 返回结果为[{dict}...]
    try:
        result = stock_base.getSelfFactorValue(stockCode)
        return result
    except Exception as ex:
        return []
"""
def md5Tag(value):
    return tools_utils.md5(value)

def operaBusinCodeTag(request, busincode):
    from InvestCopilot_App.models.user.UserInfoUtil import UserInfoUtil

    UserInfoUtil().saveUserLog(request, busincode)
    return ""


# 通过下标获取list值
def listValueByIndexTag(listData, index):
    try:
        if bool(listData):
            if (index > -1) and (index <= len(listData) - 1):
                return listData[index]
            else:
                return ""
        return ""
    except Exception as ex:
        return ""


# 通过key标获取字典值
def dictValueBykeyTag(dictData, key):
    try:
        if bool(dictData):
            if key in dictData.keys():
                return dictData[key]
            return ""
        return ""
    except Exception as ex:
        return ""
"""
# 通过key标获取字典值
from InvestCopilot_App.stock import stockutils as stock_utils

datadictTag = {'TABLE_TD': stock_utils.TABLE_TAG_COLOR}  # 打分颜色标签


def showColorTag(score):
    try:
        score = float(score)
        tag = stock_utils.countPercent(score)
        return datadictTag['TABLE_TD'][tag]
    except Exception as ex:
        return "#ffffff"

"""

# 乘以100，保留两位小数，加上百分号
def multiPercentTag(value):
    try:
        value = float(str(value))
        value = round(value * 100, 2)
        return str(value) + '%'
    except Exception as ex:
        return value


@register.filter(is_safe=False)
def subTag(value, arg):
    """Adds the arg to the value."""
    try:
        return int(value) - int(arg)
    except (ValueError, TypeError):
        try:
            return value - arg
        except Exception:
            return ''


class SetVarNode(template.Node):
    def __init__(self, var_name, var_value):
        self.var_name = var_name
        self.var_value = var_value

    def render(self, context):
        try:
            value = template.Variable(self.var_value).resolve(context)
        except template.VariableDoesNotExist:
            value = ""
        context[self.var_name] = value
        return u""


def set_var(parser, token):
    """
        {% set <var_name>  = <var_value> %}
    """
    parts = token.split_contents()
    if len(parts) < 4:
        raise template.TemplateSyntaxError("'set' tag must be of the form:  {% set <var_name>  = <var_value> %}")
    return SetVarNode(parts[1], parts[3])

def getMenu(menu,flag):
    """
    获取菜单
    """
    parentMenuName = list(menu.keys())[0]  #一级菜单名称
    subMenuList = list(menu.values())[0]  #二级菜单列表
    if flag == 'parent':
        return parentMenuName
    elif flag == 'subList':
        return subMenuList
    elif flag == 'icon':
        return subMenuList[0]['parentMenuIcon']

def formatDate(value):
    """
    格式化日期
    """
    return tools_utils.dateFormat(value,"/")
    # return '-'.join([value[0:4],value[4:6],value[6:]])

def formatDate2ChineseStyle(value):
    """
    将日期格式化成中国风格式
    """
    year = value[0:4]
    month = value[4:6]
    day = value[6:]
    if month[0:1] == '0':
        month = month[1:]
    if day[0:1] == '0':
        day = day[1:]
    dateStr = year+'年'+month+'月'+day+'日'
    return dateStr

"""
def formatCharacter(value):
    '''
    格式化文字
    '''
    if value is None:
        return stock_utils.DEFAULT_VALUE
    else:
        return value
"""
def getRoleMenu(value,flag):
    """
    获取角色菜单使用的过滤器
    """
    if flag == 'parentMenu':
        return list(value.keys())[0]
    elif flag == 'subMenu':
        return list(value.values())[0]


def jsVersion(value):
    "js/css 版本设置"
    vserion="5.0"
    return vserion

register.filter('floatFormatTag', floatFormatTag)
register.filter('md5Tag', md5Tag)
register.filter('modTag', modTag)
#register.filter('stockoptionTag', stockoptionTag)
register.filter('listValueByIndexTag', listValueByIndexTag)
register.filter('dictValueBykeyTag', dictValueBykeyTag)
register.filter('operaBusinCodeTag', operaBusinCodeTag)
#register.filter('showColorTag', showColorTag)
register.filter('multiPercentTag', multiPercentTag)
register.filter('dictionaryTagOption', dictionaryTagOption)  # 字典列表获取,用于select option展示
#register.filter('dictionaryTagList', dictionaryTagList)  # 字典列表获取
#register.filter('staticPicTag', staticPicTag)  # 静态图片路径
#register.filter('selfFactorScoreTag', selfFactorScoreTag)  # 自定义因子值获取
register.tag('set', set_var)  # 页面设置值
register.filter('getMenu',getMenu)
register.filter('formatDate',formatDate)
register.filter('formatDate2ChineseStyle',formatDate2ChineseStyle)
#register.filter('formatDigit2PercentStyle',formatDigit2PercentStyle)
#register.filter('formatCharacter',formatCharacter)
register.filter('getRoleMenu',getRoleMenu)
register.filter('jsVersion',jsVersion)

if __name__ == '__main__':
    # data= round_0(12.53)
    # data = mod(100,13)
    # data = round_0('N/A')
    # print(getstockoption('A'))

    # from login.user.UserInfoUtil import UserInfoUtil
    # userin = UserInfoUtil()
    # listData=[1,2,3,4]
    # print(listValueByIndexTag(listData,-1))
    #
    # dictdata1={'EVE': ['EVE描述', 5, 2.4294156271], 'EQUAL_WEIGHTED': 23.487635108399999, 'PCF': ['PCF描述', 4, 35.233494363900007], 'TABLE_TD': {1: '#D35F10', 2: '#B47F1A', 3: '#1B0FB0', 4: '#1B82B4', 5: '#4AF330'}, 'factorsdesc': {'EVE': 'EVE描述', 'EPE': '描述', 'PCF': 'PCF描述', 'DY': 'DY描述', 'PB': 'PB描述', 'EVS': 'EVS描述'}, 'DY': ['DY描述', 0, 0.0], 'STOCKCODE': '000005', 'PB': ['PB描述', 4, 31.368454866699999], 'EVS': ['EVS描述', 5, 11.851608667099999], 'PE': 52.682300032100002, 'factors': ['EVS', 'EVE', 'PE', 'PCF', 'PB', 'DY']}
    # listdata=dictdata1['factors']
    #
    # print(listdata)
    # key = (listValueByIndexTag(listdata,2))
    # print(key)
    # value=(dictValueBykeyTag(dictdata1,key))
    # print(value)
    # value=(listValueByIndexTag(value,2))
    # value1=95.0123444
    # value2=95.0123444
    # value1=(floatFormatTag(value1,0))
    # value2=(floatFormatTag(value2,3))
    # print(value1)
    # print(value2)

    # print(floatFormatTag(168.0, 0))
    # print(round(float(168.01)))
    # print(staticPicTag('002879', 'xxx'))
    print(md5Tag('002879xxx'))
    pass
