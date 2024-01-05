__author__ = 'Robby'

import json
# from InvestCopilot_App.models.toolsutils import MenuUtils as menu_utils
from django.shortcuts import render


class ResultData(object):
    # errorCode='0000000' 状态码 000000表示处理成功
    # errorFlag=True  处理状态
    # errorMsg=''  错误信息
    def __init__(self, errorCode='0000000', errorFlag=True, errorMsg='',translateCode='', data={}):
        self.errorCode = errorCode
        self.errorFlag = errorFlag
        self.translateCode = translateCode
        self.errorMsg = errorMsg
        self.data = data

    def errorData(self, errorCode='0000001', errorFlag=False, errorMsg='', translateCode='', data={}):
        self.errorCode = errorCode
        self.errorFlag = errorFlag
        self.translateCode = translateCode
        self.errorMsg = errorMsg
        self.data = data
        return self

    def loginTimeout(self, errorCode='00001010', errorFlag=False, errorMsg='登录已超时，请重新登录！', translateCode='loginTimeout', data={}):
        self.errorCode = errorCode
        self.errorFlag = errorFlag
        self.errorMsg = errorMsg
        self.translateCode = translateCode
        self.data = data
        return self

    def setMenus(self, Leve1Menu, Leve2Menu, user_privilegeset):
        menus = menu_utils.menusListToHtml(Leve1Menu, Leve2Menu, user_privilegeset)
        self.menus_list_str = menus
        return self

    def buildRender(self, request, returnPage, cookies={}, params={}):
        #'stock/models.html'
        returnDict= {}
        for rparams in params:
            returnDict[rparams] = params[rparams]
        selfDic =self.toDict()
        for rparams in selfDic:
            returnDict[rparams] = selfDic[rparams]
        rtrender = render(request, returnPage, returnDict)
        # 添加cookies
        for key, value in cookies.items():
            rtrender.set_cookie(key, cookies[key])
        return rtrender
    
    
    def buildErrorRender(self, request, returnPage='error_500.html'):
        #'stock/models.html'
        return render(request, returnPage, self.toDict())


    def toDict(self):
        # 将对象转换为字典格式 返回值
        # return tools_utils.beanToDict(self)
        #将对象中非data,errorCode,errorFlag之外的对象全移到data中
        #判断是否有data对象
        if hasattr(self, 'data') and self.data != {}:            
            return self.__dict__
        
        data ={}        
        deletekeys = []
        
        for key in self.__dict__:
            if key not in ['data','errorCode','errorFlag','errorMsg','translateCode']:
                data[key] = self.__dict__[key]
                deletekeys.append(key)
        #删除已经移动到data中的对象
        for key in deletekeys:
            self.__dict__.pop(key)
                
        self.data = data
        return self.__dict__

    def toJson(self):
        # 将对象转换为Json格式
        # return tools_utils.beanToJson(self)
        #将对象中非data,errorCode,errorFlag之外的对象全移到data中
        
        if hasattr(self, 'data') and self.data != {}:
            return json.dumps(self.__dict__)
        
        data = {}
        deletekeys = []
        for key in self.__dict__:
            if key not in ['data','errorCode','errorFlag','errorMsg','translateCode']:
                data[key] = self.__dict__[key]
                deletekeys.append(key)
        #删除已经移动到data中的对象
        for key in deletekeys:
            self.__dict__.pop(key)

        self.data = data
        return json.dumps(self.__dict__)


if __name__ == '__main__':
    # result = ResultData(errorCode='0000001', errorFlag=False)
    result = ResultData()
    print(result.errorCode)
    print(result.errorFlag)
    result.data = ['a', 'b', 'c']
    print(result.data)
    print(type(result.__dict__))

    print(result.toDict())
    print(result.toJson())
    jsonbean = json.dumps(result.__dict__)
    # print(tools_utils.jsonToBean(ResultData, jsonbean).data)
    # print(jsonbean)
    # myben = json.loads(jsonbean)
    # result1 = ResultData()
    # result1.__dict__ = myben
    # print(result1.data)