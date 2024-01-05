__author__ = 'Rocky'
from pyDes import des, CBC, PAD_PKCS5
import base64
import binascii
from django.conf import settings

from django.http import HttpResponseRedirect 
from InvestCopilot_App.user.UserInfoUtil import UserInfoUtil
import InvestCopilot_App.user.UserInfoUtil as user_utils

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import datetime,time

Logger = logger_utils.LoggerUtils()



def passWordEncode(passWord,key):
    # 加密
    secret_key=key
    iv=key    
    k = des(secret_key, CBC, iv, pad=None, padmode=PAD_PKCS5)
    en = k.encrypt(passWord, padmode=PAD_PKCS5)
    result= binascii.b2a_hex(en)
    return result.upper()


def passWordDecode(encodePassWord,key):
    # 解密
    secret_key = key
    iv = key
    k = des(secret_key, CBC, iv, pad=None, padmode=PAD_PKCS5)
    de = k.decrypt(binascii.a2b_hex(encodePassWord.lower()), padmode=PAD_PKCS5)
    return de

def passWordDecode2(encodePassWord,key):
    # 解密
    try:
        secret_key = key
        iv = key
        k = des(secret_key, CBC, iv, pad=None, padmode=PAD_PKCS5)
        de = k.decrypt(base64.b64decode(encodePassWord), padmode=PAD_PKCS5)
        print(de)
        return de
    except Exception as err:
        print(str(err))
        return ''


def ssologin(request):
    #http://localhost:8080/si_er/service/logon.action?direct=1&spirit=7E8BB69BEB375B2C&parameter=5A139717A99674BF78E4B50119866E1B
    key = settings.SSOLOGINKEY
        
    if request.method == 'GET':        
        s_parameter = request.GET.get('parameter',None)         
        if s_parameter =='' or s_parameter ==None:            
            return HttpResponseRedirect("/")        
        UserName =''
        UserId =''
        parameter=''
        try:
            parameter= str(passWordDecode(s_parameter,key),encoding ='utf-8')        
            allparams =parameter.split('&')
            params1= allparams[0].split('=')
            params2= allparams[1].split('=')
            if params1[0]=='username':
                UserName = params1[1]
                UserId = params2[1]
            else:
                UserInfoUtil().saveUserLog(request, "OA单点登录用户名或者ID错误", state='0', content='用户' + parameter + '单点登录失败！')
                #rs.errorData(errorMsg="单点登录用户名错误，请联系OA管理员确认登录用户名！")       
                #return JsonResponse(rs.toDict())
                return HttpResponseRedirect("/")
        except Exception as err:            
            Logger.errLineNo()            
            Logger.error("OA SSO登录参数获取失败:%s" % err)
            Logger.error("OA SSO登录加密源参数:%s" % s_parameter)
            Logger.error("OA SSO登录解密后源参数:%s" % parameter)
            return HttpResponseRedirect("/")
        Logger.info("OA SSO登录用户ID信息:")
        Logger.info([UserName, UserId])
        UserLogined = user_utils.ssouserlogin(UserName,UserId)    
        Logger.info("OA SSO登录用户登录结果信息:")
        Logger.info(UserLogined)
        if len(UserLogined) == 0:
            if request.session.get('userlogin', None) == 1:
                request.session['userlogin'] = 0

            UserInfoUtil().saveUserLog(request, "OA单点登录用户名或者ID错误", state='0', content='用户' + UserName + '单点登录失败！')
            #rs.errorData(errorMsg="单点登录用户名错误，请联系OA管理员确认登录用户名！")
            # return HttpResponse(['-100', '|', '登录失败，请重新输入用户名和密码！'])
            #return JsonResponse(rs.toDict())
            return HttpResponseRedirect("/")
        else:
            if (UserLogined[0][5] != 1):
                if request.session['userlogin'] == 1:
                    request.session['userlogin'] = 0

                UserInfoUtil().saveUserLog(request, "用户已锁定", state='0', content='用户' + UserName + '单点登录失败，用户已锁定，请联系管理员！')
                # return HttpResponse(['-101', '|', '登录失败，用户已锁定，请联系管理员！'])
                #rs.errorData(errorMsg='登录失败，用户已锁定，请联系管理员！')
                #return JsonResponse(rs.toDict())
                return HttpResponseRedirect("/")
            else:
                request.session['userlogin'] = 1
                request.session['user_id'] = UserLogined[0][0]
                request.session['user_roleid'] = UserLogined[0][3]
                request.session['userrealname'] = UserLogined[0][6]
                request.session['usernickname'] = UserLogined[0][7]

                UserInfoUtil().saveUserLog(request, "OA单点登录成功", state='1', content='OA单点登录成功！')
                redirectUrl = user_utils.getUserLoginRedirectUrl(UserLogined[0][0])
                #rs.redirectUrl = redirectUrl
                print(redirectUrl)
                if redirectUrl is None:
                    redirectUrl ='/'
                    #rs.redirectUrl = "/"
                    #rs.errorData(errorMsg="用户暂未分配权限，请联系管理员进行处理。")
                    return ''

                # print(['100', '|', '登录成功！','|',redirectUrl])
                return HttpResponseRedirect('/'+redirectUrl)

def getTimeInterval():
    #前一分钟
    begindatetime = (datetime.datetime.now()- datetime.timedelta(minutes=1)).strftime("%Y%m%d%H%M%S")
    #后三分钟
    enddatetime = (datetime.datetime.now()+ datetime.timedelta(minutes=3)).strftime("%Y%m%d%H%M%S")
    return begindatetime ,enddatetime
    

def irm_ssologin(request):
    #http://10.89.120.25/IRM_SSOLogin?parameter=1vDelkKbcHljSSXx9gN%2j2k5iMNrPqYRDm4TDBqNSKeyTAnhZaqB1Zo6Sbc%29N0ra1qLOe9t9Q1E%2l018vOCrrTWnJlqnAb0RNGJ3yawwf2yIsFzUqi5kWY03pZddkKEKMTqFG24SddT4zeXwRYVGaly8NGd56Jqj0qZhxLdxNBhXrXv8/cDurdOHaiV1FuVQk1qwoGUMK8=
    key = settings.IRM_SSOLOGINKEY
    sykey = settings.SYKEY
        
    if request.method == 'GET':        
        s_parameter = request.GET.get('parameter',None)   
        print('Get parameter:',s_parameter)      
        if s_parameter =='' or s_parameter ==None:            
            return HttpResponseRedirect("/")        
        UserName =''
        UserId =''
        irm_sykey=''
        logintime =''
        parameter=''
        redirect =''
        begintime=''
        endtime = ''
        begintime ,endtime = getTimeInterval()
        try:            
            parameter= str(passWordDecode2(s_parameter,key),encoding ='utf-8')   
            allparams =parameter.split('&')
            for params in allparams:
                param =  params.split('=')
                if param[0]=='username':
                    UserName = param[1]
                elif param[0]=='userid':
                    UserId = param[1]
                elif param[0]=='sykey':
                    irm_sykey = param[1]
                elif param[0]=='time':
                    logintime = param[1]
                elif param[0]=='redirect':
                    redirect =param[1]
            Logger.info('IRM_SSO_Login_Info:')
            Logger.info([UserName,UserId,irm_sykey,logintime,redirect])
            if UserName =='':            
                Logger.error("IRM SSO登录参数有误，用户名为空")
                UserInfoUtil().saveUserLog(request, "IRM单点登录用户名或者ID错误", state='0', content='用户' + parameter + '单点登录失败！')

                #rs.errorData(errorMsg="单点登录用户名错误，请联系IRM管理员确认登录用户名！")       
                #return JsonResponse(rs.toDict())
                return HttpResponseRedirect("/")
            if irm_sykey != sykey:
                Logger.error("IRM SSO登录参数有误，SyKey错误")
                Logger.error(irm_sykey)
                UserInfoUtil().saveUserLog(request, "IRM单点登录SyKey错误", state='0', content='用户' + parameter + '单点登录失败！')
                #rs.errorData(errorMsg="单点登录SyKey错误，请联系IRM管理员确认登录信息！")       
                #return JsonResponse(rs.toDict())
                return HttpResponseRedirect("/")
            if logintime =='' or logintime <begintime or logintime > endtime:
                Logger.error("IRM SSO登录参数有误，登录时间错误")
                Logger.error(logintime)
                UserInfoUtil().saveUserLog(request, "IRM单点登录时间错误", state='0', content='用户' + parameter + '单点登录失败！')
                #rs.errorData(errorMsg="单点登录时间错误，请联系IRM管理员确认登录信息！")       
                #return JsonResponse(rs.toDict())
                return HttpResponseRedirect("/")

        except Exception as err:            
            Logger.errLineNo()            
            Logger.error("IRM SSO登录参数获取失败:%s" % err)
            Logger.error("IRM SSO登录加密源参数:%s" % s_parameter)
            Logger.error("IRM SSO登录解密后源参数:%s" % parameter)
            return HttpResponseRedirect("/")
        try:
            Logger.info("IRM SSO登录用户ID信息:")        
            Logger.info([UserName, UserId])
            UserLogined = user_utils.ssouserlogin(UserName,UserId)    
            Logger.info("IRM SSO登录用户登录结果信息:")
            Logger.info(UserLogined)
            if len(UserLogined) == 0:
                if request.session.get('userlogin', None) == 1:
                    request.session['userlogin'] = 0
                Logger.error("IRM SSO登录用户名或者ID有错误。")
                Logger.error(UserLogined)
                
                UserInfoUtil().saveUserLog(request, "IRM单点登录用户名或者ID错误", state='0', content='用户' + UserName + '单点登录失败！')

                #rs.errorData(errorMsg="单点登录用户名错误，请联系OA管理员确认登录用户名！")
                # return HttpResponse(['-100', '|', '登录失败，请重新输入用户名和密码！'])
                #return JsonResponse(rs.toDict())
                return HttpResponseRedirect("/")
        
            else:
                if (UserLogined[0][5] != 1):
                    if request.session['userlogin'] == 1:
                        request.session['userlogin'] = 0

                    Logger.error("IRM SSO登录用户用户已锁定。")
                    Logger.error(UserName)
                

                    UserInfoUtil().saveUserLog(request, "用户已锁定", state='0', content='用户' + UserName + '单点登录失败，用户已锁定，请联系管理员！')
                    # return HttpResponse(['-101', '|', '登录失败，用户已锁定，请联系管理员！'])
                    #rs.errorData(errorMsg='登录失败，用户已锁定，请联系管理员！')
                    #return JsonResponse(rs.toDict())
                    return HttpResponseRedirect("/")
                else:
                    Logger.info("IRM SSO登录用户会话设置开始:")
                    request.session['userlogin'] = 1
                    request.session['user_id'] = UserLogined[0][0]
                    request.session['user_roleid'] = UserLogined[0][3]
                    request.session['userrealname'] = UserLogined[0][6]
                    request.session['usernickname'] = UserLogined[0][7]
                    Logger.info("IRM SSO登录用户会话设置成功！")

                    UserInfoUtil().saveUserLog(request, "IRM单点登录成功", state='1', content='IRM单点登录成功！')
                    Logger.info("IRM SSO登录用户跳转页面：")
                    Logger.info(redirect)
                    if redirect =='':
                        redirectUrl = user_utils.getUserLoginRedirectUrl(UserLogined[0][0])
                    else:
                        redirectUrl = redirect
                    #rs.redirectUrl = redirectUrl                
                    if redirectUrl is None:
                        redirectUrl ='/'
                        #rs.redirectUrl = "/"
                        #rs.errorData(errorMsg="用户暂未分配权限，请联系管理员进行处理。")
                        return ''

                    # print(['100', '|', '登录成功！','|',redirectUrl])
                    return HttpResponseRedirect('/'+redirectUrl)
        except Exception as err:
            Logger.errLineNo()            
            Logger.error("IRM SSO登录会话建立失败:%s" %  err)                    

if __name__ == '__main__':
    # data = getUserInfoByUserId('1')
    # print(data)
    # createUserPwdUrl('111', '127.0.0.1')
    # 密码加解密操作
    source= "username=admin"
    key="SIRM2012"
    newPassWord = passWordEncode(source,key)
    print(newPassWord)
    # data = resetUserPwd('051', newPassWord)
    # print(data.toDict())
    # 解密
    # password='89J+mipWq4VviXPkgDgTyw=='
    "5A139717A99674BF78E4B50119866E1B"
    "5A139717A99674BF78E4B50119866E1B"
    
    password = newPassWord
    print(passWordDecode(password,key))
