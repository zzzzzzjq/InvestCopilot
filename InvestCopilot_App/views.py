import random
from django.conf import settings
from django.shortcuts import render
from django.http import HttpResponse
from django.http import StreamingHttpResponse
# from django.core.urlresolvers import reverse
from django.urls import reverse
from django.http import HttpResponseRedirect
from django.http import HttpResponseForbidden
from wsgiref.util import FileWrapper
from django.http import JsonResponse
from django.shortcuts import  render, get_object_or_404, redirect
import uuid
from django.utils.safestring import mark_safe
import jwt
import json
import datetime
import logging
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.conf import settings as settings


os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'

#缓存数据库
import InvestCopilot_App.models.cache.cacheDB as cache_db

# 用户登录检查
from pyDes import des, CBC, PAD_PKCS5
import base64
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
from InvestCopilot_App.models.toolsutils import MenuUtils as menu_utils
from InvestCopilot_App.models.user.UserInfoUtil import UserInfoUtil
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.header import Header

#用户自选股组合
import InvestCopilot_App.models.UserPortfolio.userportfolio as user_portfolio

Logger = logger_utils.LoggerUtils()





# Create your views here.

def page_not_found(request,exception):
    # 自定义404页面    
    return render(request, 'selfdeferrorpage/404error.html')

def page_error(request):
    # 自定义500页面    
    return render(request, 'selfdeferrorpage/500error.html')

def getTableLanguage(request):
    """
    dataTable表格控件翻译
    """
    language = {
        "sProcessing": "处理中...",
        "sLengthMenu": "显示 _MENU_ 项结果",
        "sZeroRecords": "没有匹配结果",
        "sInfo": "显示第 _START_ 至 _END_ 项结果，共 _TOTAL_ 项",
        "sInfoEmpty": "显示第 0 至 0 项结果，共 0 项",
        "sInfoFiltered": "(由 _MAX_ 项结果过滤)",
        "sInfoPostFix": "",
        "sSearch": "",
        "searchPlaceholder": "搜索",
        "sUrl": "",
        "sEmptyTable": "表中数据为空",
        "sLoadingRecords": "载入中...",
        "sInfoThousands": ",",
        "oPaginate": {
            "sFirst": "首页",
            "sPrevious": "上页",
            "sNext": "下页",
            "sLast": "末页"
        },
        "oAria": {
            "sSortAscending": ": 以升序排列此列",
            "sSortDescending": ": 以降序排列此列"
        }
    }
    return JsonResponse(language)



def home(request):
    # 登录页面
    # userId = request.session.get('user_id',None)
    userId = None
    if userId is None:
        return render(request, 'login.html')
    else:
        if len(userId) == 0:
            return render(request, 'login.html')
        else:
            return HttpResponseRedirect('/')


def logout(request):
    # 用户退出
    user_utils.userlogout(request)

    rs = ResultData()
    
    #cookieUserEmail = request.COOKIES.get('user_email', None)
    #cookieUserPwd = request.COOKIES.get('user_password', None)
    #resp = HttpResponseRedirect('/')
    #if cookieUserEmail is not None:
    #    resp.set_cookie('user_email', '')
    #if cookieUserPwd is not None:
    #    resp.set_cookie('user_password', '')
    
    rs.redirectUrl = "/"

            # print(['100', '|', '登录成功！','|',redirectUrl])            
    return JsonResponse(rs.toDict())



def login(request):
    rs = ResultData()    
    emailaddr = request.POST.get('username', '')
    password = request.POST.get('userpassword', '')
    if emailaddr=='' :        
        loginRequest = json.loads(request.body)
        emailaddr = loginRequest['username']
        password = loginRequest['userpassword']
    
    syskey = settings.SYSCRYPTKEY
    Logger.debug(syskey)
    k = des(syskey, CBC, "\0\0\0\0\0\0\0\0", padmode=PAD_PKCS5)
    password = base64.b64encode(k.encrypt(password)).decode('ascii')
    UserName = emailaddr

    UserLogined = user_utils.userlogin(UserName, password)

    if len(UserLogined) == 0:
        if request.session.get('userlogin', None) == 1:
            request.session['userlogin'] = 0

        UserInfoUtil().saveUserLog(request, "户名或密码错误", state='0', content='用户' + UserName + '登录失败，请重新输入用户名和密码！')
        rs.errorData(errorMsg="户名或密码错误，请重新输入用户名和密码！")
        # return HttpResponse(['-100', '|', '登录失败，请重新输入用户名和密码！'])
        return JsonResponse(rs.toDict())
    else:
        if (UserLogined[0][5] != 1):
            if request.session['userlogin'] == 1:
                request.session['userlogin'] = 0
            UserInfoUtil().saveUserLog(request, "用户已锁定", state='0', content='用户' + UserName + '登录失败，用户已锁定，请联系管理员！')
            # return HttpResponse(['-101', '|', '登录失败，用户已锁定，请联系管理员！'])
            rs.errorData(errorMsg='登录失败，用户已锁定，请联系管理员！')
            return JsonResponse(rs.toDict())
        else:
            request.session['userlogin'] = 1
            request.session['user_id'] = UserLogined[0][0]
            request.session['user_roleid'] = UserLogined[0][3]
            request.session['userrealname'] = UserLogined[0][6]
            request.session['usernickname'] = UserLogined[0][7]
            UserInfoUtil().saveUserLog(request, "登录成功", state='1', content='登录成功！')
            redirectUrl = user_utils.getUserLoginRedirectUrl(UserLogined[0][0])
            rtdata={"redirectUrl":redirectUrl}
            getUserAccess = user_utils.getUserAccess(request)
            rtdata["UserAccess"]=getUserAccess
            rtdata["UserNickName"] =  UserLogined[0][7]
            rtsessionid = tools_utils.md5(str(random.randint(10,20))) #request.session.session_key
            rtdata["sessionid"] =  rtsessionid
            rs.data=rtdata
            if redirectUrl is None:
                rs.redirectUrl = "/"
                rs.errorData(errorMsg="用户暂未分配权限，请联系管理员进行处理。")
            # print(['100', '|', '登录成功！','|',redirectUrl])            
            return JsonResponse(rs.toDict())


@userLoginCheckDeco
def UserChangePassword(request):
    usernickname = request.session.get('usernickname')
    return render(request, 'Changepassword.html', {'usernickname': usernickname})


# User Change Pasword
@userLoginCheckDeco
def PasswordChange(request):
    if request.method == 'POST':
        resultData = ResultData()
        userid = request.session.get('user_id')
        oldpasswd = request.POST.get('oldpasswd', '')
        newpasswd1 = request.POST.get('newpasswd1', '')
        newpasswd2 = request.POST.get('newpasswd2', '')    
        if oldpasswd=='' :        
            jsonRequest = json.loads(request.body)
            oldpasswd = jsonRequest['oldpasswd']
            newpasswd1 = jsonRequest['newpasswd1']
            newpasswd2 = jsonRequest['newpasswd2']
        
        if newpasswd1 != newpasswd2:
            resultData.errorData(errorMsg='抱歉，修改密码失败，请稍后再试。')
            return JsonResponse(resultData.toDict())

        resultData = user_utils.changepwd(userid, oldpasswd, newpasswd1)
        return JsonResponse(resultData.toDict())


def PasswordChangeByEmail(request):
    # if request.method == 'POST':
    rs = ResultData()
    rtdata ={ 'msg' : ""}
    rest = tools_utils.requestDataFmt(request, fefault=None)
    if not rest.errorFlag:
        return JsonResponse(rest.toDict())
    reqData = rest.data
    # user_id = request.session.get("user_id")
    token = reqData.get('token', '')
    if(len(token))==0:
        rtdata['msg']="请输入token参数"
        rs.data = rtdata
        rs.errorData(errorMsg="请输入token参数")
        return JsonResponse(rs.toDict())
    try:
        # 解密令牌
        decoded_token = jwt.decode(token, 'ForgetPwdFindByEmail!', algorithms=['HS256'])
        # print(decoded_token)
    except jwt.InvalidSignatureError:
        rtdata['msg'] = "无效令牌,请重新输入用户名或者检查链接地址是否为发送的地址"
        rs.data = rtdata
        rs.errorData(errorMsg="无效令牌,请重新输入用户名或者检查链接地址是否为发送的地址")
        return JsonResponse(rs.toDict())

    if 'exp' in decoded_token:
        expiration_timestamp = int(decoded_token['exp'])
        now_time = int(datetime.datetime.now().timestamp())
        if(now_time>expiration_timestamp):
            rtdata['msg'] = "令牌已过期,请在10分钟内验证邮箱"
            rs.data = rtdata
            rs.errorData(errorMsg="令牌已过期,请在10分钟内验证邮箱")
            return JsonResponse(rs.toDict())
    user_id = decoded_token['user_id']
    # 检查数据库中是否有该token,没有的话报错，有的话修改state为0
    if_exists = user_utils.seachTokenForChangePwd(user_id,token)
    if(if_exists==0):
        rtdata['msg'] = "无效令牌,请重新发送邮件"
        rs.data = rtdata
        rs.errorData(errorMsg="无效令牌,请重新发送邮件")
        return JsonResponse(rs.toDict())
    elif(if_exists==1):
        rtdata['msg'] = "令牌失效,近10分钟已经更改过一次密码"
        rs.data = rtdata
        rs.errorData(errorMsg="令牌失效,近10分钟已经更改过一次密码")
        return JsonResponse(rs.toDict())
    else:
        # 更改密码
        newpasswd1 = reqData.get('newpasswd1','')
        newpasswd2 = reqData.get('newpasswd2', '')
        if newpasswd1=="" or newpasswd1 is None:
            rtdata['msg'] = "请输入新密码."
            rs.data = rtdata
            rs.errorData(errorMsg="请输入新密码.")
            return JsonResponse(rs.toDict())
        elif len(newpasswd1)<8:
            rtdata['msg'] = "请至少输入8位密码."
            rs.data = rtdata
            rs.errorData(errorMsg="请至少输入8位密码")
            return JsonResponse(rs.toDict())
        if newpasswd2=="" or newpasswd2 is None:
            rtdata['msg'] = "请再次输入新密码."
            rs.data = rtdata
            rs.errorData(errorMsg="请再次输入新密码.")
            return JsonResponse(rs.toDict())
        if newpasswd1 != newpasswd2:
            rtdata['msg'] = "抱歉，请保证两次新密码输入一致，修改密码失败。"
            rs.data = rtdata
            rs.errorData(errorMsg='抱歉，请保证两次新密码输入一致，修改密码失败。')
            return JsonResponse(rs.toDict())
        rs = user_utils.restPwd(userId=user_id, newpasswd=newpasswd1)
        rtdata['msg'] = "密码重置成功."
        rs.data = rtdata
        return JsonResponse(rs.toDict())




def PasswordForgetByEmail(request):
    """
    新浪邮箱：smtp.sina.com,
    搜狐邮箱：http://smtp.sohu.com，
    126邮箱：smtp.126.com,
    139邮箱：smtp.139.com,
    163网易邮箱：http://smtp.163.com。
    """
    if request.method == 'POST':
        rs = ResultData()
        rtdata = {'msg': ""}
        # 0. 获取用户名
        emailaddr = request.POST.get('username', '')
        # print("emailaddr",emailaddr)
        if emailaddr == '':
            loginRequest = json.loads(request.body)
            emailaddr = loginRequest['username']
        UserName = emailaddr
        # 1. 检查要发送邮箱的合法性（格式），是否为数据库内的用户邮箱
        UserLogined = user_utils.userCheckForEmail(UserName)
        if len(UserLogined) == 0:
            # 1.1 该用户不存在
            if request.session.get('userlogin', None) == 1:
                request.session['userlogin'] = 0
            UserInfoUtil().saveUserLog(request, "该用户不存在", state='0', content='用户' + UserName + '查找失败，请重新输入用户名（邮箱地址）！')
            rtdata['msg'] = "该用户不存在，查找失败，请重新输入用户名（邮箱地址）！"
            rs.data = rtdata
            rs.errorData(errorMsg="该用户不存在，查找失败，请重新输入用户名（邮箱地址）！")
            return JsonResponse(rs.toDict())
        elif (UserLogined[0][5] != 1):
            # 1.2 账户存在，但是用户锁定
                if request.session['userlogin'] == 1:
                    request.session['userlogin'] = 0
                UserInfoUtil().saveUserLog(request, "用户已锁定", state='0', content='用户' + UserName + '查找成功，但用户已锁定，请联系管理员！')
                rtdata['msg'] = "查找成功，账户存在，但用户已锁定，请联系管理员！"
                rs.data = rtdata
                rs.errorData(errorMsg='查找成功，账户存在，但用户已锁定，请联系管理员！')
                return JsonResponse(rs.toDict())
        # redirectUrl = "修改密码的链接"
        request.session['user_id'] = UserLogined[0][0]
        request.session['user_roleid'] = UserLogined[0][3]
        request.session['userrealname'] = UserLogined[0][6]
        request.session['usernickname'] = UserLogined[0][7]
        # print("Session:", request.session.items())
        # 令牌过期时间：10分钟
        expiration = datetime.datetime.now() + datetime.timedelta(minutes=10)   # <class 'datetime.datetime'>

        token = jwt.encode({'user_id': UserLogined[0][0],'exp':expiration}, 'ForgetPwdFindByEmail!', algorithm='HS256')
        remoteip_get=request.META.get('REMOTE_ADDR')
        # 把token存入数据库
        user_utils.intserEmailTokenForChangePwd(UserLogined[0][0], remoteip_get, token)
        mail_host =settings.EMAIL_HOST
        send_addr =settings.EMAIL_SEND_ADDR
        send_pwd =settings.EMAIL_SEND_PWD
        to_addr = UserName
        text = f"""
        <p>请点击以下链接重置密码：</p>
        <p><a href="https://www.intellistock.cn/#/user/resetPassword?token={token}">重设密码链接</a></p>
        """
        # https://www.intellistock.cn/#/user/resetPassword
        message = MIMEMultipart()
        html = MIMEText(text, 'html', 'utf-8')
        message['From']=send_addr
        message['To']=to_addr
        subject = "Intelli Stock:用户重置密码"  # 主题
        message['Subject'] = Header(subject, 'utf-8')
        message.attach(html)
        # print(message.as_string())
        try:
            # 1. 连接邮箱服务器
            # qq邮箱，ssl为465，普通为587
            # smtp = smtplib.SMTP()
            # smtp.connect(mail_host, 587)
            smtp = smtplib.SMTP_SSL(mail_host,465)
            smtp.login(send_addr, send_pwd)
            smtp.sendmail(send_addr, to_addr, message.as_string())
            # smtp.send_message(message)
            rs.errorMsg="邮件发送成功"
            rtdata['msg'] = "邮件发送成功"
            rs.data = rtdata
            rs.sessionid = request.session.session_key
            smtp.quit()
        except Exception as e:
            rtdata['msg'] = "Error: 无法发送邮件"
            rs.data = rtdata
            rs.errorMsg ="Error: 无法发送邮件"+str(e)
        finally:
            return JsonResponse(rs.toDict())



@userLoginCheckDeco
def welcome(request):
    usernickname = request.session.get('usernickname')

    # 返回字符串需要格式为html mark_safe()    
    return render(request, 'welcome.html', {'usernickname': usernickname})


@userLoginCheckDeco
def userportfolio(request):
    usernickname = request.session.get('usernickname')
    all_stockinfo = cache_db.getStockInfoDF()
    all_stockinfo = all_stockinfo.fillna('')
    all_stockinfo = all_stockinfo.to_dict(orient='records')
    get_userportfoliolist = user_portfolio.get_userportfolio_list(request.session.get('user_id'))
    print("get_userportfoliolist:!!!!   ",get_userportfoliolist)
    userportfolioDF = get_userportfoliolist[0]
    userportfoliolist =  userportfolioDF.to_dict(orient='records')

    return render(request, 'userportfolio/userportfolio.html', {'usernickname': usernickname, 'all_stockinfo': all_stockinfo ,'userportfoliolist':userportfoliolist})

@userLoginCheckDeco
def interresearchreport(request):
    usernickname = request.session.get('usernickname')
    return render(request, 'demo/interresearchreport.html', {'usernickname': usernickname})


@userLoginCheckDeco
def manageopenaitask(request):
    usernickname = request.session.get('usernickname')
    return render(request, 'manager/manageopenaitask.html', {'usernickname': usernickname})


