__author__ = 'Robby'

from django import forms
from captcha.fields import CaptchaField
from captcha.models import CaptchaStore
from captcha.helpers import captcha_image_url

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as Logger_utils

Logger = Logger_utils.LoggerUtils()


class CaptchaLoginForm(forms.Form):
    captcha = CaptchaField()


def flushVerifyCode():
    """
    生成验证码
    imgage_url:验证码图片
    hashkey：验证码key
    """
    try:
        # 初始化返回对象
        resultData = ResultData()  # 菜单设置

        hashkey = CaptchaStore.generate_key()
        imgage_url = captcha_image_url(hashkey)
        print('imgage_url:', imgage_url)
        resultData.hashkey = hashkey
        resultData.imgage_url = imgage_url
        return resultData

    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='生成验证码异常')
        return resultData


def checkVerifyCode(verfyCode, hashkey):
    """
    校验验证码
    verfyCode:用户输入验证码
    hashkey：验证码key
    """
    try:
        # 初始化返回对象
        resultData = ResultData()
        if verfyCode == '':
            resultData.errorData(errorMsg='请输入验证码')
            return resultData

        cs = CaptchaStore.objects.filter(hashkey=hashkey)
        true_key = cs[0].response
        verfyCode = verfyCode.lower()
        if verfyCode != true_key:
            resultData.errorData(errorMsg='验证码错误',errorCode='CODE0001')

        return resultData
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='验证码错误',errorCode='CODE0001')
        return resultData

def checkVerifyCodeRequest(request):
    """
    校验验证码 从request对象中获取校验参数
    verfyCode:用户输入验证码
    hashkey：验证码key
    """
    try:
        # 初始化返回对象
        resultData = ResultData()
        verfyCode=request.POST.get('verfyCode','')
        hashkey=request.POST.get('hashkey','')

        if verfyCode == '':
            resultData.errorData(errorMsg='请输入验证码')
            return resultData

        cs = CaptchaStore.objects.filter(hashkey=hashkey)
        true_key = cs[0].response
        verfyCode = verfyCode.lower()
        if verfyCode != true_key:
            resultData.errorData(errorMsg='验证码错误',errorCode='CODE0001')

        return resultData
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='验证码错误',errorCode='CODE0001')
        return resultData


if __name__ == '__main__':
    pass
