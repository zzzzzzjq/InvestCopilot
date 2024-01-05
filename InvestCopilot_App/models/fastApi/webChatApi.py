#企业微信消息同
from typing import Union
import datetime
import sys
import os
sys.path.append('../../..')
from fastapi import FastAPI, Request
#pip install fastapi -i https://pypi.tuna.tsinghua.edu.cn/simple some-package
#pip install uvicorn -i https://pypi.tuna.tsinghua.edu.cn/simple some-package
#pip install python-multipart -i https://pypi.tuna.tsinghua.edu.cn/simple some-package
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.summary.audioMode import audioMode
from InvestCopilot_App.models.summary.viewSummaryMode import viewSummaryMode
app = FastAPI()

# @app.get("/")
# def read_root():
#     return {"Hello": "World"}

# @app.post("/wxwork/")
# async def wxwork(request: Request):
#     # wxwork回调接口
#     rest=ResultData()
#     form_data = await request.form()
#     doMethod = form_data.get("doMethod")
#     if doMethod=="getTranslateAudio":#获取需要翻译的电话会议
#         return getTranslateAudio(form_data)
#     elif doMethod=="fillAudioTranslateText":#回填音频翻译后的内容
#         return fillAudioTranslateText(form_data)
#     elif doMethod=="fillAudioLanguage":#whisper解析后非英文，修改语种再次处理
#         return fillAudioLanguage(form_data)
#     else:
#         rest.errorData(errorMsg="doMethod参数错误")
#         return rest.toDict()

from xml.dom.minidom import parseString

from wechatpy.exceptions import InvalidSignatureException
from InvestCopilot_App.models.fastApi.WXBizMsgCrypt import WXBizMsgCrypt
qy_api = [
    WXBizMsgCrypt("4hWxG3zA7", "OJr0wyYWN8LcT417kYlDYsa5J578VCzSIgboWYf6tt5", "rLUbOj7wJvtOCdLeVrk-xxg3qjTEmjC7nhE6Ph9YgUI"),
    # WXBizMsgCrypt("4hWxG3zA7", "OJr0wyYWN8LcT417kYlDYsa5J578VCzSIgboWYf6tt5", "1000002"),
] #对应接受消息回调模式中的token，EncodingAESKey 和 企业信息中的企业id


@app.get("/webchat")
async def webchat(msg_signature,timestamp,nonce,echostr):
    # wxwork回调接口
    print("msg_signature:",msg_signature)
    print("timestamp:",timestamp)
    print("nonce:",nonce)
    print("echostr:",echostr)

    # ret, sEchoStr = WXBizMsgCrypt("4hWxG3zA7", "OJr0wyYWN8LcT417kYlDYsa5J578VCzSIgboWYf6tt5", "rLUbOj7wJvtOCdLeVrk-xxg3qjTEmjC7nhE6Ph9YgUI")\
    ret, sEchoStr = WXBizMsgCrypt("4hWxG3zA7", "OJr0wyYWN8LcT417kYlDYsa5J578VCzSIgboWYf6tt5", "5629500190675989")\
        .VerifyURL(msg_signature, timestamp, nonce, echostr)
    print(" ret,  :", ret,  )
    print("  , sEchoStr:", sEchoStr)
    print("********************************************")
    ret, sEchoStr = WXBizMsgCrypt("4hWxG3zA7", "OJr0wyYWN8LcT417kYlDYsa5J578VCzSIgboWYf6tt5", "rLUbOj7wJvtOCdLeVrk-xxg3qjTEmjC7nhE6Ph9YgUI")\
        .VerifyURL(msg_signature, timestamp, nonce, echostr)
    print(" ret,  :", ret,  )
    print("  , sEchoStr:", sEchoStr)
    print("********************************************")
    ret, sEchoStr = WXBizMsgCrypt("4hWxG3zA7", "OJr0wyYWN8LcT417kYlDYsa5J578VCzSIgboWYf6tt5", "1000002")\
        .VerifyURL(msg_signature, timestamp, nonce, echostr)
    print(" ret,  :", ret,  )
    print("  , sEchoStr:", sEchoStr)
    if (ret != 0):
        print("ERR: VerifyURL ret: " + str(ret))
        return ("failed")
    else:
        return (sEchoStr)


#运行
#uvicorn summaryTaskAPI:app --reload

#C:\Users\Administrator\AppData\Local\Programs\Python\Python36\Scripts\uvicorn summaryTaskAPI:app --reload

from uvicorn import run
if __name__ == "__main__":
    run(app, host="0.0.0.0", port=1183)