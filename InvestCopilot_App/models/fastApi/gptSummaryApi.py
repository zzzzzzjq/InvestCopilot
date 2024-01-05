# from typing import Union
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

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/audioTranslate/")
async def audioTranslate(request: Request):
    # 获取表单数据
    rest=ResultData()
    form_data = await request.form()
    doMethod = form_data.get("doMethod")
    if doMethod=="getTranslateAudio":#获取需要翻译的电话会议
        return getTranslateAudio(form_data)
    elif doMethod=="fillAudioTranslateText":#回填音频翻译后的内容
        return fillAudioTranslateText(form_data)
    elif doMethod=="fillAudioLanguage":#whisper解析后非英文，修改语种再次处理
        return fillAudioLanguage(form_data)
    else:
        rest.errorData(errorMsg="doMethod参数错误")
        return rest.toDict()

@app.post("/getSourceData/")
async def getSourceData(request: Request):
    # 获取表单数据
    rest=ResultData()
    form_data = await request.form()
    doMethod = form_data.get("doMethod")
    if doMethod=="getAudioSourceData":#获取需要翻译的电话会议
        return getAudioSourceData(form_data)
    else:
        rest.errorData(errorMsg="doMethod参数错误")
        return rest.toDict()

def getAudioSourceData(form_data):
    rest=ResultData()
    audioId = form_data.get("id")
    qid = form_data.get("id")  # 文档编号
    if qid == "" or qid is None:
        rest.errorData("文档编号不能为空!")
        return rest.toDict()
    ids = str(audioId).split(",")
    rest = viewSummaryMode().getDocContent(ids)

    if not rest.errorFlag:
        return rest.toDict()
    rtdata = rest.data
    # 设定返回的 audio_path
    if rtdata['count'] > 0:
        qdata = rtdata['data']
    else:
        return rest.toDict()
    qdt = qdata[0]
    if "source_url" in qdt:
        _audio_path = qdt['source_url']
        _cuserId = ""
        if "cuserId" in qdt:
            _cuserId = qdt['cuserId']
        if "forward" in qdt:
            fpath, fname = os.path.split(_audio_path)
            # https://www.intellistock.cn/fileupload/audio/051/cicc.mp3
            qdt['audio_path'] = "https://www.intellistock.cn/fileupload/audio/%s/%s" % (_cuserId, fname)
        qdt['count']=1
        rest.data = qdt
    return rest.toDict()

def getTranslateAudio(form_data):
    # 获取需要翻译的电话会议
    nt = datetime.datetime.now()
    beginTime = form_data.get("beginTime")
    endTime = form_data.get("endTime")
    if beginTime is None:
        beginTime = nt + datetime.timedelta(days=-5)
        beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
    if endTime is None:
        endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
    tickers = form_data.get("tickers")
    if tickers is not None and tickers != "":
        tickers = str(tickers).split(",")
    else:
        tickers = []
    companyIds = form_data.get("companyIds")
    if companyIds is not None and companyIds != "":
        companyIds = str(companyIds).split(",")
    else:
        companyIds = []
    sumaryFlag = form_data.get("sumaryFlag")
    language = form_data.get("language")
    if sumaryFlag is None:
        sumaryFlag = "1"
    # if language is None:
    #     language = "en"
    rest=audioMode().getAudioIds(beginTime, endTime, companyIds=companyIds,tickers=tickers, sumaryFlag=sumaryFlag, language=language)
    return rest.toDict()

def fillAudioTranslateText(form_data):
    # 回填音频翻译后的内容
    rest=ResultData()
    audioId = form_data.get("id")
    language = form_data.get("language")#解析出的后的语言
    audioText = form_data.get("translateText")
    translateMode = form_data.get("translateMode")
    if audioId == "" or audioId is None:
        rest.errorData("编号不能为空!")
        return rest.toDict()
    if (audioText == "" or audioText is None):
        rest.errorData("音频翻译内容不能为空!")
        return rest.toDict()
    if len(str(audioText).strip()) == 0:
        rest.errorData("音频翻译内容不能为空!")
        return rest.toDict()
    if translateMode is None:
        translateMode="whisper"
    rest = audioMode().fillAudioText(audioId, language=language, audioText=audioText, translateMode=translateMode)
    return rest.toDict()

def fillAudioLanguage(form_data):
    # 回填音频解析后的语言
    rest=ResultData()
    audioId = form_data.get("id")
    language = form_data.get("language")
    if audioId == "" or audioId is None:
        rest.errorData("编号不能为空!")
        return rest.toDict()
    if (language == "" or language is None):
        rest.errorData("音频解析语言不能为空!")
    rest = audioMode().fillAudioLanguage(audioId, language=language)
    return rest.toDict()

#运行
#uvicorn summaryTaskAPI:app --reload

#C:\Users\Administrator\AppData\Local\Programs\Python\Python36\Scripts\uvicorn summaryTaskAPI:app --reload

from uvicorn import run
if __name__ == "__main__":
    run(app, host="0.0.0.0", port=1182)