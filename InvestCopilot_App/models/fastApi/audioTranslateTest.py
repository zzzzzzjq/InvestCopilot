import traceback
import requests
import time
alyurl="http://47.106.236.106:1182"
# alyurl="http://127.0.0.1:1182"

def getTranslateAudio(errorNum=0):
    try:
        if errorNum>=3:
            return []
        rtids = []
        resp = requests.post(alyurl+"/audioTranslate",data={'doMethod': "getTranslateAudio"})
        respd = resp.json()
        print("getTranslateAudio respd:",respd)
        errorFlag = respd['errorFlag']
        if errorFlag:
            rtids = respd['data']
    except:
        print(traceback.format_exc())
        errorNum+=1
        time.sleep(10)
        return getTranslateAudio(errorNum=errorNum)
    return rtids

def fillAudioLanguage(audioId,language='en',errorNum=0):
    try:
        if errorNum>=3:
            return []
        rtids = []
        resp = requests.post(alyurl+"/audioTranslate",data={'doMethod': "fillAudioLanguage",'id': audioId,'language': "zh"})
        respd = resp.json()
        print("fillAudioLanguage respd:",respd)
        errorFlag = respd['errorFlag']
        if errorFlag:
            rtids = respd['data']
    except:
        print(traceback.format_exc())
        errorNum+=1
        time.sleep(10)
        return fillAudioLanguage(audioId,language,errorNum=errorNum)
    return rtids

def getSourceData(audioId,errorNum=0):
    try:
        if errorNum>=3:
            return []
        rtids = []
        resp = requests.post(alyurl+"/getSourceData",data={'doMethod': "getAudioSourceData",'id': audioId})
        respd = resp.json()
        print("getSourceData respd:",respd)
        errorFlag = respd['errorFlag']
        if errorFlag:
            rtids = respd['data']
    except:
        print(traceback.format_exc())
        errorNum+=1
        time.sleep(10)
        return getSourceData(audioId,errorNum=errorNum)
    return rtids

def fillAudioTranslateText(audioId,translateText,language,errorNum=0):
    try:
        if errorNum>=3:
            return []
        rtids = []
        resp = requests.post(alyurl+"/audioTranslate",data={'doMethod': "fillAudioTranslateText",
                                                            'id': audioId,
                                                            "translateText":translateText,
                                                            "language":language
                                                            })
        respd = resp.json()
        print("fillAudioTranslateText respd:",respd)
        errorFlag = respd['errorFlag']
        if errorFlag:
            rtids = respd['data']
    except:
        print(traceback.format_exc())
        errorNum+=1
        time.sleep(10)
        return fillAudioTranslateText(audioId,translateText,language,errorNum=errorNum)
    return rtids

if __name__ == '__main__':
    rtd=getTranslateAudio()#1、获取需要翻译的音频文件
    print("getTranslateAudio rtd:%s"%rtd)
    rtd=getSourceData("Audio_60e9ceac3ccc72a2a32b7b5cfe378afe")#2、获取需要翻译的音频数据 找到里面的音频路径
    print("getSourceData rtd:%s"%rtd)
    if rtd["count"]>0:
        audio_path=rtd['audio_path']
        print("audio_pathxx:",audio_path)#3、下载音频文件
    # rtd=fillAudioLanguage("Audio_5e9486d3e9985032530c50bee4f2ebc3","zh")#4、如何whisper解析为中文 就直接调用此接口； 不需要执行第5步
    # rtd=fillAudioTranslateText("Audio_5e9486d3e9985032530c50bee4f2ebc3","translateText 翻译内容",'en')#5、翻译后上传内容需要将解析后的语音上传
    # print("fillAudioTranslateText rtd:%s"%rtd)
