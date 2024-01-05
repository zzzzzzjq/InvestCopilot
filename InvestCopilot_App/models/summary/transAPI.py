#翻译处理
import datetime
import re
import os
import socket
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.summary.transMode import transMode
from InvestCopilot_App.models.manager import ManagerUserMode as manager_user_mode
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
import InvestCopilot_App.models.toolsutils.sendmsg as wx_send
from InvestCopilot_App.models.cache import cacheDB as cache_db
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from InvestCopilot_App.models.comm.fileUtils import fileUtils
from django.http import JsonResponse
from django.http import HttpResponse
from django.http import StreamingHttpResponse
from urllib.parse import quote

Logger = logger_utils.LoggerUtils()

MEETINGTYPES_zh=[
    {"meetingTypeId":"1","meetingTypeName":"管理层交流会议"},
    {"meetingTypeId":"2","meetingTypeName":"分析师交流会议"},
    {"meetingTypeId":"3","meetingTypeName":"多轮对话"},]

MEETINGTYPES_en=[
    {"meetingTypeId":"1","meetingTypeName":"Management Communication Meeting"},
    {"meetingTypeId":"2","meetingTypeName":"Analyst Exchange Meeting"},
    {"meetingTypeId":"3","meetingTypeName":"Multiple rounds of conversation"},]

@userLoginCheckDeco
def transAPIHandler(request):
    rest = ResultData()
    try:
        user_id = request.session.get("user_id")
        rest = tools_utils.requestDataFmt(request, fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        # 获取用户的配置参数
        user_privilegeset = None
        companyId = None
        userCfg_dt = cache_db.getUserConfig_dt()
        if user_id in userCfg_dt:
            userCfg = userCfg_dt[user_id]
            user_privilegeset = userCfg['PRIVILEGESET']
            companyId = str(userCfg['COMPANYID'])
        reqData = rest.data
        doMethod = reqData.get("doMethod")
        Logger.debug("transAPIHandler request:%s"%reqData)
        if doMethod == "getTransSummary":
            page = reqData.get("page")
            pageSize = reqData.get("pageSize")
            searchTitle = reqData.get("searchTitle")#标题搜索
            myTrans = reqData.get("myTrans")
            fileTypes = reqData.get("fileTypes")
            # beginTime = reqData.get("beginTime")
            # endTime = reqData.get("endTime")
            # if beginTime is None:
            #     beginTime = nt + datetime.timedelta(days=-5)
            #     beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            # if endTime is None:
            #     endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            translation = reqData.get("translation")
            if translation is None:
                translation = "zh"
            quserId=None
            if myTrans is not None:
                quserId = user_id
            if companyId is None or companyId == "":
                companyIds = []
            else:
                companyIds = [companyId]
            if fileTypes is None:
                fileTypes = []
            else:
                fileTypes = re.split("[|,]", str(fileTypes))
            if user_privilegeset == "super":
                companyInfoPdData = manager_user_mode.getAllCompanyInfo()
                companyInfoPdData = tools_utils.dfColumUpper(companyInfoPdData)
                sysCompanyIds = companyInfoPdData['COMPANYID'].values.tolist()
                for fcid in sysCompanyIds:
                    companyIds.append(fcid)
                companyIds = list(set(companyIds))
            if page is None:
                page = 1
            else:
                page = int(page)

            if pageSize is None:
                pageSize = 20
            else:
                pageSize = int(pageSize)
            queryTitle=None
            if searchTitle is not None:
                searchTitle = str(searchTitle).strip()
                if len(searchTitle)>0:
                    queryTitle=searchTitle
            vtags=['Trans']
            rest = transMode().getTransSummaryView(vtags=vtags,translation=translation, userId=quserId,
                                                         companyIds=companyIds,fileTypes=fileTypes,queryTitle=queryTitle,
                                                         page=page,pageSize=pageSize)
            return JsonResponse(rest.toDict())

        elif doMethod == "uploadTransFile":  # 上传解析文件
            file_obj = request.FILES.get('file', None)
            transTitle = reqData.get("transTitle")
            if file_obj is None or file_obj is '':
                file_obj = reqData.get('file', None)
                if file_obj is None or file_obj is '':
                    rest.errorData(errorMsg='请选择上传文件。')
                    return JsonResponse(rest.toDict())
            companyId = None
            shortCompanyName = None
            userName = None
            userCfg_dt = cache_db.getUserConfig_dt()
            if user_id in userCfg_dt:
                userCfg = userCfg_dt[user_id]
                companyId = str(userCfg['COMPANYID'])
                userName = str(userCfg['USERREALNAME'])
                shortCompanyName = str(userCfg['SHORTCOMPANYNAME'])
            if companyId is None or companyId is '' or companyId in ['temp_account']:
                rest.errorData(errorMsg='没有文件上传权限，请联系系统管理员处理，并告知登录用户名和所属公司。')
                return JsonResponse(rest.toDict())
            # 支持翻译文件类型
            file_name = file_obj.name
            transName, fileend = os.path.splitext(file_name)
            if not str(fileend).lower() in [".pdf", ".doc", ".docx", ".txt"]:
                rest.errorData(errorMsg='翻译文件必须为 .pdf,.doc,.docx,.txt 格式。')
                return JsonResponse(rest.toDict())
            ckResult = tools_utils.charLength(transName, maxLength=100, checkObj="文件名字")
            if not ckResult.errorFlag:
                return JsonResponse(ckResult.toDict())

            if transTitle is not None:
                transName=str(transTitle).strip()

            trans_mode =transMode()
            ntime = datetime.datetime.now()
            cdate = ntime.strftime("%Y-%m-%d")
            ctime = ntime.strftime("%H:%M:%S")
            publishOn=cdate+" "+ctime
            # 上传会议次数检查
            stockchatnum_dt=cache_dict.getDictByKeyNo('3')#会议上传次数
            dayUploadnum=3
            if companyId in stockchatnum_dt:
                dayUploadnum=int(stockchatnum_dt[companyId])
            # 公司当天上传会议不能超过10个
            # select count(1) from erp.callmanger c  where  companyid in(select companyid from companyuser where cuserid='1983') and cdate ='2023-10-19' ;
            upcount = trans_mode.getCountUploadTransNum(user_id, cdate)
            if upcount >= dayUploadnum:
                errormsg = f"因计算资源限额问题，限制每家公司最多上传{dayUploadnum}篇报告，感谢您的理解。如需协助，请联系客服。"
                rest.errorData(errorMsg=errormsg)
                return JsonResponse(rest.toDict())
            # 检查会议是否已经上传 支持重复上传
            # sval = [str(file_name), companyId]
            # audioId = 'trans_' + tools_utils.md5("".join(sval))
            # mtDF = trans_mode.getCallDataById(audioId)
            # if not mtDF.empty:
            #     errormsg = "[%s] 会议已经存在，请重新处理" % audioName
            #     rest.errorData(errorMsg=errormsg)
            #     return JsonResponse(rest.toDict())
            # 注意正式上传时要修改文件绝对路径
            file_abs_dir = os.path.dirname(os.path.abspath('.'))
            # 更新数据
            csource = "trans"
            meetingType='file'
            language='en'
            rest = trans_mode.addCallFileData(transName, cdate, ctime, csource, user_id, companyId, meetingType,language,
                                          cregiest=None,
                                          creplay=None, csound=None, cremark=None,
                                          calllink=None, callname=None, callpwd=None)
            if not rest.errorFlag:
                Logger.error(rest.toDict())
                return JsonResponse(rest.toDict())
            audioId=rest.cid
            rtdata = {"transId":audioId}
            #保存文件
            file_full_path = r'D:\work\share\web\upload\trans\%s' % (user_id)
            if not os.path.exists(file_full_path):
                os.makedirs(file_full_path)
            vfile_name = "%s%s" % (audioId, fileend)
            file_full_path = os.path.join(file_full_path, vfile_name)
            with open(file_full_path, 'wb') as dest:
                dest.write(file_obj.read())

            #读取文件
            rest = fileUtils().parse_file(file_full_path)
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            filePareTxt=rest.text
            if len(filePareTxt)<100:
                rest.errorData(errorMsg="解析文件内容太短，请重新选择")
                return JsonResponse(rest.toDict())
            # /opt/service/backup 上传的音频文件保存路径
            #https://www.intellistock.cn/fileupload/audio/051/cicc.mp3
            # viewAlyAudioFilePath = "https://www.intellistock.cn/fileupload/audio/%s/%s%s" % (user_id,audioId, fileend)
            #修改保存路径
            rest = trans_mode.modifyCallAudioData(user_id, audioId, file_name, file_full_path)
            if not rest.errorFlag:
                Logger.error(rest.toDict())
                if os.path.exists(file_full_path):
                    os.remove(file_full_path)
                    Logger.info(f'文件 {file_full_path} 已删除')
                # db数据
                trans_mode.delCallData(user_id, audioId)
                return JsonResponse(rest.toDict())
            rest.data=rtdata
            #将数据插入到mongodb库
            windCode = "Trans"
            insertTime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            audioData = {
                "id": audioId,
                "title": transName,
                "windCode": windCode,
                "symbols": windCode,
                "tickers": [
                    companyId
                ],
                "source_url": file_full_path,#nginx
                "local_path": file_full_path,
                "forward": "1",
                "source": "Trans",
                "dataType": str(fileend).replace(".",""),
                "meetingType": meetingType,
                "uploadLanguage": language,
                "summaryText": filePareTxt,
                "publishOn": publishOn,
                "insertTime": insertTime,
                "summary": "",
                "cuserId":user_id
            }
            trans_mode.addCallSummaryData(audioId,audioData)
            # 音频文件上传成功
            vmsg = "接收到[%s][%s]客户翻译文件[%s][%s]" % (
            shortCompanyName, userName, file_name, datetime.datetime.now().strftime("%m-%d %H:%M:%S"))
            Logger.info(vmsg + "[%s]" % tools_utils.globa_vtouser)
            issend = wx_send.send_wx_msg_operation(vmsg)
            if not issend:
                Logger.info(vmsg + "[%s]" % tools_utils.globa_vtouser)
                wx_send.send_wx_msg_operation(vmsg)
            return JsonResponse(rest.toDict())
        elif doMethod == "delTransData":  # 文件会议
            cid = reqData.get('cid', '')
            if cid is None or cid == "":
                rest.errorData(errorMsg='请选择会议主题！')
                return JsonResponse(rest.toDict())
            #删除服务器文件
            try:
                # 删除文件
                trans_mode =transMode()
                fDF=trans_mode.getCallDataById(cid,user_id)
                if fDF.empty:
                    rest.errorData(errorMsg='会议文件不存在！')
                    return JsonResponse(rest.toDict())
                file_path=str(fDF.iloc[0].AUDIOPATH)
                # fp,fn=os.path.split(audiopath)
                # file_path=os.path.join("/opt/service/backup/upload/audio",fn)
                Logger.info(f'文件 {file_path}')
                if os.path.exists(file_path):
                    os.remove(file_path)
                    Logger.info(f'文件 {file_path} 已删除')
                # db数据
                rest = trans_mode.delCallData(user_id, cid)
                if not rest.errorFlag:
                    return JsonResponse(rest.toDict())
                dbRowcount=rest.data['rowcount']
                # 删除摘要
                rest = trans_mode.delCallSummaryData(user_id, cid)
                if not rest.errorFlag:
                    return JsonResponse(rest.toDict())
                mgRowcount=rest.data['rowcount']
                rest.data={'rowcount':dbRowcount+mgRowcount}
            except:
                Logger.errLineNo(msg='会议文件[%s]删除失败，请稍后重试！'%cid)
                rest.errorData(errorMsg='会议文件删除失败，请稍后重试！')
                return JsonResponse(rest.toDict())
        else:
            errorMsg = '抱歉，请求接口不存在，请重新选择。'
            rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())

    except Exception as ex:
        errorMsg = '抱歉，获取数据失败，请稍后重试。'
        Logger.errLineNo(msg=errorMsg)
        rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())
