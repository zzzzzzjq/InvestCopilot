# encoding: utf-8
import threading
import traceback

from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.summary import conferenceMode
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils

Logger = logger_utils.LoggerUtils()


from django.http import JsonResponse

import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
import InvestCopilot_App.models.toolsutils.sendmsg as wx_send
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
import os
import pandas as pd

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dsid_mfactors.settings")
from django.core.cache import cache
# 缓存有效期24小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24

@userLoginCheckDeco
def conferenceHandler(request):
    #组合管理
    rest = ResultData()
    try:
        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        Logger.debug("conferenceHandler request:%s"%reqData)
        if doMethod=="addConferenceInfo":
            user_id = request.session.get("user_id")
            if user_id == "" or user_id is None:
                rstadata = {'msg': "Please Login "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            ctitle=reqData.get("ctitle")
            if ctitle == "" or ctitle is None:
                rstadata = {'msg': "Please enter a ctitle "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a ctitle.")
                return JsonResponse(rest.toDict())
            cdate = reqData.get("cdate")
            if cdate == "" or cdate is None:
                rstadata = {'msg': "Please enter a cdate "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a cdate.")
                return JsonResponse(rest.toDict())
            ctime = reqData.get("ctime")
            if ctime == "" or ctime is None:
                rstadata = {'msg': "Please enter a ctime "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a ctime.")
                return JsonResponse(rest.toDict())
            csource = reqData.get("csource")
            if csource == "" or csource is None:
                rstadata = {'msg': "Please enter a csource "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a csource.")
                return JsonResponse(rest.toDict())
            cregiest = reqData.get("cregiest")
            if cregiest == "" or cregiest is None or cregiest =="False":
                cregiest = False
            elif cregiest=="true" or cregiest=="True" or cregiest==True:
                cregiest=True
            else:
                cregiest=False
            creplay = reqData.get("creplay")
            if creplay == "" or creplay is None or creplay == "False":
                creplay = False
            elif creplay=="true" or creplay=="True" or creplay==True:
                creplay=True
            else:
                creplay=False
            csound = reqData.get("csound")
            if csound == "" or csound is None or csound =="False":
                csound = False
            elif csound =="true" or csound=="True" or csound==True:
                csound=True
            else:
                csound=False
            cremark = reqData.get("cremark")
            if cremark == "" or cremark is None:
                cremark = None
            cm = conferenceMode.conferenceMode()
            rest = cm.addConferenceInfo(ctitle=ctitle,cdate=cdate,ctime=ctime,csource=csource,cuserid=user_id,cregiest=cregiest,creplay=creplay,csound=csound,cremark=cremark)
            return JsonResponse(rest.toDict())
        elif doMethod=="editConferenceInfo":
            user_id = request.session.get("user_id")
            if user_id == "" or user_id is None:
                rstadata = {'msg': "Please Login "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            ctitle=reqData.get("ctitle")
            if ctitle == "" or ctitle is None:
                rstadata = {'msg': "Please enter a ctitle "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a ctitle.")
                return JsonResponse(rest.toDict())
            cdate = reqData.get("cdate")
            if cdate == "" or cdate is None:
                rstadata = {'msg': "Please enter a cdate "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a cdate.")
                return JsonResponse(rest.toDict())
            ctime = reqData.get("ctime")
            if ctime == "" or ctime is None:
                rstadata = {'msg': "Please enter a ctime "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a ctime.")
                return JsonResponse(rest.toDict())
            csource = reqData.get("csource")
            if csource == "" or csource is None:
                rstadata = {'msg': "Please enter a csource "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a csource.")
                return JsonResponse(rest.toDict())
            cid = reqData.get("cid")
            if cid == "" or cid is None:
                rstadata = {'msg': "Please enter a cid "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a cid.")
                return JsonResponse(rest.toDict())
            cregiest = reqData.get("cregiest")
            if cregiest == "" or cregiest is None or cregiest == "False":
                cregiest = False
            elif cregiest == "true" or cregiest == "True" or cregiest == True:
                cregiest = True
            else:
                cregiest = False
            creplay = reqData.get("creplay")
            if creplay == "" or creplay is None or creplay == "False":
                creplay = False
            elif creplay == "true" or creplay == "True" or creplay == True:
                creplay = True
            else:
                creplay = False
            csound = reqData.get("csound")
            if csound == "" or csound is None or csound == "False":
                csound = False
            elif csound == "true" or csound == "True" or csound == True:
                csound = True
            else:
                csound = False
            cremark = reqData.get("cremark")
            if cremark == "" or cremark is None:
                cremark = None
            cm = conferenceMode.conferenceMode()
            rest = cm.editConferenceInfo(cid=cid, ctitle=ctitle, cdate=cdate, ctime=ctime, csource=csource, cuserid=user_id, cregiest=cregiest, creplay=creplay, csound=csound, cremark=cremark)
            return JsonResponse(rest.toDict())
        elif doMethod=="editConferenceCsound":
            user_id = request.session.get("user_id")
            if user_id == "" or user_id is None:
                rstadata = {'msg': "Please Login "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cid = reqData.get("cid")
            if cid == "" or cid is None:
                rstadata = {'msg': "Please enter a cid "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a cid.")
                return JsonResponse(rest.toDict())
            csound = reqData.get("csound")
            if csound == "" or csound is None or csound == "False":
                csound = False
            elif csound == "true" or csound == "True" or csound == True:
                csound = True
                Logger.info("cid:%s,userId:%s need csound:%s!"%(cid,user_id,csound))
            else:
                csound = False
            cm = conferenceMode.conferenceMode()
            rest = cm.editConferencecsound(cid=cid,  cuserid=user_id, csound=csound)
            if rest.errorFlag:
                rownum=rest.data['rownum']
                if rownum>0:
                    threading.Thread(target=notifyMeetingMsg,args=(user_id,cid)).start()
            return JsonResponse(rest.toDict())
        elif doMethod=="delConferenceInfo":
            user_id = request.session.get("user_id")
            if user_id == "" or user_id is None:
                rstadata = {'msg': "Please Login "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cid = reqData.get("cid")
            if cid == "" or cid is None:
                rstadata = {'msg': "Please enter a cid "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a cid.")
                return JsonResponse(rest.toDict())
            cm = conferenceMode.conferenceMode()
            rest = cm.delConferenceInfo(cid=cid, cuserid=user_id)
            return JsonResponse(rest.toDict())
        elif doMethod=="getConferenceInfoOnlyOnedata":
            user_id = request.session.get("user_id")
            if user_id == "" or user_id is None:
                rstadata = {'msg': "Please Login "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cid = reqData.get("cid")
            if cid == "" or cid is None:
                rstadata = {'msg': "Please enter a cid "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please enter a cid.")
                return JsonResponse(rest.toDict())
            cm = conferenceMode.conferenceMode()
            rest = cm.getConferenceInfoOnlyOnedata(cid=cid, userid=user_id)
            return JsonResponse(rest.toDict())
        elif doMethod=="getConferenceInfowithTime":
            pageSize = reqData.get("pageSize")
            if pageSize == "" or pageSize is None:
                pageSize = 10
            else:
                pageSize = int(pageSize)
            page = reqData.get("page")
            if page == "" or page is None:
                page = 1
            else:
                page = int(page)
            user_id = request.session.get("user_id")
            if user_id == "" or user_id is None:
                rstadata = {'msg': "Please Login "}
                rest.data = rstadata
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cdate = reqData.get("cdate")
            cm = conferenceMode.conferenceMode()
            rest = cm.getConferenceInfowithTime(userid=user_id,cdate=cdate,page=page,pageSize=pageSize)
            return JsonResponse(rest.toDict())

    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        return JsonResponse(rest.toDict())

def notifyMeetingMsg(user_id,cid):
    # 发送消息
    try:
        cm = conferenceMode.conferenceMode()
        c_user_dt = user_utils.getCacheUserInfo(user_id)
        userrealname = c_user_dt['USERREALNAME']
        callDF = cm.getCallDataById(cid)
        if not callDF.empty:
            fone = callDF.iloc[0]
            csound = fone.CSOUND
            ctitle = str(fone.CTITLE)
            cdate = str(fone.CDATE)
            ctime = str(fone.CTIME)
            if str(csound) == "true":
                vmsg = "(%s %s)需要录音电话会议[%s] from %s" % (cdate, ctime, ctitle, userrealname)
                Logger.info(vmsg + "[%s]" % tools_utils.globa_vtouser)
                issend = wx_send.send_wx_msg_operation(vmsg, touser=tools_utils.globa_vtouser)
                if not issend:
                    Logger.info(vmsg + "[%s]" % tools_utils.globa_vtouser)
                    wx_send.send_wx_msg_operation(vmsg, touser=tools_utils.globa_vtouser)
            else:
                vmsg = "(%s %s)不需要录音电话会议[%s] from %s" % (cdate, ctime, ctitle, userrealname)
                Logger.info(vmsg + "[%s]" % tools_utils.globa_vtouser)
                issend = wx_send.send_wx_msg_operation(vmsg, touser=tools_utils.globa_vtouser)
                if not issend:
                    Logger.info(vmsg + "[%s]" % tools_utils.globa_vtouser)
                    wx_send.send_wx_msg_operation(vmsg, touser=tools_utils.globa_vtouser)
    except:
        Logger.error(traceback.format_exc())