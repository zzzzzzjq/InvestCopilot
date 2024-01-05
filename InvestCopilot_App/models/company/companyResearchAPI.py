# encoding: utf-8
#公司内部研究报告
import datetime
import json
import re
import os
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.company.companyResearchMode import companyMode
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
from InvestCopilot_App.models.summary.viewSummaryMode import viewSummaryMode
from InvestCopilot_App.models.cache import cacheDB as cache_db
from django.http import JsonResponse
from django.http import HttpResponse
from django.http import HttpResponse, FileResponse
from mimetypes import guess_type
Logger = logger_utils.LoggerUtils()


@userLoginCheckDeco
def companyResearchApi(request):
    rest = ResultData()
    try:
        user_id = request.session.get("user_id")
        rest = tools_utils.requestDataFmt(request, fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod = reqData.get("doMethod")
        # Logger.debug("companyAPIHandler request:%s"%reqData)
        if doMethod == "addCompanyReport":
            windCode = reqData.get("windCode")
            title = reqData.get("title")
            summaryText = reqData.get("summaryText")
            rating = reqData.get("rating")#评级
            priceTarget = reqData.get("priceTarget")#目标价
            if title is None:
                rest = rest.errorData(errorMsg="主题不能为空")
                return JsonResponse(rest.toDict())
            if summaryText is None:
                rest = rest.errorData(errorMsg="内容不能为空")
                return JsonResponse(rest.toDict())
            # 检查行业名称(100)、主题(200)、核心观点(2048)的字数是否超过最大限制
            #19467360
            #20000000
            #15728640 15M
            #10000000
            if tools_utils.charLength(summaryText, 10000000).errorFlag is False:
                rest = rest.errorData(errorMsg='报告内容超过字数限制，如果存在图片请进行压缩处理')
                return JsonResponse(rest.toDict())
            if tools_utils.charLength(title, 200).errorFlag is False:
                rest = rest.errorData(errorMsg='主题超过字数限制')
                return JsonResponse(rest.toDict())
            if windCode is None or windCode =="":
                rest=rest.errorData(errorMsg="证券代码不能为空")
                return JsonResponse(rest.toDict())
            stockInfo_dt=cache_dict.getCacheStockInfo(windCode)
            if not isinstance(stockInfo_dt,dict):
                rest=rest.errorData(errorMsg="证券代码[%s]不存在"%(windCode))
                return JsonResponse(rest.toDict())
            #上传附件
            file_objs =request.FILES.values()
            c_user_dt = user_utils.getCacheUserInfo(user_id)
            if c_user_dt is None:
                rest.errorData(errorMsg="用户信息异常，请重新选择！")
                return rest
            companyId = c_user_dt['COMPANYID']
            publishOn = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cpid = "company_" + tools_utils.md5("%s%s%s" % (windCode, title, publishOn))
            # 保存文件
            file_full_path = r'D:\work\share\web\upload\company\%s\%s' % (companyId,user_id)
            if not os.path.exists(file_full_path):
                os.makedirs(file_full_path)
            attachments=[]
            for _idx,file_obj in enumerate(file_objs):
                file_name = file_obj.name
                file_name_text,fileType = os.path.splitext(file_name)
                attachmentId=datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')+str(_idx)
                s_file_name = attachmentId+fileType # 不能使用文件名称，因为存在中文，会引起内部错误
                sfile_full_path = os.path.join(file_full_path, s_file_name)
                with open(sfile_full_path, 'wb+') as dest:
                    dest.write(file_obj.read())
                attachments.append({"attachmentId":attachmentId,"attachmentPath":sfile_full_path,"fileType":fileType,"attachmentName":file_name})
            windName = cache_dict.translateStockWindCode(windCode)
            rest = companyMode().addCompanyReport(cpid,windCode, windName, user_id,title, summaryText,publishOn, rating=rating, priceTarget=priceTarget,
                     attachments=attachments)
            return JsonResponse(rest.toDict())
        elif doMethod == "editCompanyReport":
            reportId = reqData.get("reportId")
            title = reqData.get("title")
            summaryText = reqData.get("summaryText")
            rating = reqData.get("rating")
            priceTarget = reqData.get("priceTarget")
            if reportId is  None:
                rest = rest.errorData(errorMsg='请选择需要修改的报告')
                return JsonResponse(rest.toDict())
            if summaryText is not None:
                if tools_utils.charLength(summaryText, 10000000).errorFlag is False:#10M
                    rest = rest.errorData(errorMsg='报告内容超过字数限制，如果存在图片请进行压缩处理')
                    return JsonResponse(rest.toDict())
            if title is not None:
                if tools_utils.charLength(title, 200).errorFlag is False:
                    rest = rest.errorData(errorMsg='主题超过字数限制')
                    return JsonResponse(rest.toDict())
            # 上传附件
            file_objs = request.FILES.values()
            c_user_dt = user_utils.getCacheUserInfo(user_id)
            if c_user_dt is None:
                rest.errorData(errorMsg="用户信息异常，请重新选择！")
                return rest
            companyId = c_user_dt['COMPANYID']
            # 保存文件
            file_full_path = r'D:\work\share\web\upload\company\%s\%s' % (companyId, user_id)
            if not os.path.exists(file_full_path):
                os.makedirs(file_full_path)
            attachments = []
            for _idx, file_obj in enumerate(file_objs):
                file_name = file_obj.name
                file_name_text, fileType = os.path.splitext(file_name)
                attachmentId = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f') + str(_idx)
                s_file_name = attachmentId + fileType  # 不能使用文件名称，因为存在中文，会引起内部错误
                sfile_full_path = os.path.join(file_full_path, s_file_name)
                with open(sfile_full_path, 'wb+') as dest:
                    dest.write(file_obj.read())
                attachments.append(
                    {"attachmentId": attachmentId, "attachmentPath": sfile_full_path, "fileType": fileType,
                     "attachmentName": file_name})
            rest = companyMode().editCompanyReport( reportId , user_id,title=title, summaryText=summaryText,
                                                    rating=rating, priceTarget=priceTarget,
                                                    attachments=attachments)
            return JsonResponse(rest.toDict())
        elif doMethod == "delCompanyReport":
            reportId = reqData.get("reportId")
            if reportId is None:
                rest = rest.errorData(errorMsg='请选择需要删除的报告')
                return JsonResponse(rest.toDict())
            rest = companyMode().delCompanyReport( user_id,reportId)
            return JsonResponse(rest.toDict())
        elif doMethod == "delCompanyReportAttachment":
            reportId = reqData.get("reportId")
            attachmentId = reqData.get("attachmentId")
            if reportId is  None:
                rest = rest.errorData(errorMsg='请选择需要删除的报告')
                return JsonResponse(rest.toDict())
            if attachmentId is  None:
                rest = rest.errorData(errorMsg='请选择需要删除的报告附件')
                return JsonResponse(rest.toDict())
            rest = companyMode().delCompanyReportAttachment( user_id,reportId,attachmentId)
            return JsonResponse(rest.toDict())
        elif doMethod == "getCompanyReport":#
            #搜索
            translation = reqData.get("translation")
            myResearch = reqData.get("myResearch")
            if translation is None:
                translation="zh"
            if myResearch is not  None:#我的研报
                userIds=[user_id]
            else:#公司研报
                userCfg_dt = cache_db.getUserConfig_dt()
                if user_id in userCfg_dt:
                    c_user_dt=userCfg_dt[user_id]
                    companyId = c_user_dt['COMPANYID']
                    companyUsers=[]
                    for _userId, _uinfo in userCfg_dt.items():
                        if str(_uinfo['COMPANYID']) == str(companyId):
                            companyUsers.append(_userId)
                    userIds=companyUsers
                else:
                    rest.errorData(errorMsg="用户信息异常，请重新选择！")
                    return rest

            searchTitle = reqData.get("searchTitle")#标题搜索
            queryTitle=None
            if searchTitle is not None:
                searchTitle = str(searchTitle).strip()
                if len(searchTitle)>0:
                    queryTitle=searchTitle
            page = reqData.get("page")
            pageSize = reqData.get("pageSize")
            if page is None:
                page = 1
            else:
                page = int(page)
            if pageSize is None:
                pageSize = 20
            else:
                pageSize = int(pageSize)
            rest = companyMode().getSummaryDataByPage("website", "company", queryTitle=queryTitle,
                                                                  translation=translation, userIds=userIds, page=page,
                                                                  pageSize=pageSize)
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            rtdata = rest.data
            summaryDatas=rtdata["data"]
            # 提交日期  证券公司  主题 评级 提交人 编辑 删除
            tableData = []
            for sd in summaryDatas:
                sid = sd['id']
                title = sd['title']
                windCode = sd['windCode']
                publishOn = sd['publishOn'][0:10]
                monthRating=None
                editFlag="0"
                if "cuserId" in sd:
                    cuserId = sd['cuserId']
                    if str(cuserId)==user_id:
                        editFlag="1" #用户自己的可编辑
                companydesc=None
                if "nusername" in sd:
                    ncompanyname = sd['ncompanyname']
                    nusername = sd['nusername']
                    companydesc=nusername#"%s/%s"%(nusername,ncompanyname)
                if "monthRating" in sd:
                    monthRating = sd['monthRating']
                priceTarget=None
                if "priceTarget" in sd:
                    priceTarget = sd['priceTarget']
                attachments = sd['attachments']
                tableData.append([sid, publishOn, windCode, title, monthRating,priceTarget,attachments,companydesc,editFlag])  # ,windCode,windCode
            tableColumns = ["ID", "提交日期", '证券公司', '主题','评级','目标价',"附件数量","提交人","可编辑状态"]  #
            rtdata["data"]={"columns":tableColumns,"data":tableData}
            return JsonResponse(rest.toDict())
        elif doMethod == "viewCompanyResearchAttachment":
            # pdf展示
            attachmentId = reqData.get("attachmentId")
            reportId = reqData.get("reportId")
            if reportId == "" or reportId is None:
                rest.errorData(errorMsg="Please choose research file")
                return JsonResponse(rest.toDict())
            if attachmentId == "" or attachmentId is None:
                rest.errorData(errorMsg="Please choose attachment file")
                return JsonResponse(rest.toDict())
            # 检查用户权限
            rest = viewSummaryMode().getDocContent([reportId], rtcolumns={"attachments": 1})
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            rtdata = rest.data
            filepath = None
            attachmentName = None
            fileType = None
            if rtdata['count'] > 0:
                qdata = rtdata['data']
                if len(qdata) > 0:
                    qdt = qdata[0]
                    if "attachments" in qdt:
                        attachments = qdt['attachments']
                        for attachment in attachments:
                            dbattachmentId=attachment['attachmentId']
                            filepath=attachment['attachmentPath']
                            attachmentName=attachment['attachmentName']
                            fileType=attachment['fileType']
                            if dbattachmentId ==attachmentId:
                                break
            if filepath is None:
                rest.errorData(errorMsg="文件不存在！")
                return JsonResponse(rest.toDict())
            # fp, filename = os.path.split(filepath)
            # 获取文件路径
            # 加载文件
            # 看是否直接下载，或者打开
            # 这里传入userid是为了以后数据权限控制
            # filepath,reachFileName = report_mode.getReserchFile(userid, researchId)
            # filepath = r'C:\Users\env\Downloads\19978009.pdf'
            # filepath = r'Z:\cicc\reserach\0027__HK\银河娱乐_期待银河3期放量.pdf'
            # reachFileName = "银河娱乐_期待银河3期放量.pdf"
            # 如果文件不下载，直接打开，那么在这里直接返回文件流
            if fileType == '.pdf':
                content_type = 'application/pdf'
                with open(filepath, 'rb') as wf:
                    fliedata = wf.read()
                    response = HttpResponse(fliedata, content_type)
                    # Set the Content-Disposition header to suggest a filename
                # response['Content-Disposition'] = 'attachment; filename="%s"'% quote(filename)
                # 如果文件不下载，直接打开，那么在这里直接返回文件流
                # if not download:
                return response
            else:
                # 设置文件类型和文件名
                content_type, encoding = guess_type(filepath)
                content_type = content_type or 'application/octet-stream'
                # 打开文件并准备响应
                with open(filepath, 'rb') as file:
                    response = HttpResponse(file.read())
                response['Content-Type'] = content_type
                response[
                    'Content-Disposition'] = f'{"inline" if fileType in [".pdf",".png",".jpeg",".jpg",".gif"] else "attachment"}; filename="{attachmentName}"'
                return response
            import base64
            # 可以根据不同的头实现文件流式下载
            """
            case "pdf": ctype="application/pdf"; break; 
            case "exe": ctype="application/octet-stream"; break; 
            case "zip": ctype="application/zip"; break; 
            case "doc": ctype="application/msword"; break; 
            case "xls": ctype="application/vnd.ms-excel"; break; 
            case "ppt": ctype="application/vnd.ms-powerpoint"; break; 
            case "gif": ctype="image/gif"; break; 
            case "png": ctype="image/png"; break; 
            case "jpeg":ctype="image/jpg"; break;  
            case "jpg": ctype="image/jpg"; break; 
            default: $ctype="application/force-download"; 
            """
            with open(filepath, 'rb') as wf:
                fliedata = wf.read()
            rest.file = 'data:application/pdf;base64,' + \
                        base64.b64encode(fliedata).decode('ascii')
            rest.filename = filename
            return JsonResponse(rest.toDict())

        else:
            errorMsg = '抱歉，请求类别不存在，请重新选择。'
            rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())

    except Exception as ex:
        errorMsg = '抱歉，数据处理失败，请稍后重试。'
        Logger.errLineNo(msg=errorMsg)
        rest.errorData(errorMsg=errorMsg)
        return JsonResponse(rest.toDict())
