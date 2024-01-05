# encoding: utf-8
from django.http import JsonResponse
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.business.likeMode import likeMode
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
Logger = logger_utils.LoggerUtils()

@userLoginCheckDeco
def likeAPIHandler(request):
    #用户点赞
    rest = ResultData()
    try:
        crest=tools_utils.requestDataFmt(request,fefault=None)
        if not crest.errorFlag:
            return JsonResponse(crest.toDict())
        reqData = crest.data
        doMethod=reqData.get("doMethod")
        Logger.debug("likeAPIHandler request:"%reqData)
        user_id = request.session.get("user_id")
        if doMethod=="addLike":#点赞
            sid=reqData.get("sid")
            markType=reqData.get("markType")
            slike=reqData.get("slike")#IMP,NP,LP 重要 (Important) 一般Normal Priority (NP)   不重要  Low Priority (LP)
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if slike=="" or slike is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if str(slike) not in ['IMP','NP',"LP","N","P","Cancel","1","2","3","4","5"]:
                rest = rest.errorData(errorMsg='slike参数错误')
                return JsonResponse(rest.toDict())
            if markType is None:
                markType="summary"
            else:
                if  markType not in ['summary',"news","stockchat"]:
                    rest = rest.errorData(errorMsg='markType参数错误')
                    return JsonResponse(rest.toDict())
            rest = likeMode().addLike(sid,user_id,slike,markType=markType)
            return JsonResponse(rest.toDict())
        if doMethod=="delLike":
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest = likeMode().delLike(sid,user_id)
            return JsonResponse(rest.toDict())
        elif doMethod=="getLikeNum":
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest1 = likeMode().getLikeNum(sid)#点赞总数
            rest2 = likeMode().getLikeData(sid,user_id)#我是否有点赞
            rest.data={"myLike":rest2.data,"likes":rest1.data}
            return JsonResponse(rest.toDict())
        if doMethod=="addComments":
            #添加点评
            sid=reqData.get("sid")
            scomments=reqData.get("scomments")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if scomments=="" or scomments is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            # 评论数据检查
            if tools_utils.charLength(scomments, 1000).errorFlag is False:
                rest = rest.errorData(errorMsg='评论内容超过字数限制')
                return JsonResponse(rest.toDict())
            rest = likeMode().addComments(sid,user_id,scomments)
            return JsonResponse(rest.toDict())
        if doMethod=="delComments":
            cid=reqData.get("cid")
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if cid=="" or cid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest = likeMode().delComments(cid,sid,user_id)
            return JsonResponse(rest.toDict())
        elif doMethod=="getCommentsData":
            #考虑分页处理
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            user_privilegeset = None
            companyId = None
            userCfg_dt = cache_db.getUserConfig_dt()
            if user_id in userCfg_dt:
                userCfg = userCfg_dt[user_id]
                user_privilegeset = userCfg['PRIVILEGESET']
                companyId = str(userCfg['COMPANYID'])
            pageSize = reqData.get("pageSize")
            if pageSize=="" or pageSize is None:
                pageSize=10
            else:
                pageSize=int(pageSize)
            page = reqData.get("page")
            if page=="" or page is None:
                page=1
            else :page=int(page)
            if user_privilegeset=="super":
                #能查看所有公司的对该摘要的点评
                rest = likeMode().getCommentsData(sid=sid,userId=None,companyId=None,page=page,pageSize=pageSize)
            else:
                #能查看用户所属公司的对该摘要的点评
                rest = likeMode().getCommentsData(sid=sid, userId=None, companyId=companyId,page=page,pageSize=pageSize)
            #返回数据处理
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            fmtdatas = rest.data
            editFalg=True
            rtdata= {"page": fmtdatas["page"], "pageSize": fmtdatas["pageSize"], "totalNum": fmtdatas["totalNum"], "data": [],"pageTotal":fmtdatas["pageTotal"]}
            for fdt in fmtdatas["data"]:#comments,userid,updatetime,cid
                cuserid=fdt[1]
                rt_dt={"cid":fdt[3],"editFlag":-1,"comments":fdt[0],"updateTime":fdt[2],"vtag":""}
                if str(cuserid)==str(user_id):#可以编辑
                    if editFalg:
                        editFalg=False
                        rt_dt["editFlag"]=1
                #vtag
                if cuserid in userCfg_dt:
                    userCfg = userCfg_dt[cuserid]
                    rt_dt["vtag"] ="%s/%s"%(userCfg['USERREALNAME'],userCfg['SHORTCOMPANYNAME'])
                rtdata["data"].append(rt_dt)
            rest.data=rtdata
            return JsonResponse(rest.toDict())

        elif doMethod=="getCommentsNum":
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest = likeMode().getCommentsNum(sid)
            return JsonResponse(rest.toDict())
        else:
            rest.errorData("likeAPI not find")
            return JsonResponse(rest.toDict())

        return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        #UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())

