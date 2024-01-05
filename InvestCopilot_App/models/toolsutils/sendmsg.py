import json
import datetime
import requests
import traceback

import InvestCopilot_App.models.toolsutils.LoggerUtils as Logger_utils
Logger = Logger_utils.LoggerUtils()

glob_wx_userId='robby.xia'
glob_wx_trade_userId=['robby.xia','michael']

def getWxKey(wxId):
    if wxId == "smartset":
        corpid = "wwbc4232e645d1f018"
        secret = "c8EPHOLdt8v2HM0YpP-3kDJIU8l0xlyRob5kXevkynw"
        agentid = "1000002"
        return corpid, secret, agentid
    elif wxId == "inari_old":
        corpid = "wwa2f8b692afdd0849"
        secret = "rRxJz7U_cOBl73ZiHYbovfFaLBVlpY1mSjf3xOx4iRE"
        agentid = "1000002"
        return corpid, secret, agentid

    elif wxId == "trade_old":  # 交易通知
        corpid = "wwa2f8b692afdd0849"
        secret = "55So7nvVnJTCdieSw7rEdyl4VUmR3aJfzxCrK6NPNO8"
        agentid = "1000005"
        return corpid, secret, agentid


    elif wxId == "inari":  # 企业微信通知
        corpid = "ww7b1f7b8eeec676a8"
        secret = "vU5JwyMRYrSnCWZRhFGWj6kWCgMAjLMA8x5z2KuzxjE"
        agentid = "1000002"
        return corpid, secret, agentid


    elif wxId == "trade":  # 交易通知
        corpid = "ww7b1f7b8eeec676a8"
        secret = "25_VqETM2eWflPyTP1lfDVAf45dy4eYSl0A4AiqiBzY"
        agentid = "1000003"
        return corpid, secret, agentid
    elif wxId == "strategy":  # 策略消息通知
        corpid = "ww7b1f7b8eeec676a8"
        secret = "rFDurH30cXf1uTeWTofgeod9lRqfZ3jgzAgchILQMYo"
        agentid = "1000005"
        return corpid, secret, agentid


    return '', '', ''


class WeChat(object):
    def __init__(self, corpid, secret, agentid):
        self.url = "https://qyapi.weixin.qq.com"
        self.corpid = corpid
        self.secret = secret
        self.agentid = agentid

    # 获取企业微信的 access_token
    def access_token(self):
        url_arg = '/cgi-bin/gettoken?corpid={id}&corpsecret={crt}'.format(
            id=self.corpid, crt=self.secret)
        url = self.url + url_arg
        response = requests.session().get(url=url)
        text = response.text
        self.token = json.loads(text)['access_token']

    def upload(self, mediaPath, type):
        self.access_token()
        send_url = '{url}/cgi-bin/media/upload?access_token={token}'.format(
            url=self.url, token=self.token)
        files = {'file': open(mediaPath, 'rb')}
        # print("files:", files)
        # headers={'Content-Disposition':'form-data; name="media";filename="wework.txt"; filelength=6'}
        values = {
            "type": type,
        }
        # files = {'file': open('D:/test/test.xlsx', 'rb')}
        #            "media": files,
        response = requests.post(url=send_url, data=values, files=files)
        # print("resp:", response.text)
        rsp = json.loads(response.text)
        return rsp

    # 构建消息格式
    def messages(self, msg='', media_id='', msgtype='text', textcard={}, touser='@all'):
        if msgtype == 'file':
            values = {
                # "touser": '@all',
                "touser": touser,  # touser	否	成员ID列表（消息接收者，多个接收者用‘|’分隔，最多支持1000个）。特殊情况：指定为@all，则向该企业应用的全部成员发送
                "msgtype": msgtype,
                "agentid": self.agentid,
                "file": {
                    "media_id": media_id
                },
                "safe": 0
            }
        elif msgtype == "textcard":
            values = {
                "touser": touser,
                "agentid": self.agentid,
                "msgtype": msgtype,
                "textcard": textcard,
                "safe": 0
            }
        else:
            values = {
                "touser": touser,
                "msgtype": msgtype,
                "agentid": self.agentid,
                "text": {'content': msg},
                "safe": 0
            }
        # python 3
        # self.msg = (bytes(json.dumps(values), 'utf-8'))
        # python 2
        self.msg = json.dumps(values)

    # 发送信息
    def send_message(self, msg='', media_id='', msgtype='text', textcard={}, touser='@all'):
        self.access_token()
        self.messages(msg, media_id=media_id, msgtype=msgtype, textcard=textcard, touser=touser)

        send_url = '{url}/cgi-bin/message/send?access_token={token}'.format(
            url=self.url, token=self.token)
        response = requests.post(url=send_url, data=self.msg)
        # print("response.text:", response.text)
        errcode = json.loads(response.text)['errcode']

        if errcode == 0:
            # print('Succesfully')
            return True
        else:
            # print('Failed')
            return False


def send_wx_msg(msg, media_id='', msgtype='text', textcard={}, wxId='smartset', touser='@all'):
    try:
        orgMsg = msg
        if wxId == "smartset":
            msg = "[阿里云]" + msg[0:300]
        else:
            msg = msg[0:300]

        corpid, secret, agentid = getWxKey(wxId)

        # 记录日志
        # filePath = sys.path[0]
        # fileCtx = os.path.join(filePath, "logs")
        # if not os.path.exists(fileCtx):
        #     os.makedirs(fileCtx)

        # exeDay = datetime.datetime.now().strftime("%Y%m%d")
        # fileName = os.path.join(fileCtx, "runlog_" + exeDay + ".log")
        # with open(fileName, mode='a') as wf:
        #     wf.write(orgMsg + "\n******************\n")

        wechat = WeChat(corpid, secret, agentid)
        # wechat.send_message(msg)

        return wechat.send_message(msg, media_id=media_id, msgtype=msgtype, textcard=textcard, touser=touser)
    except:
        errorMsg="发送微信提示信息失败"
        Logger.error(errorMsg)
        errorMsg = traceback.format_exc()
        print(errorMsg)
        Logger.error(errorMsg)
    return False

def send_wx_msg_operation(msg, media_id='', msgtype='text', textcard={}, wxId='inari', touser=glob_wx_userId):
    try:

        if wxId == "smartset":
            msg = "[阿里云]" + msg[0:300]
        else:
            msg = msg[0:300]
        corpid, secret, agentid = getWxKey(wxId)
        wechat = WeChat(corpid, secret, agentid)
        return wechat.send_message(msg, media_id=media_id, msgtype=msgtype, textcard=textcard, touser=touser)
    except:
        errorMsg = "发送微信提示信息失败"
        Logger.error(errorMsg)
        errorMsg = traceback.format_exc()
        print(errorMsg)
        Logger.error(errorMsg)
        return False


# https://work.weixin.qq.com/api/doc#90000/90135/90253
def uploadFile(mediaPath, type, wxId="smartset"):
    try:
        corpid, secret, agentid = getWxKey(wxId)
        wechat = WeChat(corpid, secret, agentid)
        return wechat.upload(mediaPath, type)
    except:
        errorMsg = "发送微信提示信息失败"
        Logger.error(errorMsg)
        errorMsg = traceback.format_exc()
        print(errorMsg)
        Logger.error(errorMsg)

        return {'errcode': 'exception', "errmsg": errorMsg}


def send_upload_file(filePath, wxId='inari', type='file', touser='@all'):
    """
    企业微信消息推送文件
    :param filePath:文件路径
    :return:
    """
    rsMsg = uploadFile(mediaPath=filePath, type=type, wxId=wxId)
    if rsMsg['errcode'] == 0:
        media_id = rsMsg['media_id']
        type = rsMsg['type']
        return send_wx_msg("text", media_id=media_id, msgtype=type, wxId=wxId, touser=touser)


def getAccessToken():
    # https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=APPID&secret=APPSECRET

    appid = "wx1f905356833887b4"
    secret = "381f3787d26f5c3913ebdc44c97b69a3"

    url = "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential" \
          "&appid={}" \
          "&secret={}".format(appid, secret)
    resp = requests.get(url)
    print(resp.status_code)
    print(resp.json())
    resp_json = resp.json()
    access_token = resp_json['access_token']

    return access_token


def subscribeSend(userOpenId,direction):
    #订单生成通知 HKiBBUDIsSysH4fphPKFl7bn3jrMgqaWQmH8kLuTga0
    tradeKey="HKiBBUDIsSysH4fphPKFl7bn3jrMgqaWQmH8kLuTga0"
    #订单确认通知 _nUlhdCNLHHXz20V6359a76YEP0Gt4HLGylEuUEbRuw
    confirmKey="_nUlhdCNLHHXz20V6359a76YEP0Gt4HLGylEuUEbRuw"

    template_id="XXXXX"
    if direction=="trade":#买入/卖出
        template_id = tradeKey

    if direction=="confirm":#交易后复核确认
        template_id = confirmKey

    """
    #订单生成通知 HKiBBUDIsSysH4fphPKFl7bn3jrMgqaWQmH8kLuTga0
    详细内容
    商品名称
    {{thing5.DATA}}    
    报告类型
    {{thing7.DATA}}    
    订单状态
    {{thing3.DATA}}    
    备注
    {{thing4.DATA}}    
    订单时间
    {{date2.DATA}}
    """

    """
    #订单确认通知 _nUlhdCNLHHXz20V6359a76YEP0Gt4HLGylEuUEbRuw
    详细内容
    订单商品
    {{thing2.DATA}}    
    订单内容
    {{thing6.DATA}}    
    确认时间
    {{time5.DATA}}    
    备注
    {{thing7.DATA}}
    """

    assess_token = getAccessToken()
    url = "https://api.weixin.qq.com/cgi-bin/message/subscribe/send?access_token={}".format(assess_token)

    openId=userOpenId#"oA6e-4uB-SwJQzIvb3sgViwHx-ck"
    #[TEST2组合] 微盟集团(2013.HK) buy 1000 GXZQ [11:08:57]

    tradeTime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%s")
    print("tradeTime:",tradeTime)
    data = {
        "touser": openId,
        "template_id": template_id,
        "page": "index",
        "miniprogram_state": "developer",
        "lang": "zh_CN",
        "data": {
            "thing5": {
                "value": "贵州茅台(600519.SH)"
            },
            "thing7": {
                "value": "Buy"
            },
            "thing3": {
                "value": "报送"
            },
            "thing4": {
                "value": "Buy 1000 GXZQ"
            },
            "date2": {
                "value": tradeTime
            }
        }
    }

    resp=requests.post(url,data=json.dumps(data))
    print(resp.status_code)
    print(resp.text)



if __name__ == '__main__':
    # corpid = "wwbc4232e645d1f018"
    # secret = "c8EPHOLdt8v2HM0YpP-3kDJIU8l0xlyRob5kXevkynw"
    # agentid = "1000002"
    # msg = "mysql 出现错误"
    #
    # wechat = WeChat(corpid, secret, agentid)
    # wechat.send_message(msg)
    # send_wx_msg_operation('sssssss')
    # wx_test()
    # subscribeSend()

    # send_wx_msg("测试消息通知，请忽略！", wxId="trade")
    send_wx_msg("1测试消息通知，请忽略！", wxId="inari", touser="robby.xia")
    send_wx_msg("2测试消息通知，请忽略！", wxId="inari", touser="robby.xia")
    send_wx_msg("3测试消息通知，请忽略！", wxId="inari", touser="robby.xia")
    send_wx_msg("4测试消息通知，请忽略！", wxId="inari", touser="robby.xia")
    # send_wx_msg_operation("测试消息通知，请忽略！")
    # send_wx_msg("系统提示：各位研究员，关注覆盖的公司模型和目标价是否需调整，请及时更新！", wxId="inari", touser="robby.xia|jingyuan_zou|ellie.tang|celia.xie|vincent@pinnacle-cap.cn|yangzhan")
    pass
