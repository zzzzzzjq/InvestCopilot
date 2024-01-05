import pandas as pd

import InvestCopilot_App.models.toolsutils.dbutils as dbutils

def hkworkday():
    #https://www.cmschina.com.hk/CS/Calendar
    html="""
    <div class="table_hk table_common content1"> <table> <tbody><tr class="color_lightGray"> <th class="td1">日期</th> <th class="td2">假期</th> <th class="td3"></th> </tr> <tr> <td class="td1">2023年1月2日</td> <td class="td2">元旦翌日</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年1月23日</td> <td class="td2">農曆年初二</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年1月24日</td> <td class="td2">農曆年初三</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年1月25日</td> <td class="td2">農曆年初四</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年4月5日</td> <td class="td2">清明節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年4月7日</td> <td class="td2">耶穌受難節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年4月10日</td> <td class="td2">復活節星期一</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年5月1日</td> <td class="td2">勞動節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年5月26日</td> <td class="td2">佛誕</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年6月22日</td> <td class="td2">端午節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年10月2日</td> <td class="td2">國慶日翌日</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年10月23日</td> <td class="td2">重陽節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年12月25日</td> <td class="td2">聖誕節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年12月26日</td> <td class="td2">聖誕節後第一個周日</td> <td class="td3">休市</td> </tr> </tbody></table> </div>
        """

    import bs4
    bs = bs4.BeautifulSoup(html,"html.parser")

    # 查找表格（table）中所有的行（tr）
    table = bs.find('table')
    rows = table.find_all('tr')

    # 将每一行的文本内容转为列表，并将所有行组合成一个二维列表
    table_data = [[cell.get_text(strip=True) for cell in row.find_all('td')] for row in rows]
    import datetime
    # 输出结果
    noworkdays=[]
    for row in table_data:
        # 解析日期字符串
        print(row)
        if len(row)==0:
            continue
        date_str=row[0]
        date_obj = datetime.datetime.strptime(date_str, "%Y年%m月%d日")

        # 将日期对象格式化为YYYYMMDD格式的字符串
        formatted_date = date_obj.strftime("%Y%m%d")
        print(formatted_date)
        noworkdays.append(formatted_date)

    print("noworkdays:",noworkdays)
    noworkdays=['20240101', '20240212', '20240213', '20240329', '20240401', '20240404', '20240501', '20240515',
     '20240610','20240701', '20240918', '20241001', '20241011', '20241225', '20241226']

    beday='20231231'
    eday='20241231'
    btime=datetime.datetime.strptime(beday,'%Y%m%d')
    hkworkdays=[]
    for i in range(1,369):
        btime=btime+datetime.timedelta(days=1)
        if btime.weekday() in [5,6]:
            continue
        bday=btime.strftime("%Y%m%d")
        if bday in noworkdays:
            continue
        if bday>eday:
            continue
        hkworkdays.append(bday)
        print(
            "INSERT INTO config.hkworkday(workday)VALUES('%s');"%bday
        )


def usworkday():
    #https://www.cmschina.com.hk/CS/Calendar
    html="""
    <div class="table_hk table_common content1"> <table> <tbody><tr class="color_lightGray"> <th class="td1">日期</th> <th class="td2">假期</th> <th class="td3"></th> </tr> <tr> <td class="td1">2023年1月2日</td> <td class="td2">元旦翌日</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年1月23日</td> <td class="td2">農曆年初二</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年1月24日</td> <td class="td2">農曆年初三</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年1月25日</td> <td class="td2">農曆年初四</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年4月5日</td> <td class="td2">清明節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年4月7日</td> <td class="td2">耶穌受難節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年4月10日</td> <td class="td2">復活節星期一</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年5月1日</td> <td class="td2">勞動節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年5月26日</td> <td class="td2">佛誕</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年6月22日</td> <td class="td2">端午節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年10月2日</td> <td class="td2">國慶日翌日</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年10月23日</td> <td class="td2">重陽節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年12月25日</td> <td class="td2">聖誕節</td> <td class="td3">休市</td> </tr> <tr> <td class="td1">2023年12月26日</td> <td class="td2">聖誕節後第一個周日</td> <td class="td3">休市</td> </tr> </tbody></table> </div>
        """

    import bs4
    bs = bs4.BeautifulSoup(html,"html.parser")

    # 查找表格（table）中所有的行（tr）
    table = bs.find('table')
    rows = table.find_all('tr')

    # 将每一行的文本内容转为列表，并将所有行组合成一个二维列表
    table_data = [[cell.get_text(strip=True) for cell in row.find_all('td')] for row in rows]
    import datetime
    # 输出结果
    noworkdays=[]
    for row in table_data:
        # 解析日期字符串
        print(row)
        if len(row)==0:
            continue
        date_str=row[0]
        date_obj = datetime.datetime.strptime(date_str, "%Y年%m月%d日")

        # 将日期对象格式化为YYYYMMDD格式的字符串
        formatted_date = date_obj.strftime("%Y%m%d")
        print(formatted_date)
        noworkdays.append(formatted_date)

    print("noworkdays:",noworkdays)
    noworkdays=['20240101', '20240115', '20240219', '20240329', '20240401', '20240527', '20240619',
 '20240704', '20240902', '20241128',  '20241224', '20241225']

    beday='20231231'
    eday='20241231'
    btime=datetime.datetime.strptime(beday,'%Y%m%d')
    hkworkdays=[]
    for i in range(1,369):
        btime=btime+datetime.timedelta(days=1)
        if btime.weekday() in [5,6]:
            continue
        bday=btime.strftime("%Y%m%d")
        if bday in noworkdays:
            continue
        if bday>eday:
            continue
        hkworkdays.append(bday)
        print(
            "INSERT INTO config.usaworkday(workday)VALUES('%s');"%bday
        )


def us2000():
    rd  = pd.read_excel("c:/Users/env/Downloads/2000_stocks(3).xlsx")
    print(rd)
    #RIC	Company Common Name	Ticker

    i_sql=" insert into  config.lastus2000 (ric,stockname,ticker) values(%s,%s,%s)"
    rdat = rd.values.tolist()
    con,cur = dbutils.getConnect()
    cur.executemany(i_sql,rdat)
    con.commit()
    cur.close()
    con.close()

if __name__ == '__main__':
    # hkworkday()
    # usworkday()
    # us2000()

    import pandas as pd

    # 示例数据
    data = {'A': ['group1', 'group1', 'group2', 'group2', 'group3'],
            'B': [-5, 10, 15, -20, 25]}
    df = pd.DataFrame(data)

    # 找到每个组中 B 列绝对值最大的行的索引
    max_abs_index = df.groupby('A')['B'].apply(lambda x: x.abs().idxmax())

    # 根据索引获取最终结果
    result_df = df.loc[max_abs_index]

    print(result_df)



    pass