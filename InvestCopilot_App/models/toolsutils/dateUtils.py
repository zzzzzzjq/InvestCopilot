
import traceback
import calendar
import datetime


def get_weekday(datetime_obj, week_day="monday"):
    """
    获取指定时间的当周的星期x
    :param datetime_obj: 时间
    :param week_day: 指定的星期x
    :return:
    """
    d = dict(zip(("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"), range(7)))  # datetime 模块中，星期一到星期天对应数字 0 到 6
    delta_hour = datetime.timedelta(days=1)  # 改变幅度为 1 天
    while datetime_obj.weekday() != d.get(week_day):
        if datetime_obj.weekday() > d.get(week_day):
            datetime_obj -= delta_hour
        elif datetime_obj.weekday() < d.get(week_day):
            datetime_obj += delta_hour
        else:
            pass
    return datetime_obj

def get_first_and_last_weekday(year, month, n=1, w="monday"):
    """
    获取 year 年，month 月 的第n个星期w和倒数第n个星期w的日期
    :param year: 指定年份，如 2019
    :param month: 指定月份，如 6
    :param n: 第n个
    :param w: 指定的星期w
    :return:
    """
    # 获取第一和最后一天
    d = dict(zip(("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"), range(7)))  # datetime 模块中，星期一到星期天对应数字 0 到 6
    weekday, count_day = calendar.monthrange(year=year, month=month)  # 返回指定月份第一天（即1号）的星期日期，和本月的总天数 https://blog.csdn.net/tz_zs/article/details/86629959
    first_day = datetime.datetime(year=year, month=month, day=1)  # <type 'datetime.datetime'>
    last_day = datetime.datetime(year=year, month=month, day=count_day)
    # first_day, last_day = get_month_firstday_and_lastday(year=year, month=month, n=1)

    # 第1个星期w
    if first_day.weekday() > d.get(w):  # 说明本周的星期w在上个月
        datetime_obj = first_day + datetime.timedelta(weeks=1)
    else:
        datetime_obj = first_day
    datetime_obj += datetime.timedelta(weeks=n - 1)
    first_weekday = get_weekday(datetime_obj=datetime_obj, week_day=w)

    # 倒数第1个星期w
    if last_day.weekday() < d.get(w):  # 说明本周的星期w在下一个月
        datetime_obj = last_day - datetime.timedelta(weeks=1)
    else:
        datetime_obj = last_day
    datetime_obj -= datetime.timedelta(weeks=n - 1)
    last_weekday = get_weekday(datetime_obj=datetime_obj, week_day=w)

    return first_weekday, last_weekday

def amTradeSeason(nowDay=""):
    """
    美股交易 夏令 冬令
    :return:
    """
    # 22年3月的第二个星期日 夏令
    if nowDay=="":
        nowDay = datetime.datetime.now()
        n_year = nowDay.year
        nowDay = datetime.datetime.strptime(nowDay.strftime("%Y-%m-%d"), '%Y-%m-%d')
    else:
        nowDay = datetime.datetime.strptime(nowDay, '%Y-%m-%d')
        n_year = nowDay.year

    xl_weekday, last_weekday = get_first_and_last_weekday(year=n_year, month=3, n=2, w="sunday")
    # print(xl_weekday.date())
    # 令时为每年11月的第一个星期日
    dl_weekday, last_weekday = get_first_and_last_weekday(year=n_year, month=11, n=1, w="sunday")
    # print(dl_weekday.date(), type(dl_weekday.date()))
    # print(nowDay, type(nowDay))
    if nowDay > xl_weekday and nowDay < dl_weekday:
        # print("夏令")
        return "summer"
    else:
        # print("冬令")
        return "winter"
