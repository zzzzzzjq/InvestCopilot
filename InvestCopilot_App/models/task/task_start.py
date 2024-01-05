import traceback
import datetime
import time
import os
import logging
import socket
import sys
sys.path.append("../../..")
from logging.handlers import TimedRotatingFileHandler
from apscheduler.schedulers.background import BackgroundScheduler
from InvestCopilot_App.models.portfolio import portfolioMode
from InvestCopilot_App.models.market.spider.hk import eastHkMarket as east_hk_mk
from InvestCopilot_App.models.market.spider.us import eastUSMarket as east_us_mk
from InvestCopilot_App.models.market.spider.ch import eastCHMarket as east_ch_mk
from InvestCopilot_App.models.task.taskMode import taskMode
from InvestCopilot_App.models.business.strategy.strategyMode import strategyMode



scheduler = BackgroundScheduler()
logging.basicConfig(level=logging.DEBUG)
LOG_FILE = 'copilot_schedulers.log'
fh = TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30)
# 定义一个Handler打印INFO及以上级别的日志到sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# 设置日志打印格式
formatter = logging.Formatter('%(levelname)s:%(message)s')
console.setFormatter(formatter)
# 将定义好的console日志handler添加到root logger
logging.getLogger('').addHandler(console)
logging.getLogger('').addHandler(fh)

def flushPortfolioChg():
    #刷新组合收益
    bt=datetime.datetime.now()
    if bt.weekday() in [5]:
        if bt.hour>6:
            return
    if bt.weekday() in [6]:
            return
    hm=bt.strftime("%H%M")
    if hm>"1630" and hm<"2125":
        return
    if hm>"0600" and hm<"0925":
        return
    logging.info("countAllPortfoliosAverageYield bt:%s"%bt)
    portfolioMode.cportfolioMode().countAllPortfoliosAverageYield()
    et = datetime.datetime.now()
    logging.info("countAllPortfoliosAverageYield et:%s,spend(%s)s"%(et,(et-bt).total_seconds()))
    pass
def synHkMarket():
    bt=datetime.datetime.now()
    logging.info("synHkMarket bt:%s"%bt)
    east_hk_mk.synHkMarket(stocktype='stocks')
    # east_hk_mk.synHkMarket(stocktype='idx')
    strategyMode().sg_HK_StockPriceVolatility()#筛选股票
    et = datetime.datetime.now()
    logging.info("synHkMarket et:%s,spend(%s)s"%(et,(et-bt).total_seconds()))
    pass
def synCHMarket():
    bt=datetime.datetime.now()
    logging.info("synCHMarket bt:%s"%bt)
    east_ch_mk.synCHMarket(stocktype='stocks')
    et = datetime.datetime.now()
    logging.info("synCHMarket et:%s,spend(%s)s"%(et,(et-bt).total_seconds()))
    pass
def synAMMarket():
    bt=datetime.datetime.now()
    logging.info("synAMMarket bt:%s"%bt)
    east_us_mk.synusMarket(stocktype='stocks')#更新行情
    strategyMode().sg_US_StockPriceVolatility()#筛选股票
    et = datetime.datetime.now()
    logging.info("synAMMarket et:%s,spend(%s)s"%(et,(et-bt).total_seconds()))
    pass
def news_repeat_nodisplay():
    bt=datetime.datetime.now()
    logging.info("news_repeat_nodisplay bt:%s"%bt)
    tmode = taskMode()
    tmode.news_repeat(preDays=10)#处理futu新闻重复数据
    tmode.research_repeat(preDays=10)
    et = datetime.datetime.now()
    logging.info("news_repeat_nodisplay et:%s,spend(%s)s"%(et,(et-bt).total_seconds()))

if __name__ == '__main__':
    # realCountNav()
    hostName = socket.gethostname()
    # scheduler.add_job(spiderResearch, 'cron', hour=12, minute=35 , id='spiderResearch1')
    # scheduler.add_job(spiderResearch, 'cron', hour=17, minute=5 , id='spiderResearch2')
    scheduler.add_job(flushPortfolioChg, 'interval', seconds=120, id='flushPortfolioChg')
    scheduler.add_job(news_repeat_nodisplay, 'interval', seconds=60, id='news_repeat_nodisplay')
    scheduler.add_job(synCHMarket, 'cron', hour=15, minute=5, id='synCHMarket1')
    scheduler.add_job(synCHMarket, 'cron', hour=16, minute=10, id='synCHMarket2')

    scheduler.add_job(synHkMarket, 'cron', hour=16, minute=5, id='synHkMarket1')
    scheduler.add_job(synHkMarket, 'cron', hour=17, minute=30, id='synHkMarket2')

    scheduler.add_job(synAMMarket, 'cron', hour=5, minute=10, id='synAMMarket1')
    scheduler.add_job(synAMMarket, 'cron', hour=7, minute=55, id='synAMMarket2')
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(1000000)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
