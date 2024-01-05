"""dsid_mfactors URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.9/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.urls import path,re_path,include

from InvestCopilot_App.models.stocks  import stockAPI  as stockAPI_views
from InvestCopilot_App.models.portfolio  import portfolioAPI  as portfolioAPI_views
from InvestCopilot_App.models.summary  import viewSummaryAPI  as summary_views
from InvestCopilot_App.models.factor import factorAPI as factorAPI_views
from InvestCopilot_App.models.factor import factorUtilsAPI as factorUtilsAPI_views
from InvestCopilot_App.models.business import likeAPI as likeAPI_views
from InvestCopilot_App.models.query import financialReportAPI as financialReportAPI_views
from InvestCopilot_App.models.query import usStockPriceAPI as usStockPriceAPI_views
from InvestCopilot_App.models.news  import newsAPI  as newsAPI_views
from InvestCopilot_App.models.summary  import audioAPI  as audioAPI_views
from InvestCopilot_App.models.summary  import transAPI  as transAPI_views
from InvestCopilot_App.models.summary  import editSummaryAPI  as editSummaryAPI_views
from InvestCopilot_App.models.summary import  conferenceAPI as conferenceAPI_views
from InvestCopilot_App.models.company  import companyResearchAPI  as companyAPI_views
from InvestCopilot_App.models.business.stockchat import stockChatAPI  as stockChatAPI_views
from InvestCopilot_App.models.business.textss import stockSsAPI  as stockSsAPI_views

urlpatterns = [
    re_path(r'^api/stockApi/', stockAPI_views.stockAPIHandler),
    re_path(r'^api/portfolioApi/', portfolioAPI_views.portfolioAPIHandler),
    re_path(r'^api/summaryApi/', summary_views.viewSummaryAPIHandler),
    re_path(r'^api/transApi/', transAPI_views.transAPIHandler),
    re_path(r'^api/factorApi/', factorAPI_views.factorAPIHandler),
    re_path(r'^api/factorMenuApi/', factorAPI_views.factorMenuAPIHandler),
    re_path(r'^api/factorUtilsApi/', factorUtilsAPI_views.factorUtilsAPIHandler),
    re_path(r'^api/likeApi/', likeAPI_views.likeAPIHandler),
    re_path(r'^api/viewInterResearch/', summary_views.sesearchAPIHandler),
    re_path(r'^api/financialReportApi/', financialReportAPI_views.FinancialReportHandler),
    re_path(r'^api/usStockPriceApi/', usStockPriceAPI_views.usStockPriceHandler),
    re_path(r'^api/newsApi/', newsAPI_views.newsAPIHandler),
    re_path(r'^api/audioApi/', audioAPI_views.audioAPIHandler),
    re_path(r'^api/summaryEditApi/', editSummaryAPI_views.summaryEditAPIHandler),
    re_path(r'^api/companyResearchApi/', companyAPI_views.companyResearchApi),
    re_path(r'^api/conferenceApi/', conferenceAPI_views.conferenceHandler),
    re_path(r'^api/stockChatApi/', stockChatAPI_views.stockChatAPIHandler),
    re_path(r'^api/textStockSelectApi/', stockSsAPI_views.stockSelectApiHandler)
]