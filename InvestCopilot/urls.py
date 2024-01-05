"""
URL configuration for InvestCopilot project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path,re_path,include

#favicon.ico
from django.views.generic.base import RedirectView



from InvestCopilot_App import Api_views as Api_views
from InvestCopilot_App import views as Base_views

from InvestCopilot_App.viewHandler.manager import ManagerMenuViewHandler as manager_menu
from InvestCopilot_App.viewHandler.manager import ManagerUserViewHandler as manager_user
from InvestCopilot_App.viewHandler.manager import ManagerCacheViewHandler as manager_cache
from InvestCopilot_App.viewHandler.manager import ManagerCommViewHandler as manager_comm
from InvestCopilot_App.viewHandler.manager import ManagerMenuViewHandler2 as manage_menu1
from InvestCopilot_App.models.summary import summary_urls as summary_urls
from InvestCopilot_App.models import copilot_urls as copilot_urls

handler404 = Base_views.page_not_found
handler500 = Base_views.page_error

#from django.contrib.staticfiles.views import serve
from django.views.static import serve


urlpatterns = [

    re_path(r'^favicon.ico$',RedirectView.as_view(url=r'static/image/favicon.ico')),
    re_path(r'^$', Base_views.home),
    re_path(r'^login/', Base_views.login),
    re_path(r'^api/login/', Base_views.login),
    re_path(r'^logout/', Base_views.logout),
    re_path(r'^api/logout/', Base_views.logout),

    ###############菜单权限管理 开始#####################
    re_path(r'^managemenu.html$', manager_menu.managerMenuPage),  # 菜单权限管理页面
    re_path(r'^api/getAllRoleData2/$', manager_user.getAllRoleData),  # 获取所有的角色信息数据
    re_path(r'^api/amendMenuPriv/$', manager_menu.amendMenuPriv),  # 修改角色菜单权限
    re_path(r'^api/delUserFundRole/$', manager_menu.delUserFundRole),  # 删除用户基金权限
    re_path(r'^api/addMenuPriv/$', manager_menu.addMenuPriv),  # 添加菜单权限
    re_path(r'^api/removeMenuPriv/$', manager_menu.removeMenuPriv),  # 删除菜单权限
    re_path(r'^managerfundrole.html$', manager_menu.managerFundRolePage),  # 基金权限管理页面
    re_path(r'^api/getUserFundRole/$', manager_menu.getUserFundRole),  # 获取用户基金权限
    re_path(r'^api/modifyUserFundRole/$', manager_menu.modifyUserFundRole),  # 修改用户基金权限
    re_path(r'^api/CorrectionMenuPriv/$', manager_menu.CorrectionMenuPriv),  # 校正菜单权限数据
    ###############菜单权限管理 结束#####################

    ###############用户管理  开始########################
    re_path(r'^manageuser.html$', manager_user.managerUserPage),  # 用户管理页面
    re_path(r'^api/getAllUserData/$', manager_user.getAllUserData),  # 获取所有的用户数据
    re_path(r'^api/companyApi/$', manager_user.companyApi),  # 获取所有公司数据
    re_path(r'^api/getAllRoleData/$', manager_user.getAllRoleData),  # 获取所有的角色信息数据
    re_path(r'^api/getUserRole/$', manager_user.getUserRole),  # 用户角色
    re_path(r'^api/addUser/$', manager_user.addUser),  # 添加用户
    re_path(r'^api/addUserBtn/$', manager_user.addUser),  # 添加用户
    re_path(r'^api/restUserPwd/$', manager_user.restUserPwd),  # 重置用户密码
    re_path(r'^api/restPwdBtn/$', manager_user.restUserPwd),  # 重置用户密码
    re_path(r'^api/editeUserRole/$', manager_user.editeUserRole),  # 修改用户权限
    re_path(r'^api/updateUserBtn/$', manager_user.editeUserRole),  # 修改用户权限
    re_path(r'^api/stopUser/$', manager_user.stopUser),  # 停用用户
    re_path(r'^api/startUser/$', manager_user.startUser),  # 启用用户
    re_path(r'^api/flushUserCache/$', manager_user.flushUserCache),  # 刷新用户缓存
    re_path(r'^api/refreshCacheBtn/$', manager_user.flushUserCache),  # 刷新用户缓存
    ###############用户管理  结束########################

    ###############字典管理 开始#####################
    re_path(r'^managerdict.html$', manager_comm.managerCommPage),  # 字典管理页面
    re_path(r'^api/getDictKeyList/$', manager_comm.getDictKeyList),  # 获取字典列表
    re_path(r'^api/getDictValueList/$', manager_comm.getDictValueList),  # 获取字典值列表
    re_path(r'^api/delDictValueByKey/$', manager_comm.delDictValueByKey),  # 删除字典值列表
    # re_path(r'^api/getDictionaryByKey/$', multiset_api.getDictionaryByKey),  # 获取字典值列表 无需登陆
    re_path(r'^api/delDictKey/$', manager_comm.delDictKey),  # 删除字典列表
    re_path(r'^api/addDictKey/$', manager_comm.addDictKey),  # 新增字典列表
    re_path(r'^api/addDictValueByKey/$', manager_comm.addDictValueByKey),  # 新增字典值
    re_path(r'^api/reloadDictData/$', manager_comm.reloadDictData),  # 刷新字典值
    ###############字典管理 结束#####################

    ###############菜单管理 开始#####################
    re_path(r'^api/getAllMenuData/$', manage_menu1.getAllMenuData),
    re_path(r'^api/getCertainMenu/$', manage_menu1.getCertainMenuData),
    re_path(r'^api/addMenu/$', manage_menu1.addMenu),
    re_path(r'^api/deleteMenu/$', manage_menu1.deleteMenu),
    re_path(r'^api/changeMenu/$', manage_menu1.changeMenu),
    ###############菜单管理 结束#####################

    ###################################系统缓存管理(新版) 开始#####################################
    re_path(r'^managecache.html$', manager_cache.managerCachePage),  # 展示缓存管理的页面
    re_path(r'^api/getSysCacheList/$', manager_cache.getSysCacheList),  # 获取缓存列表数据
    re_path(r'^api/flushSysCacheKey/$', manager_cache.flushSysCacheKey),  # 刷新缓存
    re_path(r'^api/searchSysCache/$', manager_cache.searchSysCache),  # 搜索缓存
    ####################################系统缓存管理(新版) 结束####################################

    #表格中文翻译
    re_path(r'^api/dataTableLanguage/', Base_views.getTableLanguage),

    #User Edit Password
    re_path(r'^Changepassword.html', Base_views.UserChangePassword),
    re_path(r'^api/PasswordChange/', Base_views.PasswordChange),
    re_path(r'^api/passwordForgetByEmail/', Base_views.PasswordForgetByEmail),
    re_path(r'^api/PasswordChangeByEmail/', Base_views.PasswordChangeByEmail),

    #欢迎页面
    re_path(r'^welcome.html', Base_views.welcome),

    #path(r'^index.html', Base_views.main),   #首页

    #自选股
    re_path(r'^userportfolio.html', Base_views.userportfolio),  #自选股
    re_path(r'^api/proc_userportfolio/', Api_views.proc_userportfolio),  #自选股相关API

    #内部研报读取 summary_urls中处理
    # re_path(r'^interresearchreport.html', Base_views.interresearchreport),  #获取研报
    # re_path(r'^api/viewInterResearch/', Api_views.viewInterResearch),  #获取研报

    # jsr_add for factors test
    # re_path(r'^api/proc_factors/', Api_views.proc_factors),
    #openAI 爬虫调用 计算摘要 需要用的接口
    re_path(r'^mobileApp/', include(summary_urls)),
    #react 调用接口
    re_path(r'^', include(copilot_urls)),

    #openAI任务管理
    re_path(r'^manageopenaitask.html', Base_views.manageopenaitask),
    re_path(r'^api/procOpenAITaskInfo/', Api_views.procOpenAITaskInfo),
]
