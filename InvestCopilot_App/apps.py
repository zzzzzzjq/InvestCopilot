from django.apps import AppConfig


class InvestCopilotAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'InvestCopilot_App'

    def ready(self):
        # 初始化缓存数据
        import InvestCopilot_App.models.cache.cacheDB as cache_db
        cache_db.initCacheData()
