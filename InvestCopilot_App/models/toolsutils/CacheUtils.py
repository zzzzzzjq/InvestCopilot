__author__ = 'Robby'

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()


from InvestCopilot_App.models.toolsutils import LoggerUtils
logger = LoggerUtils.LoggerUtils()

#import os
#os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 缓存失效时间30分钟
CACHEUTILS_CACHE_SECENDS = 30 * 60


class CacheUtils():
    # 是否启用缓存

    # 缓存时长

    # 缓存对象类型

    # 缓存key规则

    def __init__(self, idFix='', cacheSeconds=30 * 60):
        self.idFix = idFix
        self.cacheSeconds = cacheSeconds
        pass

    def cacheIdFix(idFix):
        pass

    def setValue(self, key, value, seconds=(30 * 60)):
        print(self.idFix)
        print(self.cacheSeconds)

        if self.cacheSeconds is None:
            cache.set(key, value, seconds)
        else:
            cache.set(key, value, self.cacheSeconds)

        print(cache.get(key))

    def getValue(self, key):
        return cache.get(key)

    def delKey(self, key):
        cache.delete(key)

if __name__ == '__main__':
    cacheUtis = CacheUtils()
    # dataDF = cacheUtis.getCacheBaseTables('NEWDATA.ANALYSTDATA')
    # print(dataDF)
    # cacheUtis.cacheFactorTables()
    # print(cache.get('NEWDATA.ANALYSTDATA'))
    # print(cache.__dict__)
    # print(dir(cache))
    from django.core.cache import cache
    from django_redis import get_redis_connection

    r = get_redis_connection("default")  # Use the name you have defined for Redis in settings.CACHES
    connection_pool = r.connection_pool
    print("Created connections so far: %d" % connection_pool._created_connections)
    pass