"""
Django settings for InvestCopilot project.

Generated by 'django-admin startproject' using Django 4.2.5.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/4.2/ref/settings/
"""

#from pathlib import Path
import socket
import os

# Build paths inside the project like this: BASE_DIR / 'subdir'.
#BASE_DIR = Path(__file__).resolve().parent.parent
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-6k(+jan@s-q!b^mtbb#0%#6-voniw8*m96anpc3yv2jwqwrx7y'

# SECURITY WARNING: don't run with debug turned on in production!
import socket
DEBUG = True
if socket.gethostname() in ['iZebwatodogxv8Z']:
    DEBUG = False

ALLOWED_HOSTS = ['*']

CORS_ALLOW_ALL_ORIGINS = True # If this is used then `CORS_ALLOWED_ORIGINS` will not have any effect
CORS_ALLOW_CREDENTIALS = True
CORS_ORIGIN_WHITELIST = ()
CORS_ALLOW_METHODS = (
	'DELETE',
	'GET',
	'OPTIONS',
	'PATCH',
	'POST',
	'PUT',
	'VIEW',
)
CORS_ALLOW_HEADERS = (
    'authorization',
	'content-type',    
    # 常用就上面两个    
	'XMLHttpRequest',
	'X_FILENAME',
	'accept-encoding',
	'dnt',
	'origin',
    'token',
	'user-agent',
	'x-csrftoken',
	'x-requested-with',
	'Pragma',
)


#CORS_ALLOWED_ORIGINS = [
#    '*',
#] # If this is used, then not need to use `CORS_ALLOW_ALL_ORIGINS = True`
#CORS_ALLOWED_ORIGIN_REGEXES = [
#    'http://localhost:3030',
#]

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'corsheaders',
    'InvestCopilot_App',
    'File_Sync_Check',    
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    #'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
]

ROOT_URLCONF = 'InvestCopilot.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'InvestCopilot_App.models.toolsutils.MenuUtils.initNewMenusList',
                
            ],
        },
    },
]

WSGI_APPLICATION = 'InvestCopilot.wsgi.application'


# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    }
}

SESSION_ENGINE = "django.contrib.sessions.backends.cache"
SESSION_CACHE_ALIAS = "session"
SESSION_COOKIE_AGE = 1209600  # 设置session cookie的过期时间，以秒为单位（这里是两周）

if socket.gethostname() in ['iZebwatodogxv8Z','WIN-BN8GBRE6EOE',"iZtkd54y1pp0z4Z"] :
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis://:daoheRedis6379@127.0.0.1:6379/5",
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                "PICKLE_VERSION": -1,
                "SOCKET_CONNECT_TIMEOUT": 6,
                "SOCKET_TIMEOUT": 6,
            }
        },
        "session": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis://:daoheRedis6379@127.0.0.1:6379/6",
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                "PICKLE_VERSION": -1,
                "SOCKET_CONNECT_TIMEOUT": 6,
                "SOCKET_TIMEOUT": 6,
            }
        }
    }
else:
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis://127.0.0.1:6379/5",
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                "PICKLE_VERSION": -1,
                "SOCKET_CONNECT_TIMEOUT":6,
                "SOCKET_TIMEOUT": 6,
            }
        },
        "session": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis://127.0.0.1:6379/5",
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                "PICKLE_VERSION": -1,
                "SOCKET_CONNECT_TIMEOUT": 6,
                "SOCKET_TIMEOUT": 6,
            }
        }
    }
# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

#LANGUAGE_CODE = 'en-us'


FILE_CHARSET = 'utf-8'

DEFAULT_CHARSET = 'utf-8'

LANGUAGE_CODE = 'zh-Hans'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True




# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = '/static/'

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'




# 自定义日志输出信息
LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s'}  #日志格式
    },
    'filters': {
    },
    'handlers': {
        'default': {
            'level':'INFO',
            #'class':'logging.handlers.RotatingFileHandler',
            'class':'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'filename': BASE_DIR +'/logs/InvestCopilot_log.log',     #日志输出文件,注意这里一定要使用绝对路径
            'maxBytes': 1024*1024*5,                  #文件大小
            'backupCount': 5,                         #备份份数
            'formatter':'standard',                   #使用哪种formatters日志格式
        },
        'error': {
            'level':'ERROR',
            #'class':'logging.handlers.RotatingFileHandler',
            'class':'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'filename': BASE_DIR +'/logs/InvestCopilot_error.log',  #注意这里一定要使用绝对路径，否则在部署时会出问题
            'maxBytes': 1024*1024*5,
            'backupCount': 5,
            'formatter':'standard',
        },
        'debug': {
            'level':'DEBUG',
            #'class':'logging.handlers.RotatingFileHandler',
            'class':'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'filename': BASE_DIR +'/logs/InvestCopilot_debug.log',  #注意这里一定要使用绝对路径，否则在部署时会出问题
            'maxBytes': 1024*1024*5,
            'backupCount': 5,
            'formatter':'standard',
        },
        'console':{
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
    },
    'loggers': {
        'django': {
            'handlers': ['default', 'console'],
            'level': 'INFO',
            'propagate': True  #冒泡：是否将日志信息记录冒泡给其他的日志处理系统，工作中都是True，不然django的日志系统捕捉到日志信息之后，其他模块中的日志信息就捕捉不到了
        },  
        'InvestCopilotLogger': {
            'handlers': ['debug'],
            'level': 'DEBUG',
            'propagate': True
        },        
    }
}


#配置seesion失效时间为30分钟
# SESSION_COOKIE_AGE = 60*30

#配置IRM Iframe
#X_FRAME_OPTIONS='ALLOWALL'

URLLIST = []

PRIVROLELIST = []

SYSCRYPTKEY="YEsZhrYX"

DBTYPE = 'postgresql'

SEND_EMAIL_NOTIFICATION_WHEN_CREATE_USER = False
EMAIL_HOST = "smtp.exmail.qq.com"
EMAIL_SEND_ADDR = "notice@pinnacle-cap.cn"
EMAIL_SEND_PWD = "gCWoruifU5ZUCn6C"
