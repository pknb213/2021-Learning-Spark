from urllib import request, parse
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import logging
from logging.handlers import TimedRotatingFileHandler
import time

url = 'http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
parms = {
    'ServiceKey': request.unquote('VXsaG%2B4LKw1ZEbhE%2FDpmlJm4RQiHtreZp2ksrBKmQf4quRUpkOU8xd5423ZqgFmAgFnM3%2BG1XsTTrhZHz7lG4g%3D%3D'),
    'numOfRows': '100',
    'pageNo': '1',  # 강서구
    'sidoName': '서울',
    'ver': '1.3',
    '_returnType': 'json'
}

logger = logging.getLogger(__name__)
formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')
logger.setLevel(logging.DEBUG)
fileHandler = TimedRotatingFileHandler(filename='./scheduler.log', when='h', interval=1, encoding='utf-8')
fileHandler.setFormatter(formatter)
fileHandler.suffix = '%Y%m%d'
fileHandler.setLevel(logging.DEBUG)
logger.addHandler(fileHandler)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour='1', id='job_1')
def job_1():
    res = requests.get(
        url="http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty",
        params=parms)

    for i in res.json()['list']:
        # print("\n", i)
        logger.debug(' {}'.format(str(i)))


sched.start()

while 1:
    time.sleep(1)

