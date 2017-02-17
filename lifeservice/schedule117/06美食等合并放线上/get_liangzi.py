#coding=gb2312
import xml.dom.minidom

from xmlParser import parseService

import sys, logging, time
from DateTimeTool import DateTimeTool as dateTool

today = dateTool.today()
logging.basicConfig(level=logging.DEBUG,
		format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		filename='logs/get_liangzi_service' + today + '.log',
		filemode='a')


xmlFile=sys.argv[1]
parseService(xmlFile, 'liangzi')
