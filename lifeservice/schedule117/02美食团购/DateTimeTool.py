#!/bin/python
#coding=gb2312
from datetime import datetime
from datetime import timedelta
import time

class DateTimeTool:
	
	def __init__(self):
		pass
	
	@staticmethod
	def today():
		return datetime.today().strftime('%Y%m%d')
	
	@staticmethod
	def todayStr():
		return datetime.today().strftime('%Y-%m-%d')

	@staticmethod
	def now():
		return datetime.today().strftime('%Y-%m-%d %H:%M:%S')

	@staticmethod
	def sec2time(sec):
		return time.strftime('%Y-%m-%d', time.localtime(float(sec)))

	@staticmethod
	def year():
		return datetime.today().strftime('%Y')

	@staticmethod
	def current():
		return int(1000 * time.time())

	@staticmethod
	def second2str(second):
		str = "";
		try:
			min = int(second) / 60
			second = int(second) % 60
			if min != 0:
				str = u"%d分" % min
			str += (u"%d秒" % second)
		except:
			return str
		return str

	@staticmethod
	def nHoursAgoStr(preNHour = 0):
		return (datetime.now() - timedelta(hours=preNHour)).strftime('%Y-%m-%d %H:%M:%S')



if __name__ == '__main__':
	print DateTimeTool().today()
	print DateTimeTool().now()
	print DateTimeTool.sec2time(1450575000)
	print  DateTimeTool().nHoursAgoStr(2)
