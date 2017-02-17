#!/bin/bash
#coding=gb2312
from datetime import datetime
import time

class DateTimeTool:
	
	def __init__(self):
		pass
	
	@staticmethod
	def today():
		return datetime.today().strftime('%Y%m%d')
	
	@staticmethod
	def now():
		return datetime.today().strftime('%Y-%m-%d %H:%M:%S')

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


