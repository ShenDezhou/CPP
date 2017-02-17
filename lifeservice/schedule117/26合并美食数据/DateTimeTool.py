#!/bin/bash
#coding=gb2312
from datetime import datetime


class DateTimeTool:
	
	def __init__(self):
		pass
	
	@staticmethod
	def today():
		return datetime.today().strftime('%Y%m%d')
	
	@staticmethod
	def now():
		return datetime.today().strftime('%Y-%m-%d %H:%M:%S')

