#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-27 16:02
# * Filename	 : monitor_service_app.py
# * Description	 : 监控服务搜索的数据更新
# * *****************************************************************************/

from PathFileTool import *
from DateTimeTool import *

class Monitor(object):
	''' 监控  '''

	def __init__(self, conf, inEncode='gb2312'):
		self.monitorConf = conf.strip()
		self.inEncode = inEncode
		self.load_monitor_conf()


	# 加载监控配置文件
	def load_monitor_conf(self):
		self.monitorItemDict = dict()
		for line in open(self.monitorConf):
			if line.startswith('#') or len(line.strip()) == 0:
				continue
			segs = line.strip('\n').decode(self.inEncode, 'ignore').split('\t')
			if len(segs) != 5:
				continue
			type = segs[0]
			if type not in self.monitorItemDict:
				self.monitorItemDict[type] = []
			self.monitorItemDict[type].append(segs)


	# 计算监控项的信息
	def monitor_imp(self, monitorItem):
		monitorResult = monitorItem[1:3]
		if len(monitorItem) != 5:
			return None
		filePath, fileRegex = monitorItem[3:]
		# 计算每个监控项的基本信息
		fileSize, fileCtime = '', ''
		latestFile = PathFileTool.get_latest_file(filePath, fileRegex)
		if latestFile:
			fileSize = PathFileTool.get_filesize(latestFile, 'M')
			fileCtime = PathFileTool.get_ctime(latestFile)

		# 生成每个监控项的结果
		monitorResult.append(latestFile)
		monitorResult.append(fileCtime)
		monitorResult.append(fileSize)
		return monitorResult


	# 对所有监控项进行计算
	def monitor(self):
		monitorResultDict = dict()
		for type in self.monitorItemDict:
			monitorResultList = []
			for monitorItem in self.monitorItemDict[type]:
				monitorResult = self.monitor_imp(monitorItem)
				monitorResultList.append(monitorResult)
			monitorResultDict[type] = monitorResultList
		return monitorResultDict



MonitorTitle = u'服务搜索项目监控报告'


def create_td(segs):
	tdLine = ''
	for item in segs:
		tdLine = '%s<td>%s</td>' % (tdLine, item)
	return tdLine


def create_table_head():
	headList = [u'站点', u'描述', u'监控文件', u'文件时间', u'文件大小']
	return '<tr align="center" bgcolor="#808080"><font size="5">%s</font></tr>' % create_td(headList)


def create_monitor_report(monitorResultDict):
	today = DateTimeTool.todayStr()
	report = '<html><title>%s</title><body>' % MonitorTitle
	for type in monitorResultDict:
		report = '%s<p><h2>%s</h2></p>' % (report, type)
		report = '%s<table border=1>%s' % (report, create_table_head())
		# 表格的每行
		for resultItem in monitorResultDict[type]:
			fileCtime = resultItem[3]
			fileSize = resultItem[4]
			tdLine = create_td(resultItem)
			if fileCtime.find(today) != -1 and str(fileSize) != '0':
				report = '%s<tr bgcolor="#00FF00">%s</tr>' % (report, tdLine)
			else:
				report = '%s<tr bgcolor="#FF0000">%s</tr>' % (report, tdLine)
		report = "%s</table></br></br>" % report
	report = '%s</body></html>' % report
	return report




monitor = Monitor('conf/serviceapp_monitor_conf')
monitorResultDict = monitor.monitor()
report = create_monitor_report(monitorResultDict)
print report.encode('gb2312', 'ignore')


