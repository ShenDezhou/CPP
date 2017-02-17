#!/bin/python
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-27 16:16
# * Filename	 : PathPathFileTool.py
# * Description	 : 文件处理工具
# * *****************************************************************************/
import os, time, re
import os.path


#检验给出的路径是否是一个文件：os.path.isfile()
#检验给出的路径是否是一个目录：os.path.isdir()
#检验给出的路径是否真地存:os.path.exists()
#返回一个路径的目录名和文件名:os.path.split()
#返回指定目录下的所有文件和目录名:os.listdir()
#获取路径名：os.path.dirname()
#获取文件名：os.path.basename()
#获取文件属性：os.stat（file）
#获取文件大小：os.path.getsize（filename）
# filemt= time.localtime(os.stat(filename).st_mtime)  
# time.strftime("%Y-%m-%d %H:%M",filemt)   



class PathFileTool(object):

	''' 文件处理工具 '''
	
	def __init__(self):
		pass


	# 获取文件的最后修改时间
	@staticmethod
	def get_mtime(path):
		if not os.path.exists(path):
			return None
		filemt= time.localtime(os.stat(path).st_mtime)
		return time.strftime("%Y-%m-%d %H:%M:%S",filemt) 


	# 获取文件的创建时间
	@staticmethod
	def get_ctime(path):
		if not os.path.exists(path):
			return None
		filemt= time.localtime(os.stat(path).st_ctime)
		return time.strftime("%Y-%m-%d %H:%M:%S",filemt) 
	

	# 获取指定目录下的最新文件
	@staticmethod
	def get_latest_file(path, fileRegex=None):
		latestCtime = ""
		latestFile = None
		if not os.path.exists(path) or os.path.isfile(path):
			return latestFile
		for file in os.listdir(path):
			filePath = '%s/%s' % (os.path.dirname(path), file)
			if os.path.isdir(filePath):
				continue
			if fileRegex and not re.match(fileRegex, file):
				continue
			curCtime = PathFileTool.get_mtime(filePath)
			if curCtime > latestCtime:
				latestCtime = curCtime
				latestFile = filePath
		return latestFile

	
	# 获取指定文件大小
	@staticmethod
	def get_filesize(file, unit='K'):
		if not os.path.exists(file) or os.path.isdir(file):
			return -1
		fileSize = os.path.getsize(file)
		if unit == 'K':
			unitSize = '%.2f' % (float(fileSize) / 1024)
			if unitSize != '0.00':
				return '%sk' % unitSize
		elif unit == 'M' or unit == 'm':
			unitSize = '%.2f' % (float(fileSize) / 1024 / 1024)
			if unitSize != '0.00':
				return '%sm' % unitSize
			return PathFileTool.get_filesize(file)
		elif unit == 'G' or unit == 'g':
			unitSize = '%.2f' % (float(fileSize) / 1024 / 1024 / 1024)
			if unitSize != '0.00':
				return '%sg' % unitSize
			return PathFileTool.get_filesize(file, 'M')
		return fileSize


		


#print PathFileTool.get_mtime('pathTool.py')
#print PathFileTool.get_ctime('pathTool.py')
#print PathFileTool.get_latest_file('/search/zhangk/Fuwu/Source/Crawler/beijing/movie/', 'mtime_summary$')
#print PathFileTool.get_latest_file('/search/liubing/spiderTask/result/system/task-144/', '14')
#print PathFileTool.get_latest_file('/search/liubing/spiderTask/result/system/task-144/')
#print PathFileTool.get_filesize('/search/liubing/Tool/Python/test.py', 'M')
#print PathFileTool.get_filesize('/fuwu/Merger/Output/beijing/restaurant/dianping_detail.comment.table', 'G')
#print PathFileTool.get_filesize('/fuwu/Merger/Output/beijing/restaurant/dianping_detail.comment.table', 'M')
