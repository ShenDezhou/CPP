#!/bin/python
#coding=gb2312

# Creator : liubing@sogou-inc.com
# Date : 2015-12-22

import sys
from keyConf import *



#1. 字数＜50，有赞数且赞数最多的
#2. 字数＜50，有用户等级且等级最高的
#3. 字数＜50，最新一条评论

def get_key_index_map(keyList):
	keyIndexMap = dict()
	for idx in xrange(0, len(keyList)):
		keyIndexMap[keyList[idx]] = idx
	return keyIndexMap


# 只保留一定长度范围的评论
def filte_by_commentLen(commentList, keyIndexMap):
	MinCommentLen, MaxCommentLen = 3, 50
	filteCommentList = []
	for commentSeg in commentList:
		commentLen = len(commentSeg[keyIndexMap[CommentKey]])
		if commentLen >= MinCommentLen and commentLen <= MaxCommentLen:
			filteCommentList.append(commentSeg)
	return filteCommentList

# 保留评分在平均值以上的评论
def filte_by_commentStar(commentList, keyIndexMap):
	if len(commentList) == 0:
		return commentList
	commentStarSum = 0.0
	filteCommentList = []
	for commentSeg in commentList:
		commentStar = commentSeg[keyIndexMap[CommentStarKey]]
		if len(commentStar) == 0:
			commentStar = 0.0
		commentStarSum += float(commentStar)
	commentStarAvg = commentStarSum / float(len(commentList))

	for commentSeg in commentList:
		commentStar = commentSeg[keyIndexMap[CommentStarKey]]
		if int(commentStar) * 0.85 >= commentStarAvg:
			filteCommentList.append(commentSeg)
	return filteCommentList


# 保留某一列数值最大的评论（数值型列）
def filte_by_max_numKey(commentList, keyIndexMap, key):
	MaxNumber = 0.0
	filteCommentList = []
	for commentSeg in commentList:
		number = commentSeg[keyIndexMap[key]]
		if len(number) == 0: number = 0
		if float(number) >= MaxNumber: MaxNumber = float(number)

	# 必须 

	for commentSeg in commentList:
		number = commentSeg[keyIndexMap[key]]
		if len(number) == 0: number = 0
		if float(number) >= MaxNumber:
			filteCommentList.append(commentSeg)
	return filteCommentList


# 用户等级高的
def filte_by_userStar(commentList, keyIndexMap):
	MaxUserStar = 0.0
	filteCommentList = []
	for commentSeg in commentList:
		userStar = commentSeg[keyIndexMap[UserStarKey]]
		if len(userStar) == 0: userStar = 0
		if float(userStar) >= MaxUserStar:
			MaxUserStar = float(userStar)

	for commentSeg in commentList:
		userStar = commentSeg[keyIndexMap[UserStarKey]]
		if len(userStar) == 0: userStar = 0
		if float(userStar) >= MaxUserStar:
			filteCommentList.append(commentSeg)
	return filteCommentList



def get_short_comment_imp(fd, commentList, keyIndexMap):
#1. 字数＜50，有赞数且赞数最多的
#2. 字数＜50，有用户等级且等级最高的
#3. 字数＜50，最新一条评论
	commentList = filte_by_commentLen(commentList, keyIndexMap)
	commentList = filte_by_max_numKey(commentList, keyIndexMap, ZanKey)
	
	#commentList = filte_by_commentStar(commentList, keyIndexMap)
	commentList = filte_by_max_numKey(commentList, keyIndexMap, UserStarKey)
	

	if len(commentList) > 0:
		print_short_comment(fd, commentList[0], keyIndexMap)


def print_short_comment(fd, commentSeg, keyIndexMap):
	#print ('%s\t%s\t%s\t%s\t%s' % (commentSeg[keyIndexMap[UrlKey]], commentSeg[keyIndexMap[CommentKey]], commentSeg[keyIndexMap[UserStarKey]], commentSeg[keyIndexMap[CommentStarKey]], commentSeg[keyIndexMap[ZanKey]])).encode('gb2312', 'ignore')
	#print ('%s\t%s\t%s\t%s\t%s' % (commentSeg[keyIndexMap[UrlKey]], commentSeg[keyIndexMap[UserKey]], commentSeg[keyIndexMap[UserImgKey]], commentSeg[keyIndexMap[CommentStarKey]], commentSeg[keyIndexMap[CommentKey]])).encode('gb2312', 'ignore')
	#print ('%s\t%s' % (commentSeg[keyIndexMap[UrlKey]], commentSeg[keyIndexMap[CommentKey]])).encode('gb2312', 'ignore')
	fd.write(('%s\t%s\n' % (commentSeg[keyIndexMap[UrlKey]], commentSeg[keyIndexMap[CommentKey]])).encode('gb2312', 'ignore'))


def get_short_comment(input, output):
	lastUrl = ''
	sameUrlCommentList = []
	keyIndexMap = get_key_index_map(dianping_review_item_key)
	fd = open(output, 'w+')
	for line in open(input):
		segs = line.strip('\n').decode('utf8', 'ignore').split('\t')
		if len(segs) != len(dianping_review_item_key):
			continue
		curUrl = segs[keyIndexMap[UrlKey]]
		if curUrl != lastUrl:
			get_short_comment_imp(fd, sameUrlCommentList, keyIndexMap)
			sameUrlCommentList = list()
			lastUrl = curUrl
		sameUrlCommentList.append(segs)
	get_short_comment_imp(fd, sameUrlCommentList, keyIndexMap)
	fd.close()

def test_comment(input, url):
	lastUrl = ''
	sameUrlCommentList = []
	keyIndexMap = get_key_index_map(dianping_review_item_key)
	for line in open(input):
		segs = line.strip('\n').decode('utf8', 'ignore').split('\t')
		if len(segs) != len(dianping_review_item_key):
			continue
		curUrl = segs[keyIndexMap[UrlKey]]
		if curUrl == url:
			print_short_comment(segs, keyIndexMap)




if __name__ == '__main__':
	if len(sys.argv) < 2:
		print 'Usage: python %s opt [input] [output]' % sys.argv[0]
		sys.exit(-1)
	opt = sys.argv[1]
	if opt == '-short-comment':
		input, output = sys.argv[2], sys.argv[3]
		get_short_comment(input, output)
	if opt == '-test':
		input, url = sys.argv[2], sys.argv[3]
		test_comment(input, url)
		
