#!/bin/python
#coding=gb2312

# 
foodKeyNames = ['baseinfo', 'recomfood', 'tuan', 'comment']

###################  大众点评的美食商家相关key列表 ###########################

# 基本信息的key
#baseinfoKey = ['url','title','breadcrumb','poi','photo','photoSet','photoCount','commentCount', 'commentSummary', 'avgPrice','score','scoreTaste','scoreCondition','scoreService','address','tel','businessDate','tags','conditionPhotoSet', 'city', 'area', 'district', 'cuisine', 'subcuisine']
# 暂时去除 conditionPhotoSet 字段
baseinfoKey = ['url','title','breadcrumb','poi','photo','photoCount','commentCount', 'commentSummary', 'avgPrice','score','scoreTaste','scoreCondition','scoreService','address','tel','businessDate','tags', 'city', 'area', 'district', 'cuisine', 'subcuisine', 'serviceTuan','serviceDing','serviceWai', 'vipInfo']


baseinfoKey = ['url','title','breadcrumb','poi','photo','photoSet','photoCount','downreduce','commentCount', 'commentSummary', 'avgPrice','score','scoreTaste','scoreCondition','scoreService','address','tel','businessDate','tags', 'city', 'area', 'district', 'cuisine', 'subcuisine', 'serviceTuan','serviceDing','serviceWai', 'vipInfo']

# 推荐菜的key
# 处理后扩展成 [recomfoodid resid   recommUrl   recommName  recommCount recommPhoto recommPrice]
recomfoodKey = ['url','title','recommFood']


# 团购，优惠的key
#tuanKey = ['url','title','serviceTuan','serviceDing','serviceWai','tuanInfo','dingInfo','huoInfo','cuInfo','vipInfo']
tuanKey = ['url','title','tuanInfo']

# 图集数据
photosetKey = ['url', 'title', 'photoSet', 'conditionPhotoSet']

# 评论的key
commentKey = ['url','title','userName','userPhoto','userStar','commentStar','cTasteStar','cConditionStar','cServiceStar','comment','commentPhoto','commentDate','commentZanCnt']



################### 电影类别的相关key列表 ####################################


# 时光网电影基本信息数据  可以添加  片花url  图集url  图集总数
movie_movie_detailKey = ['url','title','enName','photo','year','runtime','type','date','displaytype','onlinestatus','releasecountry','scorecnt','wantcnt', 'shortdesc', 'summary','score','rank','boxoffice', 'photosUrl', 'videosUrl', 'photosCnt', 'photoSet']

# 时光网电影片花
movie_movie_videosKey = ['url', 'title', 'video']

# 时光网电影剧照集
movie_movie_actorsKey = ['url', 'title', 'actorInfo']

# 电影评论相关
movie_movie_commentsKey = ['url', 'title', 'commentUrl', 'comment']
