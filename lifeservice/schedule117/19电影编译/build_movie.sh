#!/bin/bash

. ./Tool/Shell/Tool.sh

# 每天更新一遍电影的数据(chrome插件抓取的以及时光网的数据)
cd /search/zhangk/Fuwu/Source/Crawler/beijing/movie/
	sh cp.sh
cd -

# 检查电影原始数据是否为空
detailLine=$(cat /search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_detail | wc -l)
actorsLine=$(cat /search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_actors | wc -l)
photosLine=$(cat /search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_photos | wc -l)

if [ $detailLine -lt 20 -o $actorsLine -lt 20 -o $photosLine -lt 20 ]; then
	LOG "movie base data error!"
	sendReportMail "ServiceAppSpideErrorReport" "[Error]: Update Movie Error, Check 10.134.14.117:/fuwu/build_movie.sh"
	exit -1
fi

# 抓票房信息
cd /search/zhangk/Fuwu/Spider/Cbooo
	sh get_cbo_allbox.sh 
cd -


# 影院/电影基本信息
cd Source
	sh bin/build_mtime_movie.sh 1>movie.std 2>movie.err
cd -


# 特殊处理
cd Merger
	# 合并影院的基本信息
	sh bin/build_cinema_movie.sh 1>cinema.std 2>cinema.err

	# 更新票房数据
	sh bin/build_movie_allbox.sh 1>movie_allbox.std 2>movie_allbox.err
cd -


# 发邮件通知更新结果
movieResultDir="/fuwu/Merger/Output/movie/"
movieResultInfo=$(getFileInfoOfDirectory $movieResultDir)
sendReportMail "ServiceAppSpideErrorReport" "[Info]: Update Movie Success! <br> $movieResultInfo"


# 检查一下文件行数
cmRelLine=$(cat /fuwu/Merger/Output/movie/cinema_movie_rel.table | wc -l)
movieLine=$(cat /fuwu/Merger/Output/movie/movie_detail.table | wc -l)

[ $cmRelLine -lt 10000 ] && echo "cinema-movie-relation file is too small" && exit -1
[ $movieLine -lt 80 ] && echo "movie file is too small" && exit -1

LOG "movie base data is done. begin to index it"

ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/build_movie.sh 1>std 2>err &"
