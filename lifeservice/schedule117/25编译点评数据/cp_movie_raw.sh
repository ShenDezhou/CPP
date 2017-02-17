#!/bin/bash
#coding=gb2312

# maoyan_comments  maoyan_shortdesc  mtime_actors  mtime_detail  mtime_photos  mtime_shortdesc  mtime_videos
# task-118: 时光网电影详情数据  对应  mtime_detail
# task-119: 猫眼电影短评  对应  maoyan_shortdesc
# task-120: 猫眼电影评论  对应  maoyan_comments

# task-144: 时光网电影列表,短评信息 对应 mtime_shortdesc
# task-121: 时光网电影演员表  对应 mtime_actors
# task-122: 时光网电影剧照  对应  mtime_photos
# task-123: 时光网电影片花  对应  mtime_videos

. /search/liubing/Tool/Shell/Tool.sh 
#User="movie"
User="system"
ResultPath="/search/liubing/spiderTask/result/$User"


# 去重，保留最新数据,行数比较多的
function uniq_movie_files_() {
	input=$1;  output=$2
	awk -F'\t' 'BEGIN {
		url = ""; lastItem = "";
	}{
		if (NF==2 && $1=="url") {
			if (lastItem != "") {
				if (!(url in itemArray) || lastItemLen >= lastItemLenArray[url]) {
					itemArray[url] = lastItem
					lastItemLenArray[url] = lastItemLen
				}
			}
			url = $2; lastItem=$0; lastItemLen=1
		} else {
			lastItem = lastItem "\n" $0
			lastItemLen += 1
		}
	} END {
		if (!(url in itemArray) || lastItemLen >= lastItemLenArray[url]) {
			itemArray[url] = lastItem
			lastItemLenArray[url] = lastItemLen
		}
		for (url in itemArray) {
			print itemArray[url]
		}
	}' $input > $output
	LOG "uniq $input to $output done."
}

# 去重，保留最新数据,行数比较多的
function uniq_movie_files() {
	input=$1;  output=$2
	awk -F'\t' 'BEGIN {
		url = ""; lastItem = "";
	}{
		if (NF==2 && $1=="url") {
			if (lastItem != "") {
				if (!(url in itemArray) || lastItemLen > 5) {
					itemArray[url] = lastItem
				}
			}
			url = $2; lastItem=$0; lastItemLen=1
		} else {
			lastItem = lastItem "\n" $0
			lastItemLen += 1
		}
	} END {
		if (!(url in itemArray) || lastItemLen > 5) {
			itemArray[url] = lastItem
			lastItemLenArray[url] = lastItemLen
		}
		for (url in itemArray) {
			print itemArray[url]
		}
	}' $input > $output
	LOG "uniq $input to $output done."
}

# 去重
function uniq_file() {
	input=$1;  output=$2
	awk -F'\t' '{
		if (!($0 in lines)) {
			lines[$0]
			print
		}
	}' $input > $output
	LOG "uniq $input to $output done. "
}





# 获取一个路径下当天的最新的结果文件
function get_today_latest_result() {
	inputPath=$1;  output=$2
	today=$(date +%Y-%m-%d)

	latestFileName=$(ls -l $inputPath | tail -n1 | awk '{print $NF}')
	if [ ! -f $inputPath/$latestFileName ]; then
		echo "$inputPath has no file!!!!"
		exit -1
	fi

	latestFileSize=$(ls -l $inputPath/$latestFileName | tail -n1 | awk '{print $5}')
	latestFileDate=$(ls -l --full-time $inputPath/$latestFileName | tail -n1 | awk '{print $6}')

	if [ "$latestFileDate" != "$today" ]; then
		echo "$inputPath/$latestFileName is not created today!!!"
		exit -1
	fi
	
	bakdate=$(date +%Y%m%d)
	mv $output history/$output.$bakdate
	iconv -futf8 -tgbk -c $inputPath/$latestFileName > $output
}


# 获取一个路径下所有最新的结果文件
function get_today_all_result() {
	inputPath=$1;  output=$2
	today=$(date +%Y-%m-%d)

	files=$(ls -l --full-time $inputPath | fgrep "$today" | awk -v PATH=$inputPath '{
		files = files" "PATH"/"$NF} END { print files}')
	echo $files

	bakdate=$(date +%Y%m%d)
	mv $output history/$output.$bakdate
	cat $files | iconv -futf8 -tgbk -c > $output
}


function backup() {
	rm -f mtime_detail.bak;  mv mtime_detail mtime_detail.bak
	rm -f maoyan_shortdesc.bak; mv maoyan_shortdesc maoyan_shortdesc.bak
	rm -f maoyan_comments.bak; mv maoyan_comments maoyan_comments.bak
	rm -f mtime_actors.bak; mv mtime_actors mtime_actors.bak
	rm -f mtime_photos.bak; mv mtime_photos mtime_photos.bak
	rm -f mtime_videos.bak; mv mtime_videos mtime_videos.bak
	rm -f mtime_shortdesc.bak; mv mtime_shortdesc mtime_shortdesc.bak
}



function copy_movie_files()  {
	get_today_all_result $ResultPath/task-118 mtime_detail.raw
	get_today_all_result $ResultPath/task-119 maoyan_shortdesc.raw
	get_today_all_result $ResultPath/task-120 maoyan_comments.raw

	get_today_all_result $ResultPath/task-121 mtime_actors
	get_today_all_result $ResultPath/task-122 mtime_photos
	get_today_all_result $ResultPath/task-123 mtime_videos
	get_today_all_result $ResultPath/task-144 mtime_shortdesc

	# 对电影去重
	uniq_movie_files mtime_detail.raw mtime_detail

	awk -F'\t' 'NF>4{ desc[$5] = $0} END{ for(id in desc) print desc[id]}' maoyan_shortdesc.raw > maoyan_shortdesc

	# 只保留上映电影的评论
	#uniq_movie_files maoyan_comments.raw maoyan_comments
	awk -F'\t' 'ARGIND==1 {
		if (NF < 5) { next }
		movieurl = $4;   gsub("?_v_=yes", "", movieurl);
		hotMovieUrl[movieurl]
	} ARGIND==2 {
		#"http://m.maoyan.com/movie/238607"
		if(NF==2 && $1=="url") {
			if (lastItem != "" && lasturl in hotMovieUrl) {
				print lastItem
			}
			lastItem=$0;  lasturl=$2
		} else {
			lastItem = lastItem "\n" $0
		}
	} END {
		if (lastItem != "" && lasturl in hotMovieUrl) {
			print lastItem
		}
	}' maoyan_shortdesc maoyan_comments.raw > maoyan_comments.filte

	uniq_movie_files maoyan_comments.filte maoyan_comments
}

copy_movie_files
