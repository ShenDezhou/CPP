#!/bin/bash
#coding=gb2312

. ./bin/Tool.sh


# /search/zhangk/Fuwu/Source/Crawler/beijing/movie/*

# 猫眼短评 maoyan_shortdesc <title \t desc  url  id>
# 时光网演员表 mtime_actors
#	url
#	directorInfo <chName @@@ enName url photo>
#	actorInfo <chName enName url photo charactorName charactorPhoto>
# 时光网详情  mtime_detail <url title enName photo displaytype year runtime type date score scorecnt wantcnt releasecountry summary rank boxoffice onlinestatus> 
# 时光网图片集 mtime_photos <url  title  photoCnt  photo>
# 时光网短评  mtime_shortdesc  <shortdesc \t url  desc>
# 时光网片花视频 mtime_videos	
#	url
#	title
#	video <title  photo  video  date>



function add_shortdesc() {
	detailFile=$1;  descFile=$2;  output=$1.out;
	awk -F'\t' 'ARGIND == 1{
		if (NF < 2) { next }
		url=$1;  desc=$2;
		if (NF >= 3 && $3~/^[0-9]+$/) {
			wantcnt = $3
			urlWantcnt[url] = wantcnt
		}
		urlDesc[url] = desc
	} ARGIND == 2 {
		print
		key=$1; value=$2;
		if (key == "url") { url = value; next }
		if (key == "title" && url in urlDesc) { 
			print "shortdesc\t" urlDesc[url]
			if (url in urlWantcnt) {
				print "wantcnt\t" urlWantcnt[url]
			}
		}
		
	}' $descFile $detailFile > $output
	rm -f $detailFile
	mv -f $output $detailFile
	LOG "add shortdesc for [$detailFile] done."
}


# 获取猫眼电影与时光网电影的映射
# maoyan_desc_file <title desc  wantcnt  http://m.maoyan.com/movie/246369?_v_=yes    246369>
# mtime_detail <key  value> 
function merge_maoyan_mtime() {
	maoyanFile=$1;  mtimeFile=$2;  output=$3;
	awk -F'\t' 'ARGIND == 1 {
		key=$1; value=$2;
		if (key == "url") { 
			url = value
		} else if (key == "title") { 
			mtimeTitleUrl[value] = url; 
		}
	} ARGIND == 2 {
		if (NF < 5) { next }
		title=$1;  url="http://m.maoyan.com/movie/"$5;
		mtimeUrl = url;
		if (title in mtimeTitleUrl) {
			mtimeUrl = mtimeTitleUrl[title]
		}
		print title "\t" url "\t" mtimeUrl
	}' $mtimeFile $maoyanFile > $output
	LOG "merge [$mtimeFile] and [$maoyanFile] done. output is [$output]."
}


# 合并猫眼与时光网的短评数据
function merge_shortdesc() {
	maoyanDesc=$1;  mtimeDesc=$2;  maoyanMtimeMap=$3;  output=$4;
	# map <title  maoyanUrl  mtimeUrl>
	# 时光网短评  mtime_shortdesc  <shortdesc \t url  desc>
	# 猫眼短评 maoyan_shortdesc <title \t desc wantcnt url  id>
	awk -F'\t' 'ARGIND == 1 {
		if (NF < 3) { next }
		maoyanUrl=$2; mtimeUrl=$3;
		urlMap[maoyanUrl] = mtimeUrl
	} ARGIND == 2 {
		# 时光网的短评数据
		if (NF < 3) { next }
		url=$2; desc=$3;
		if (desc != "") {
			shortdescs[url] = desc
		}
	} ARGIND == 3 {
		# 如果猫眼存在短评，优先选择猫眼的数据
		if (NF < 5) { next }
		desc=$2"\t"$3;  url="http://m.maoyan.com/movie/"$5;
		if (url in urlMap) {
			url = urlMap[url]
		}
		if (desc != "") {
			shortdescs[url] = desc
		}
	} END {
		for (url in shortdescs) {
			print url "\t" shortdescs[url]
		}
	}' $maoyanMtimeMap $mtimeDesc $maoyanDesc > $output
	LOG "merge shortdesc file done. [$output]"
}


# 修改猫眼的评论数据里的url 到 时光网
function replace_url_of_comments() {
	urlMapFile=$1;  commentFile=$2;  output=$3;
	awk -F'\t' 'ARGIND == 1 {
		if (NF != 3) { next }
		mtimeUrl=$3;  maoyanUrl=$2;
		urlMap[maoyanUrl] = mtimeUrl
	} ARGIND == 2 {
		key=$1;  value=$2;
		if (key == "url" && value in urlMap) {
			value = urlMap[value]
		}
		print key "\t" value
	}' $urlMapFile $commentFile > $output
	LOG "replace url for [$commentFile] done. output is [$output]"
}


# 为时光网的演员表文件添加title数据
function add_title_for_actorfile() {
	detailFile=$1;  actorFile=$2;  output=$3;
	awk -F'\t' 'ARGIND == 1 {
		# http://movie.mtime.com/224891/
		key=$1; value=$2;
		if (key == "url") { url = value; next }
		if (key == "title") { urlTitle[url] = value; next }
	} ARGIND == 2 {
		# http://m.mtime.cn/#!/movie/207337/
		key=$1; value=$2;
		# 将无线的URL转成web URL
		if (key == "url") {
			gsub(/.*movie/, "", value)
			url = "http://movie.mtime.com" value
			filterMovie=1
			if (url in urlTitle) {
				filterMovie=0
				print "url\t" url
				print "title\t"urlTitle[url]
			}
		} else {
			if (filterMovie == 0) {
				print
			}
		}
	}' $detailFile $actorFile > $output
	LOG "add title for [$actorFile] done."
}


# 为时光网的短评文件添加title数据
function add_title_for_shortdescfile() {
	detailFile=$1;  shortdescFile=$2;  output=$3;
	awk -F'\t' 'ARGIND == 1{
		key=$1; value=$2;
		if (key == "url") { url = value; next }
		if (key == "title") { urlTitle[url] = value; next }
	} ARGIND == 2 {
		if (NF != 3) { next }
		key=$1; url=$2; desc=$3
		if (key=="shortdesc" && desc!="" && url in urlTitle) {
			print "url\t" url
			print "title\t" urlTitle[url]
			print "shortdesc\t" desc
		}
	}' $detailFile $shortdescFile > $output
	LOG "add title for [$shortdescFile] done."
}


# 为时光网的短评文件添加title数据
function add_photos_for_detailFile() {
	detailFile=$1;  photoFile=$2;  output=$3;
	awk -F'\t' 'ARGIND == 1{
		key=$1; value=$2;

		if (key == "url") { 
			# http://m.mtime.cn/#!/movie/207337/
			# http://movie.mtime.com/207337/
			gsub(/.*movie/, "", value)
			url = "http://movie.mtime.com" value
		}
		if (key == "photoCnt") { urlPhotoCnt[url] = value; }
		if (key == "photo") { urlPhoto[url] = value; }
	} ARGIND == 2 {
		print
		key=$1; value=$2;
		if (key == "url") { url = value; next;}
		if (key == "title") {
			# 剧照数据放入基本信息中
			if (url in urlPhotoCnt) {
				print "photosCnt\t" urlPhotoCnt[url]
				print "photoSet\t" urlPhoto[url]
			}

			# 添加片花链接，剧照集链接
			# http://m.mtime.cn/#!/movie/207337/posters_and_images/
			# http://m.mtime.cn/#!/movie/207337/videos/
			
			gsub(/.*mtime.com\//, "", url)
			photosUrl = "http://m.mtime.cn/#!/movie/" url "posters_and_images/"
			videosUrl = "http://m.mtime.cn/#!/movie/" url "videos/"
			
			print "photosUrl\t" photosUrl
			print "videosUrl\t" videosUrl
		}
	}' $photoFile $detailFile > $output
	LOG "add photos for [$detailFile] done."
}


# 替换完整的summary; 归一化票房
function replace_summary_of_movie() {
	summary=$1;  detail=$2;
	if [ ! -f $summary ]; then
		LOG "[Error]: there are no movie summary file [$summary]"
		return -1
	fi
	iconv -futf8 -tgbk -c $summary > $summary.gbk

	awk -F'\t' 'ARGIND == 1 {
		if (NF != 2) {
			next
		}
		urlSummary[$1] = $2
	} ARGIND == 2 {
		if (NF != 2) {
			next
		}
		key = $1;  value = $2;
		if (key == "url") {
			url = value
		}
		if (key == "summary" && (url in urlSummary)) {
			value = urlSummary[url]
		}
		if (key == "boxoffice") {
			gsub(/[,元]/, "", value)
		}
		print key "\t" value
	}' $summary.gbk $detail > $detail.addsummary
	rm -f $detail
	mv -f $detail.addsummary $detail
	LOG "update summary for [$detail] done."
}





# 大众点评的美食(餐馆)数据处理
function build_mtime_movie() {
	rm -f Input/*movie_*
	rm -f Input/*cinema*
	MTIME_MOVIE_PATH="/search/zhangk/Fuwu/Source/Crawler/beijing/movie"
	
	# 预处理片花数据,无线片花URL转PC片花URL
	awk -F'\t' '{
		key=$1; value=$2;
		if (key == "url") { 
			# http://m.mtime.cn/#!/movie/207337/  ==> http://movie.mtime.com/207337/
			gsub(/.*movie/, "", value)
			value = "http://movie.mtime.com" value
		}
		print key "\t" value
	}' $MTIME_MOVIE_PATH/mtime_videos > Input/movie_movie_videos
	
	# 构建猫眼电影与时光网电影的映射关系
	maoyanDescFile="$MTIME_MOVIE_PATH/maoyan_shortdesc"
	detailFile="$MTIME_MOVIE_PATH/mtime_detail"
	mapFile="tmp/maoyan_mtime_map"
	merge_maoyan_mtime $maoyanDescFile $detailFile $mapFile
	
	# 合并猫眼电影与时光网电影的短评信息
	mtimeDescFile="$MTIME_MOVIE_PATH/mtime_shortdesc"
	mergeDescFile="tmp/shortdesc"
	merge_shortdesc $maoyanDescFile $mtimeDescFile $mapFile $mergeDescFile

	# 替换猫眼的评论数据里的URL
	commentFile="$MTIME_MOVIE_PATH/maoyan_comments"
	output="Input/movie_movie_comments"
	replace_url_of_comments $mapFile $commentFile $output

	# 合并图集到detail文件中
	detailFile="$MTIME_MOVIE_PATH/mtime_detail"
	photosFile="$MTIME_MOVIE_PATH/mtime_photos"
	output="Input/movie_movie_detail"
	add_photos_for_detailFile $detailFile $photosFile $output
	
	# 合并短评数据到detail文件中
	detailFile="Input/movie_movie_detail"
	add_shortdesc $detailFile $mergeDescFile

	# 替换完整summary; 归一化票房字段
	summaryFile="$MTIME_MOVIE_PATH/mtime_summary"
	replace_summary_of_movie $summaryFile $detailFile
	

	# 为演员表文件添加title数据
	actorFile="$MTIME_MOVIE_PATH/mtime_actors"
	output="Input/movie_movie_actors"
	add_title_for_actorfile $detailFile $actorFile $output


	# 去重
	for file in $(ls Input/*movie_*); do
		uniqRestaurant $file
		LOG "unique for [$file] done."
	done

	# 归一化数据
	normConf='conf/mtime_movie_norm_conf'
	for file in $(ls Input/*movie*); do
		python bin/ServiceAppPartition.py -normal $file $normConf
		LOG "normalize for [$file] done."
	done

	# 转成表格式
	for file in $(ls Input/*movie*.norm); do
		python bin/ServiceAppPartition.py -table $file
		LOG "transfer to table format for [$file] done."
	done

	
}


# 预处理如评论数据 1.vs.多的情况
function preHandle() {
	input=$1; 
	awk -F'\t' '{
		key=$1;  val=$2;
		if (key == "url") { url = val; }
		if (key == "title") { title = val; }
		if (key == "userName" || key == "recommFood") {
			print "url\t"url;
			print "title\t"title;
		}
		print
	}' $input > $input.add
	rm -f $input.bak; 
	mv -f $input $input.bak
	mv $input.add $input
	LOG "add url/title info for [$input] done."
}


# 去重
function uniqRestaurant() {
	input=$1; output=$1.uniq
	awk -F'\t' 'BEGIN {
		filteFlag = 0;
		lastItemLines = "";
	}
	function printLastItem(lines){
		if (lines != "") {
			print lines
		}
	}
	{
		key=$1;  val=$2;
		if (key == "url") {
			url = val;
			# 是否需要过滤
			if (filteFlag == 0) {
				printLastItem(lastItemLines)
				lastItemLines = "";
			}
			filteFlag = 0;
			# 若当前url已经存在
			if (url in existUrl) {
				filteFlag = 1;
			}
			existUrl[url] = 1
			lastItemLines = $0
		} else {
			lastItemLines = lastItemLines "\n" $0
		}
	} END {
		if (filteFlag == 0) {
			printLastItem(lastItemLines)
		}
	}' $input > $output
	
	rm -f $input
	mv $output $input
	#rm -f ${input}_raw;
	#mv $input ${input}_raw
	#mv $output $input

	LOG "uniq [$input] done."
}


# 为电影类实体添加ID
# 需要对电影类的 actors, videos文件添加ID
function updateAllMovieID() {
	# 电影的URL-ID 映射关系
	URL_ID_CONF="conf/movie_url_id_conf"
	
	filePrefix=$1

	# 为基本信息表添加ID,即使用时光网的ID作为电影全局ID
	detailFile=${filePrefix}_detail.table
	awk -F'\t' '{
		# 添加ID字段，找到URL所在列
		if (FNR == 1) {
			print "id\t" $0
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					URLROW = row; break;
				}
			}
		} else {
			# 直接使用时光网的ID作为电影全局ID，因为电影的详情信息只来自时光网
			movieurl = $URLROW;  movieid = $URLROW			
			if (movieid == "" || movieid!~/movie.mtime.com/) { 
				next 
			}
			gsub("http://movie.mtime.com/", "", movieid)
			gsub("/", "", movieid)
			print movieid "\t" $0

			urlidMap[movieurl] = movieid
		}
	} END {
		# 更新url-id配置文件
		for (url in urlidMap) {
			print url "\t" urlidMap[url] > "'$URL_ID_CONF'"
		}
	}' $detailFile > $detailFile.id
	LOG "add id for [$detailFile] done. output is [$detailFile.id] "
	

	# 为演员表添加ID，并拆分字段
	actorsFile=${filePrefix}_actors.table
	awk -F'\t' 'BEGIN {
		actorID=0; urlRow=-1; titleRow=-1; actorRow=-1;
	} ARGIND == 1 {
		url = $1;  id = $2;
		urlIDMap[url] = id;	
	} ARGIND == 2 {
		if (FNR == 1) {
			print "actorid\tmovieid\tmurl\tmname\ttype\taname\taenname\taurl\taimg\tcname\tcimg"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					urlRow = row;
				} else if ($row == "title") {
					titleRow = row;
				} else if ($row == "actorInfo") {
					actorRow = row;
				}
			}
		} else {
			movieUrl=$urlRow; movieTitle=$titleRow; actorInfo=$actorRow;
			if (!(movieUrl in urlIDMap) || actorInfo=="") {
				next
			}
			actorLen = split(actorInfo, actorInfoArr, "###")
			for (i=1; i<=actorLen; i++) {
				info = actorInfoArr[i]
				gsub("@@@", "\t", info)
				print (++actorID) "\t" urlIDMap[movieUrl] "\t" movieUrl "\t" movieTitle "\t" info
			}
		}
	}' $URL_ID_CONF $actorsFile > $actorsFile.id
	LOG "update tuanid for [$actorsFile] to [$actorsFile.id] done."



	# 为电影的视频片花添加ID
	videoFile=${filePrefix}_videos.table
	awk -F'\t' 'BEGIN {
		videoID = 0; urlRow = -1; videoRow = -1;
	} ARGIND == 1 {
		url = $1;  id = $2;
		urlIDMap[url] = id;	
	} ARGIND == 2 {
		if (FNR == 1) {
			print "videoid\tmovieid\tvtitle\tvimg\tvurl\tvtime"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					urlRow = row;
				} else if ($row == "video") {
					videoRow = row;
				}
			}
		} else {
			movieUrl = $urlRow; videoInfo = $videoRow;
			if (!(movieUrl in urlIDMap) || videoInfo=="") {
				next
			}
			videoLen = split(videoInfo, videoInfoArr, "###")
			for (i=1; i<=videoLen; i++) {
				info = videoInfoArr[i]
				gsub("@@@", "\t", info)
				print (++videoID) "\t" urlIDMap[movieUrl] "\t" info
			}
		}
	}' $URL_ID_CONF $videoFile > $videoFile.id
	LOG "update tuanid for [$videoFile] to [$videoFile.id] done."


	# 为评论添加ID
	commentFile=${filePrefix}_comments.table
	awk -F'\t' 'BEGIN {
		commentID=0; urlRow=-1; titleRow=-1; commentUrlRow=-1; commentRow=-1;  
	} ARGIND == 1 {
		url = $1;  id = $2;
		urlIDMap[url] = id;	
	} ARGIND == 2 {
		if (FNR == 1) {
			print "commentid\tmovieid\tcommentUrl\tuser\tuimg\tustar\tzan\tcommentDate\tcItemUrl\tcomment"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					urlRow = row;
				} else if ($row == "title") {
					titleRow = row;
				} else if ($row == "comment") {
					commentRow = row;
				} else if ($row == "commentUrl") {
					commentUrlRow = row
				}
			}
		} else {
			movieUrl=$urlRow; movieTitle=$titleRow; commentInfo=$commentRow; commentUrl=$commentUrlRow;
			if (!(movieUrl in urlIDMap) || commentInfo=="") {
				next
			}
			commentLen = split(commentInfo, commentInfoArr, "###")
			for (i=1; i<=commentLen; i++) {
				info = commentInfoArr[i]
				gsub("@@@", "\t", info)
				#print (++commentID) "\t" urlIDMap[movieUrl] "\t" movieUrl "\t" movieTitle "\t" commentUrl "\t" info
				print (++commentID) "\t" urlIDMap[movieUrl] "\t" commentUrl "\t" info
			}
		}
	}' $URL_ID_CONF $commentFile > $commentFile.id
	LOG "update tuanid for [$commentFile] to [$videoFile.id] done."

}


# 更新电影的上映状态
function update_movie_onlinestatus() {
	moviefile=/fuwu/Source/Input/movie_movie_detail.table.id
	# 上线状态和排序文件
	onlinestatus=/search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_online
	movieRank=/search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_rank
	statusConf=/fuwu/Source/conf/mtime_online
	rankConf=/fuwu/Source/conf/mtime_rank
	
	# 判断在线数据是否存在
	if [ ! -f $onlinestatus ]; then
		LOG "[Error]: online status file [$onlinestatus] is not exist."
	else
		lines=$(cat $onlinestatus | wc -l)	
		if [ $lines -lt 30 ]; then
			LOG "[Error]: online status file [$onlinestatus] is too small."
		else
			rm -f $statusConf;  cp $onlinestatus $statusConf
		fi
	fi
	# 判断排序数据是否存在
	if [ ! -f $movieRank ]; then
		LOG "[Error]: movie rank file [$movieRank] is not exist."
	else
		lines=$(cat $movieRank | wc -l)	
		if [ $lines -lt 40 ]; then
			LOG "[Error]: movie rank file [$movieRank] is too small."
		else
			rm -f $rankConf;  cp $movieRank $rankConf
		fi
	fi
	

	awk -F'\t' 'BEGIN {
		urlRow=-1;  onlinestatusRow=-1;
	} ARGIND==1 {
		# 加载上映状态,评分信息信息
		if (NF < 3) {
			next
		}
		url=$1; onlinestatus=$2; score=$3
		if (!(url in urlStatus)) {
			urlStatus[url] = onlinestatus
		}
		if (score > 0 && score <= 10) {
			urlScore[url] = score
		}

	} ARGIND==2 {
		# 加载排序信息
		if (NF < 2) {
			next
		}
		url=$1;  rank=$2;
		if (!(url in urlRank)) {
			urlRank[url] = rank
		}
	} ARGIND==3 {
		# 首行数据，查出url, 上映状态列
		if (FNR == 1) {
			for (row=1; row<=NF; ++row) {
				if ($row == "url") { urlRow = row; }
				if ($row == "onlinestatus") { onlinestatusRow = row; }
				if ($row == "rank") { rankRow = row; }
				if ($row == "score") { scoreRow = row; }
			}
			print
		} else {
			if (urlRow==-1 || onlinestatusRow==-1) {
				print; next;
			}
			url = $urlRow;
			if (urlStatus[url] == "coming") {
				$onlinestatusRow = "即将上映"
			} else {
				$onlinestatusRow = "正在热映"
			}

			if (url in urlRank) {
				$rankRow = urlRank[url]
			} else if ($rankRow != "" && $rankRow < 10) {
				# 如果不在排序文件中，而且排序比较靠前，做处理
				$rankRow += 10
			}
			if (url in urlScore) {
				$scoreRow = urlScore[url]
			}
			# 
			if ($urlRow == "") {
				$rankRow = "";
			}

			# 打印替换后的行
			line = $1
			for (i=2; i<=NF; ++i) {
				line = line "\t" $i
			}
			print line
		}
	}' $statusConf $rankConf $moviefile > $moviefile.status
	
	lines=$(cat $moviefile.status | wc -l)	
	if [ $lines -gt 30 ]; then
		rm -f $moviefile
		cp $moviefile.status $moviefile
	fi
	
	LOG "update online status for [$moviefile] done."
}


# 使用猫眼的评分作为电影最后的评分
function update_movie_score() {
	maoyanMovieDetail=/fuwu/Source/tmp/cooperation_movie_detail 
	movieIDMap=/fuwu/Source/tmp/movie_id_merge
	moviefile=/fuwu/Source/Input/movie_movie_detail.table.id
	awk -F'\t' 'ARGIND==1 {\
		# 加载猫眼与时光网电影id的映射
		if (NF < 3) { next }
		mtimeid=$1; source=$2; id=$3
		if (source == "Maoyan") {
			idMap[id] = mtimeid
		}
	} ARGIND==2 {
		# 加载猫眼的电影评分数据
		if (NF < 17) { next }
		id=$1;  source=$2;  score=$17;
		if (!(score > 0 && score < 10)) {
			next
		}
		if (source == "Maoyan" && (id in idMap)) {
			mtimeid = idMap[id]
			idScore[mtimeid] = score
		}
	} ARGIND == 3 {
		# 替换时光网里的分数
		if (FNR == 1) {
			for(row=1; row<=NF; ++row) {
				if ($row == "score") { scoreRow = row }
				if ($row == "id") { idRow = row }
			}
			print; next
		}
		if ($idRow in idScore) {
			$scoreRow = idScore[$idRow]
		}
		line = $1
		for (i=2; i<=NF; ++i) {
			line = line "\t" $i
		}
		print line
	}' $movieIDMap $maoyanMovieDetail $moviefile > $moviefile.score

	lines=$(cat $moviefile.score | wc -l)	
	if [ $lines -gt 30 ]; then
		rm -f $moviefile
		cp $moviefile.score $moviefile
	fi

	LOG "update movie socre done."
}



# 将处理好的table格式文件分发到不同目录下
# 需要保证输入文件的格式为  city_type_filename  的严格的格式
function dispatch() {
	for srcFile in $(ls Input/movie*.table.id); do
		basename=$(basename $srcFile)
		destFile=$(echo $basename | awk '{gsub(/.id$/, "", $1); sub("_", "/", $1); print "Output/"$1}')
		destPath=$(dirname $destFile)
		if [ ! -d $destPath ]; then
			LOG "[Info]: $destPath is not exist, make it"
			mkdir -p $destPath
		fi
		rm -f $destFile;  cp -f $srcFile $destFile;

		LOG "copy [$srcFile] to [$destFile] done."
	done
	LOG "dispatch done."
}







function main() {
	# 时光网电影数据的基本处理
	 build_mtime_movie

	# 处理人工运营的操作

	# 更新实体ID
	 updateAllMovieID Input/movie_movie
	
	# 合并影院信息
	sh bin/build_cooperation_cinema.sh 

	# 更新电影的在线状态和排名信息
	update_movie_onlinestatus

	# 更新电影的评分（使用猫眼的评分）
	update_movie_score

	# 分发到指定目录下，用于建索引
	 dispatch

}

#main

build_mtime_movie

