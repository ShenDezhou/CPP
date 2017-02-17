#!/bin/bash
#coding=gb2312

source ~/.bash_profile

INPUT="./Input"

. ./bin/Tool.sh

# 大众点评的影院信息 <source  id  city  area districe  score>
DIANPING_CINEMA_INFO="/search/zhangk/Fuwu/Spider/Dianping/data/cinema_info"
# 影院的ID映射关系 <score  id  innerid>
CINEMA_ID_CONF="/search/zhangk/Fuwu/Source/conf/cinema_vrid_id_conf"
# 线上影院信息 <id  source  title  brand  address  province  city  district  area  poi  tel  businesstime  hasimax score>
CINEMA_INFO=$INPUT/movie/cinema_detail.table
# 电影-上映影院映射信息
MOVIE_CINEMA_INFO=$INPUT/movie/movie_cinema_rel.table 

# 影院-上映电影映射信息
CINEMA_MOVIE_INFO=$INPUT/movie/cinema_movie_rel.table
# 电影数据信息
MOVIE_INFO=$INPUT/movie/movie_detail.table
# 城市-电影列表信息
CITY_MOVIE_LIST_INFO=$INPUT/movie/city_movie_list.table


# 将不同影院的信息合并起来
# 优先选择猫眼的数据
function merge_cinema_info() {
	awk -F'\t' 'BEGIN {
		areaRow = -1; scoreRow = -1;  districtRow = -1; rowNum = -1;
		print "id\ttitle\tbrand\taddress\tprovince\tcity\tdistrict\tarea\tpoi\ttel\tbusinesstime\thasimax\tscore"
	} function normArea(area) {
		split(area, array, ";")
		return array[1]
	} {
		# 不再使用，区域信息直接使用 猫眼本身的，需要切一下
		# 其他字段的消息，如果猫眼没有，而其他有，则替换
		# 合并三家合作商家的信息
		if (FNR == 1) {
			rowNum = NF;
			for(row=1; row<=NF; ++row) {
				if ($row == "score") { scoreRow = row; continue }
				if ($row == "district") {districtRow = row; continue }
				if ($row == "area") {areaRow = row; continue }
			}
		} else {
			id=$1; source=$2;
			cinemaids[id]
			# 猫眼的 作为主要的数据来源
			if (source ~ /Maoyan/) {
				for (row=3; row<=NF; ++row) {
					key = id "\t" row
					# 猫眼的区域需要切一下
					if (areaRow == row) {
						$row = normArea($row)
					}
					maoyanCinemaInfo[key] = $row
				}
			} else {
				for (row=3; row<=NF; ++row) {
					key = id "\t" row
					if (!(key in cinemaInfo) || $row != "") {
						cinemaInfo[key] = $row
					}
				}
			}
		}
	} END {
		# 输出最后结果
		for (id in cinemaids) {
			line = id;
			for (row=3; row<=rowNum; ++row) {
				key = id "\t" row;  value=""
				if (key in cinemaInfo && cinemaInfo[key] != "") {
					value = cinemaInfo[key]
				}
				if (key in maoyanCinemaInfo && maoyanCinemaInfo[key] != "") {
					value = maoyanCinemaInfo[key]
				}
				if (row==scoreRow && length(value) > 3) {
					value = substr(value, 0, 3)
				}
				line = line "\t" value
			}
			print line
		}
	}' $CINEMA_INFO > $CINEMA_INFO.merge

	LOG "update cinema done. [$CINEMA_INFO.merge]"
}



# 为影院添加别名列
function add_alias_for_cinema() {
	[ ! -f $CINEMA_INFO.merge ] && return -1
	[ ! -f $CINEMA_INFO ] && return -1

	awk -F'\t' 'ARGIND == 1 {
		if (NF < 3) {
			next
		}
		cinemaid=$1; title=$3;
		if (cinemaid in aliasMap) {
			if (index(aliasMap[cinemaid], title) == 0) {
				aliasMap[cinemaid] = aliasMap[cinemaid] ";" title
			}
		} else {
			aliasMap[cinemaid] = title
		}
	} ARGIND == 2 {
		if (FNR == 1) {
			print $0 "\talias";  next
		}
		innerid=$1;  alias=$2
		if (innerid in aliasMap) {
			alias = aliasMap[innerid]
		}
		print $0 "\t" alias
	}' $CINEMA_INFO $CINEMA_INFO.merge > $CINEMA_INFO.alias
	LOG "add alias for $CINEMA_INFO done. [$CINEMA_INFO.alias]"
}





# 找出不是当前热映和即将热映的电影
function filte_invalid_movie() {

	# 即onlinestatus不为空的，如果为空，至少有排片
	awk -F'\t' 'BEGIN {
		onlineRow=-1; movieidRow=-1; dateRow=-1;
	} ARGIND == 1 {
		movieid = $1
		onlineMovie[movieid]
	} ARGIND == 2 {
		if (FNR == 1) {
			for (row=1; row<=NF; ++row) {
				if ($row=="id") {movieidRow = row; continue}
				if ($row=="onlinestatus") {onlineRow = row; continue}
				if ($row=="date") {dateRow = row; continue}
			}
		} else {
			onlineStatus=$onlineRow;  movieid=$movieidRow;
			if (onlineStatus == "" && movieid in onlineMovie) {
				#$onlineRow = "正在上映"
				$onlineRow = "正在热映"
			}
			
			gsub(/[年月]/, "-", $dateRow)
			gsub(/日[ \s]*\(/, "(", $dateRow)
		}


		line = $1;
		for (row=2; row<=NF; ++row) {
			line = line "\t" $row
		}
		print line
	}' $MOVIE_CINEMA_INFO $MOVIE_INFO > $MOVIE_INFO.update
	
	# mv $MOVIE_INFO.update $MOVIE_INFO
	LOG "update movie info done. [$MOVIE_INFO.update]"
}


function restore_cinema_source() {
	[ ! -f $CINEMA_MOVIE_INFO ] && return -1
	awk -F'\t' '{
		if (FNR == 1 || NF < 4) {
			print; next
		}
		
		gsub("Maoyan", "猫眼电影", $4)
		gsub("Dianping", "大众点评", $4)
		gsub("Wepiao", "微信电影票", $4)
		gsub("Kou", "抠电影", $4)
		
		line = $1
		for (idx=2; idx<=NF; ++idx) {
			line = line "\t" $idx
		}
		print line
	}' $CINEMA_MOVIE_INFO > $CINEMA_MOVIE_INFO.upsource

	rm -f $CINEMA_MOVIE_INFO.bak;  mv $CINEMA_MOVIE_INFO $CINEMA_MOVIE_INFO.bak
	cp $CINEMA_MOVIE_INFO.upsource $CINEMA_MOVIE_INFO
	
	LOG "restore cinema source from en to ch"
}


function dispatch() {
	for srcFile in $(ls $INPUT/movie/*.table); do
		destFile=${srcFile/Input/Output}
		destDir=$(dirname $destFile)
		if [ ! -d $destDir ]; then
			mkdir -p $destDir
		fi
		if [ ! -d $destDir/history ]; then
			mkdir -p $destDir/history
		fi
		
		rm -f $destDir/history/$(basename $destFile).$(date "+%Y%m%d")
		mv $destFile $destDir/history/$(basename $destFile).$(date "+%Y%m%d")
		cp $srcFile $destFile
	done

	# dispatch cinema  movie info
	movie_dest_file=${MOVIE_INFO/Input/Output}
	rm -f $movie_dest_file;  cp $MOVIE_INFO.update $movie_dest_file


	cinema_dest_file=${CINEMA_INFO/Input/Output}
	rm -f $cinema_dest_file; 
	if [ -f $CINEMA_INFO.alias ]; then
		cp $CINEMA_INFO.alias $cinema_dest_file
	elif [ -f $CINEMA_INFO.merge ]; then
		cp $CINEMA_INFO.merge $cinema_dest_file
	fi
}


function add_nuomi_movie() {
	NuomiCMRelation=/fuwu/Source/Cooperation/Nuomi/data/nuomi_movie_cm_relation
	MovieIDMap=/fuwu/Source/tmp/movie_id_merge
	CinemaIDMap=/fuwu/Source/conf/cinema_vrid_id_conf

	[ ! -f $NuomiCMRelation -o ! -f $MovieIDMap -o ! -f $CinemaIDMap ] && return -1;
	
	today=$(date +%Y-%m-%d)
	awk -F'\t' -v TODAY=$today 'BEGIN {
		lineNum = 2000000
	} ARGIND==1 {
		# 加载电影ID的映射
		if (NF < 3) {
			next
		}
		movieid=$1;  source=$2;  nuomiMovieid=$3;
		if (source~/糯米/ && movieid!~/糯米/) {
			movieidMap[nuomiMovieid] = movieid
		}
	} ARGIND==2 {
		# 加载影院ID映射
		if (NF < 3) {
			next
		}
		source=$1;  nuomiCinemaid=$2;  cinemaid=$3;
		if (source!~/糯米/ || NF < 5) {
			next
		}
		cinemaidMap[nuomiCinemaid] = cinemaid
	} ARGIND == 3 {
		if (NF < 4) {
			next
		}
		nuomiCinemaid=$1;  nuomiMovieid=$2; date=$4
		if (date < TODAY) {
			next
		}
		if (!(nuomiCinemaid in cinemaidMap) || !(nuomiMovieid in movieidMap)) {
			next
		}
		$1=cinemaidMap[nuomiCinemaid];  $2=movieidMap[nuomiMovieid]
		# 过滤重复的
		key = $1 "" $2 "" $4 "" $6 "" $9
		if (key in existKeys) {
			next
		}
		existKeys[key]		

		line=(++lineNum)
		for(idx=1; idx<=NF; ++idx) {
			line = line "\t" $idx
		}
		print line
	}' $MovieIDMap $CinemaIDMap $NuomiCMRelation >> $CINEMA_MOVIE_INFO
	LOG "add nuomi's cinema_movie relation done."
}


# 处理影院的大厅的问题
function norm_cinema_room() {
	# 去重
	awk -F'\t' '{
		if (FNR == 1) {
			for (i=1; i<=NF; ++i) {
				if ($i == "cinemaid") { cidIdx=i }
				if ($i == "movieid") { midIdx=i }
				if ($i == "source") { sourceIdx=i }
				if ($i == "date") { dateIdx=i }
				if ($i == "start") { startIdx=i }
				if ($i == "room") { roomIdx=i }
			}
			print; next
		}
		if (NF < 10) {
			next
		}
		key = $cidIdx "" $midIdx "" $sourceIdx "" $dateIdx "" $startIdx "" $roomIdx
		if (key in existLines) {
			print > "dddd"
			next
		}
		existLines[key]
		print
	}' $CINEMA_MOVIE_INFO > $CINEMA_MOVIE_INFO.uniq
	LOG "uniq $CINEMA_MOVIE_INFO done."
	

	# 计算厅名称不一致的
	/usr/bin/python bin/cinemaTingMerger.py $CINEMA_MOVIE_INFO.uniq > tmp/ciname_room_conf
	LOG "get ciname_room_conf done."	

	# 映射
	awk -F'\t' 'ARGIND==1 {
		# 加载映射文件
		if (NF != 4) {
			next
		}
		cid=$1; source=$2; sRoom=$3; dRoom=$4
		roomMap[cid "\t" source "\t" sRoom] = dRoom
	} ARGIND==2 {
		# 替换
		if (FNR == 1) {
			for (i=1; i<=NF; ++i) {
				if ($i == "cinemaid") { cidIdx=i }
				if ($i == "source") { sourceIdx=i }
				if ($i == "room") { roomIdx=i }
			}
			print; next
		}
		if (NF < 10) {
			next
		}
		key = $cidIdx "\t" $sourceIdx "\t" $roomIdx
		if (key in roomMap) {
			$roomIdx = roomMap[key]
		}
		line = $1
		for (i=2; i<=NF; ++i) {
			line = line "\t" $i
		}
		print line
	}'  tmp/ciname_room_conf $CINEMA_MOVIE_INFO.uniq > $CINEMA_MOVIE_INFO.map

	rm -f $CINEMA_MOVIE_INFO.bak;  mv $CINEMA_MOVIE_INFO $CINEMA_MOVIE_INFO.bak
	cp $CINEMA_MOVIE_INFO.map $CINEMA_MOVIE_INFO

	LOG "map cinema room done."
}


# 计算 city-movielist 
function get_city_movie_list() {
	awk -F'\t' 'BEGIN {
		print "id\tprovince\tcity\tmovieid"
		lineCnt = 0;
	} ARGIND == 1 {
		province=$6; city = $7;  cinemaid = $1;
		if (province != "") {
			cityprovince[city] = province;
		}
		cinemaCity[cinemaid] = city
	} ARGIND==2 {
		movieids[$1]
	} ARGIND == 3 {
		cinemaid = $2;  movieid = $3;
		if (!(cinemaid in cinemaCity) || !(movieid in movieids)) {
			next
		}
		city = cinemaCity[cinemaid];
		item = cityprovince[city] "\t" city "\t" movieid
		if (item in itemArray) {
			next
		}
		itemArray[item]
		print ++lineCnt "\t" item

	}' $CINEMA_INFO $MOVIE_INFO $CINEMA_MOVIE_INFO > $CITY_MOVIE_LIST_INFO
	LOG "get city movie-list done."
}


# 合作方排片信息中没有与时光网上映列表中的，通过搜索接口下载
function add_merge_failed_movies_info() {
	extraActorFile=/search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_actors.extra
	extraVideoFile=/search/zhangk/Fuwu/Source/Crawler/beijing/movie/mtime_videos.extra
	onlineMovieDetailFile=/fuwu/Merger/Input/movie/movie_detail.table
	onlineMovieActorsFile=/fuwu/Merger/Input/movie/movie_actors.table
	onlineMovieVideosFile=/fuwu/Merger/Input/movie/movie_videos.table

	if [ -f $extraActorFile ]; then
		awk -F'\t' 'ARGIND==1 {
			movieids[$1]
		} ARGIND==2 {
			if ($2 in movieids) {
				print
			}
		}' $onlineMovieDetailFile $extraActorFile >> $onlineMovieActorsFile
	fi

	if [ -f $extraVideoFile ]; then
		awk -F'\t' 'ARGIND==1 {
			movieids[$1]
		} ARGIND==2 {
			if ($2 in movieids) {
				print
			}
		}' $onlineMovieDetailFile $extraVideoFile >> $onlineMovieVideosFile

	fi
	LOG "add merge failed movie's info done."
}


function main() {
	# 合并多家影院的信息
	merge_cinema_info

	# 为影院添加别名列
	add_alias_for_cinema

	#　整理电影的数据
	filte_invalid_movie

	# 创建城市-电影列表
	get_city_movie_list

	# 将排片信息的来源改回中文
	restore_cinema_source

	# 添加糯米网的数据
	norm_cinema_room
	
	# 添加单独抓取的不在时光网放映列表中的额外信息
	add_merge_failed_movies_info

	# 分发数据
	dispatch
}

main

