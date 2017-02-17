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
	}' $CINEMA_INFO > $CINEMA_INFO.merge2

	LOG "update cinema done. [$CINEMA_INFO.merge2]"
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

