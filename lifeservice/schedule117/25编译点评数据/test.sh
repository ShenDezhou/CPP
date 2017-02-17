#!/bin/bash
#coding=gb2312

# 接入点评，，猫眼，微信三家的影院数据

# 保留之前的merge信息

# 三家商业合作的影院放映信息
COOPERATION_PATH="/search/zhangk/Fuwu/Source/Cooperation"
MAOYAN_PATH="$COOPERATION_PATH/Maoyan"
WEPIAO_PATH="$COOPERATION_PATH/Wepiao"
DIANPING_PATH="$COOPERATION_PATH/Dianping"

# 服务项目影院ID 与 第三方商业合作的影院ID 的映射关系
CINEMA_ID_CONF="/search/zhangk/Fuwu/Source/conf/cinema_vrid_id_conf"
BAKPATH="/search/zhangk/Fuwu/Source/history"

# 时光网电影详情信息; 影院的细节信息
MTIME_MOVIE_DETAIL="/search/zhangk/Fuwu/Source/Input/movie_detail.table.id"
MTIME_CINEMA_INFO="conf/mtime_cinema_info"

# 第三方影片ID 与 innerid 的映射
movie_id_merge="tmp/movie_id_merge"

# 第三方影院/电影数据的合集
cooperation_movie="tmp/cooperation_movie"
cooperation_cinema="tmp/cooperation_cinema"
cooperation_cm_relation="tmp/cooperation_cm_relation"

. ./bin/Tool.sh

function download_cooperation_cinema_info() {
	LOG "begin to download maoyan cinema info..."
	cd $MAOYAN_PATH
	sh build.sh
	cd -
	LOG "download maoyan cinema info done."

	LOG "begin to download dianping cinema info..."
	cd $DIANPING_PATH
	sh build.sh
	cd -
	LOG "download dianping cinema info done."

	LOG "begin to download wepiao cinema info..."
	cd $WEPIAO_PATH
	sh build.sh
	cd -
	LOG "download wepiao cinema info done."
}

# 抽取时光网电影的数据
function extract_mtime_movie_idtitle() {
	output=$1
	awk -F'\t' 'BEGIN {
		idRow=-1; titleRow=-1;
	}{
		if (FNR == 1) {
			for(row=1; row<=NF; row++) {
				if ($row == "id") { idRow = row }
				if ($row == "title") { titleRow = row }
			}
		} else {
			print $idRow "\t" $titleRow
		}
	}' $MTIME_MOVIE_DETAIL > $output
	LOG "extract mtime movie id done. [$output]"
}


# 合并时光网与第三方电影
function merge_movie_id_imp() {
	mtimeMovieID=$1;  vrMovieID=$2;  output=$3;
	awk -F'\t' '
	function normalTitle(title) {
		gsub(/[：，？:,]/, "", title)
		gsub(/;.*$/, "", title)
		return title
	}
	
	ARGIND ==1 {
		id=$1; title=$2;
		title = normalTitle(title)
		titleid[title] = id
	} ARGIND == 2 {
		site=$1;  id=$2;  title=$3;
		mtimeid = ""
		title = normalTitle(title)
		if (title in titleid) {
			mtimeid = titleid[title]
		}
		#if (mtimeid != "") {
			print mtimeid "\t" $0
		#}
	}' $mtimeMovieID $vrMovieID > $output

}

# 合并时光网电影与合作方电影的ID
function merge_movie_id() {
	mtime_movie_id="tmp/mtime_movie_id"

	extract_mtime_movie_idtitle	$mtime_movie_id

	merge_movie_id_imp $mtime_movie_id $cooperation_movie $movie_id_merge
	LOG "merge movie id done. [$movie_id_merge]"
}


# 给第三方影院分配ID
# 这里需要考虑人工处理的情况
function update_cinema_id() {
	cinema_merge_id=$1
	awk -F'\t' 'BEGIN {
		cinemaMaxID = 0;
	} ARGIND == 1 {
		if (NF != 3) { next }
		src=$1; srcid=$2;  cinemaid=$3;
		cinemaIDMap[src "\t" srcid] = cinemaid
		if (cinemaid > cinemaMaxID) {
			cinemaMaxID = cinemaid;
		}
	} ARGIND == 2 {
		if (NF != 5) { next }
		leftSrc=$2; leftID=$3; rightSrc=$4; rightID=$5;
		leftKey = leftSrc "\t" leftID;
		rightKey = rightSrc "\t" rightID;
		# 如果之前没有合并，现在合并了情况，直接人工处理吧
		
		#if ((leftKey in cinemaIDMap) && !(rightKey in cinemaIDMap)) {
		#	cinemaIDMap[rightKey] = cinemaIDMap[leftKey]
		#} else if (!(leftKey in cinemaIDMap) && (rightKey in cinemaIDMap)) {
		#	cinemaIDMap[leftKey] = cinemaIDMap[rightKey]

		if (leftKey in cinemaIDMap) {
			cinemaIDMap[rightKey] = cinemaIDMap[leftKey]
		} else if (rightKey in cinemaIDMap) {
			cinemaIDMap[leftKey] = cinemaIDMap[rightKey]
		} else if (!(leftKey in cinemaIDMap) && !(rightKey in cinemaIDMap)) {
			++cinemaMaxID
			cinemaIDMap[leftKey] = cinemaMaxID
			cinemaIDMap[rightKey] = cinemaMaxID
		} 
	} END {
		for (key in cinemaIDMap) {
			print key "\t" cinemaIDMap[key]
		}
	}' $CINEMA_ID_CONF $cinema_merge_id > $CINEMA_ID_CONF.update

	# 更新cinema_id_conf文件
	bakFile=$(basename $CINEMA_ID_CONF)
	mv $CINEMA_ID_CONF $BAKPATH/conf/$bakFile.$(date +%Y%m%d%H)
	mv $CINEMA_ID_CONF.update $CINEMA_ID_CONF
	LOG "allocate and update $CINEMA_ID_CONF done."

}


# 合并合作方的影院id
function merge_cinema_id() {
	# cinema <1:id, 2:resource, 3:title, 4:brand, 5:address, 6:province, 7:city, 8:area, 9:district, 10:poi>
	
	# 将时光网的影院数据合进来
	# 需要 "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" 	
	awk -F'\t' 'BEGIN {
		brand=""; province=""; area=""; district=""; source="时光网"
	} {
		if (FNR == 1) {
			for(row=1; row<=NF; ++row) {
				if ($row == "cinemaid") { idRow = row; continue }
				if ($row == "title") { titleRow = row; continue }
				if ($row == "city") { cityRow = row; continue }
				if ($row == "addr") { addrRow = row; continue }
				if ($row == "poi") { poiRow = row; continue }
			} 
		} else {
			print $idRow"\t"source"\t"$titleRow"\t"brand"\t"$addrRow"\t"province"\t"$cityRow"\t"area"\t"district"\t"$poiRow
		}
	}' $MTIME_CINEMA_INFO > $cooperation_cinema.combine
	cat $cooperation_cinema >> $cooperation_cinema.combine


	# 归一化title
	awk -F'\t' '
	function normalTitle(title) {
		gsub(/[（）\(\)]/, "", title)
		return title
	}
	{
		title = normalTitle($3)
		$3 = title
		line = $1
		for (i=2; i<=NF; i++) {
			line = line "\t" $i
		}
		print line
	}' $cooperation_cinema.combine > $cooperation_cinema.norm

	# 按照地域，名称, poi进行排序后合并
	echo -e "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" > $cooperation_cinema.title.sort
	echo -e "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" > $cooperation_cinema.addr.sort
	echo -e "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" > $cooperation_cinema.poi.sort
	sort -t$'\t' -k7,7 -k3,3 $cooperation_cinema.norm >> $cooperation_cinema.title.sort
	sort -t$'\t' -k7,7 -k5,5 $cooperation_cinema.norm >> $cooperation_cinema.addr.sort
	sort -t$'\t' -k10,10 $cooperation_cinema.norm >> $cooperation_cinema.poi.sort
	
	python bin/ServiceAppCinemaMerge.py -title $cooperation_cinema.title.sort > tmp/cinema.title.merge
	python bin/ServiceAppCinemaMerge.py -addr $cooperation_cinema.addr.sort > tmp/cinema.addr.merge
	python bin/ServiceAppCinemaMerge.py -poi $cooperation_cinema.poi.sort > tmp/cinema.poi.merge
	
	# 给合并后的第三方影院ID分配统一的ID
	cat tmp/cinema.title.merge tmp/cinema.addr.merge tmp/cinema.poi.merge | awk -F'\t' 'NF==5{print}' > tmp/cinema.id.merge
	# 没有合并的影院也附到后面，用于分配新的ID
	awk -F'\t' '{
		id=$1; source=$2
		print "null\t" source "\t" id "\t" source "\t" id
	}' $cooperation_cinema >> tmp/cinema.id.merge


	# 为这些影院重新分配ID
	#update_cinema_id tmp/cinema.id.merge $cooperation_cinema
	update_cinema_id tmp/cinema.id.merge

	LOG "merge cooperation cinema id done. [$cooperation_cinema]"
}


# 拷贝第三方影院/电影数据到本地
function copy_ciname_movie_data() {
	# 拷贝之前应该添加一个检测，检测是否是当前的最新数据

	
	#cinema <1:id, 2:resource, 3:title, 4:brand, 5:address, 6:province, 7:city, 8:area, 9:district, 10:poi>
	cat $MAOYAN_PATH/data/maoyan_movie_movie $DIANPING_PATH/data/dianping_movie_movie $WEPIAO_PATH/data/wepiao_movie_movie > $cooperation_movie
	
	cat $MAOYAN_PATH/data/maoyan_movie_cinema $DIANPING_PATH/data/dianping_movie_cinema $WEPIAO_PATH/data/wepiao_movie_cinema > $cooperation_cinema

	# 过滤 start end 为空的行
	# $cooperation_cm_relation <1:cinemaid 2:movieid 3:resource 4:date 5:week 6:start 7:end 8:price 9:room 10:seat>
	cat $MAOYAN_PATH/data/maoyan_movie_cm_relation $DIANPING_PATH/data/dianping_movie_cm_relation $WEPIAO_PATH/data/wepiao_movie_cm_relation  | awk -F'\t' '$6!="" && $7!=""{print}' > $cooperation_cm_relation
	LOG "copy cooperation cinema/movie data done."
}



# 合并来自时光网影院数据的 tel hasimax businesstime
function merge_mtime_cinema_info() {
	# 	
	
	


	LOG "merge mtime cinema extra info done."
}



function build_cinema_movie_relation() {
	# tmp/movie_id_merge 电影ID的映射关系 <0:innerid  1:source  2:id  3:title  4:presale   5:director>
	# $CINEMA_ID_CONF 影院ID的映射关系  <1:source  2:id  3:inner>

	# $cooperation_movie <1:source  2:id  3:title  4:presale   5:director>
	# $cooperation_cinema <1:id, 2:source, 3:title, 4:brand, 5:address, 6:province, 7:city, 8:area, 9:district, 10:poi>
	# $cooperation_cm_relation <1:cinemaid 2:movieid 3:resource 4:date 5:week 6:start 7:end 8:price 9:room 10:seat>


	# 影院表 <影院ID  影院title  省份 城市 地址 等信息>
	# 分发是否按照城市进行？影院去重
	cinema_table="Input/cinema_detail.table.id"
	awk -F'\t' 'BEGIN {
		print "id\tsource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi"
	} ARGIND == 1 {
		if (NF != 3) { next }
		source=$1;  id=$2;  innerid=$3
		cinemaIDMap[source "\t" id] = innerid
	} ARGIND == 2 {
		id=$1;  source=$2;
		key = source "\t" id
		if (!(key in cinemaIDMap)) { next }
		line = cinemaIDMap[key]
		for (row=2; row<=NF; ++row) {
			line = line "\t" $row
		}
		print line
	}' $CINEMA_ID_CONF $cooperation_cinema | sort -t$'\t' -k7,7 -k1,1n > $cinema_table
	LOG "create cinema table done. [$cinema_table]"

	# 排片表 <影院ID 电影ID 来源 日期 星期 开始时间  结束时间 包间 价格>
	# 结果按照 <影院id  电影id  日期  开始时间>排序
	cinema_movie_rel_table="Input/cinema_movie_rel.table.id"
	awk -F'\t' 'BEGIN {
		print "id\tcinemaid\tmovieid\tsource\tdate\tweek\tstart\tend\tprice\troom\tseat"
	} ARGIND == 1 {
		if (NF != 3) { next }
		source=$1;  id=$2;  innerid=$3
		cinemaIDMap[source "\t" id] = innerid
	} ARGIND == 2 {
		if (NF != 6) { next }
		innerid=$1; source=$2;  id=$3
		if (innerid != "") {
			movieIDMap[source "\t" id] = innerid
		}
	} ARGIND == 3 {
		cinemaid=$1;  movieid=$2;  source=$3;
		movieKey = source "\t" movieid
		cinemaKey = source "\t" cinemaid
		if (movieKey in movieIDMap && cinemaKey in cinemaIDMap) {
			line = FNR "\t" cinemaIDMap[cinemaKey] "\t" movieIDMap[movieKey]
			for (row =3; row<=NF; ++row) {
				line = line "\t" $row
			}
			print line
		}
	}' $CINEMA_ID_CONF $movie_id_merge $cooperation_cm_relation | sort -t$'\t' -k1,1n -k2,2n -k4,4 -k6,6 > $cinema_movie_rel_table
	LOG "create cinema-movie relation table done. [$cinema_movie_rel_table]"

	# 电影-上映影院表 <电影ID  影院ID 城市 poi>
	movie_cinema_rel_table="Input/movie_cinema_rel.table.id"
	awk -F'\t' 'BEGIN {
		movieidRow=-1; cinemaidRow=-1; cityRow=-1; poiRow=-1; dateRow=-1;
		print "movieid\tcinemaid\tdate\tcity\tpoi"
	} ARGIND == 1 {
		if (FNR == 1) {
			for (row=1; row<=NF; ++row) {
				if ($row == "id") { cinemaidRow=row }
				else if ($row == "city") { cityRow=row }
				else if ($row == "poi") { poiRow=row }
			}
		} else {
			cinemaInfo[$cinemaidRow] = $cityRow "\t" $poiRow
		}
	} ARGIND == 2 {
		if (FNR == 1) {
			for (row=1; row<=NF; ++row) {
				if ($row == "cinemaid") { cinemaidRow=row }
				if ($row == "movieid") { movieidRow=row }
				if ($row == "date") { dateRow=row }
			}
		} else {
			mcItem = $movieidRow "\t" $cinemaidRow "\t" $dateRow
			if (!(mcItem in existItems)) {
				existItems[mcItem] = 1
				if ($cinemaidRow in cinemaInfo) {
					print mcItem "\t" cinemaInfo[$cinemaidRow]
				}
			}
		}
	}' $cinema_table $cinema_movie_rel_table > $movie_cinema_rel_table
	LOG "create movie-cinema relation table done. [$cinema_movie_rel_table]"

	# 电影表：更新是否预购的信息
	# movie_id_merge <1: movieid  2:source 3:id  4:title  5:presale  6:director>
	movie_online="Input/movie_detail.table.id"
	awk -F'\t' 'BEGIN {
		movieidRow = -1
	} ARGIND == 1 {
		if (NF != 6 || $1 =="") { next }
		movieid=$1; presale=$5
		if (!(movieid in moviePresale) || moviePresale[movieid] < presale) {
			moviePresale[movieid] = presale
		}
	} ARGIND == 2 {
		if (FNR == 1) { 
			print $0 "\tpresale";
			for (row=1; row<=NF; ++row) {
				if ($row == "id") { movieidRow = row; break }
			}
		} else {
			presale = ""
			if ($movieidRow in moviePresale) {
				presale = moviePresale[$movieidRow]
			}
			print $0 "\t" presale
		}
	}' $movie_id_merge $movie_online > $movie_online.update
	rm -f $movie_online
	mv $movie_online.update $movie_online
	LOG "update movie info table done. [Input/movie_detail.table.id]"

	LOG "build cinema-movie relation tables done."
}


function main() {
	# 下载合作方排片数据, 这一步可以使用crontab方式进行
	 download_cooperation_cinema_info
	
	# 将上步生成的第三方的影院/电影数据拷贝到tmp/下
	 copy_ciname_movie_data

	# 合并电影ID
	 merge_movie_id

	# 合并合作方的影院ID
	 merge_cinema_id

	#　添加来自时光网影院的数据
	# merge_mtime_cinema_info

	# 构建影院与排片电影的关系(使用新ID)
	build_cinema_movie_relation
}

#main
merge_cinema_id



