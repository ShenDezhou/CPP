#!/bin/bash
#coding=gb2312

# 接入点评，，猫眼，微信三家的影院数据

# 保留之前的merge信息

# 三家商业合作的影院放映信息
CooperationPath="/search/zhangk/Fuwu/Source/Cooperation"
MapyanPath="$CooperationPath/Maoyan"
WepiaoPath="$CooperationPath/Wepiao"
DianpingPath="$CooperationPath/Dianping"
KouPath="$CooperationPath/KouMovie"
NuomiPath="$CooperationPath/Nuomi"


# 服务项目影院ID 与 第三方商业合作的影院ID 的映射关系
CINEMA_ID_CONF="/search/zhangk/Fuwu/Source/conf/cinema_vrid_id_conf"
BackupPath="/search/zhangk/Fuwu/Source/history"


# 时光网电影详情信息; 影院的细节信息
MtimeActors="/search/zhangk/Fuwu/Source/Input/movie_movie_actors.table.id"
MtimeVideos="/search/zhangk/Fuwu/Source/Input/movie_movie_videos.table.id"
MtimeComments="/search/zhangk/Fuwu/Source/Input/movie_movie_comments.table.id"
MtimeMovieDetail="/search/zhangk/Fuwu/Source/Input/movie_movie_detail.table.id"

MtimeBakActors="/fuwu/Merger/history/movie/movie_actors.table"
MtimeBakVideos="/fuwu/Merger/history/movie/movie_videos.table"
MtimeBakComments="/fuwu/Merger/history/movie/movie_comments.table"
MtimeBakMovieDetail="/fuwu/Merger/history/movie/movie_detail.table"


MtimeCinemaInfo="conf/mtime_cinema_info"



# 抓取的大众点评影院的一些基本信息和团购信息
DianpingCinemaInfo="/search/zhangk/Fuwu/Spider/Dianping/data/cinema_info"


# 第三方影片ID 与 innerid 的映射
movie_id_merge=tmp/movie_id_merge

# 第三方影院/电影数据的合集
CoopMovie=tmp/cooperation_movie
CoopCinema=tmp/cooperation_cinema
CoopMoviePresale=tmp/cooperation_movie_presale
CoopCMRelation=tmp/cooperation_cm_relation
CoopMovieDetail=tmp/cooperation_movie_detail

. ./bin/Tool.sh


# 抽取时光网电影的数据
function extract_mtime_movie_idtitle() {
	input=$1;  output=$2
	if [ ! -f $input ]; then
		LOG "[Error]: extract mtime movie idtitle info, [$input] not exist"
		return -1
	fi
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
	}' $input > $output
	LOG "extract mtime movie id from [$input] done. [$output]"
}




# 合并时光网与第三方电影
function merge_movie_id_imp() {
	mtimeMovieID=$1;  vrMovieID=$2;  output=$3;
	awk -F'\t' ' BEGIN {
		movieid=1;	
	}
	# 归一化电影的名称
	function normalTitle(title) {
		gsub(/[：\.，？……:,\-]/, "", title)
		gsub(/;.*$/, "", title)
		gsub(/[\s ]/, "", title)
		return tolower(title)
	}
	# 加载时光网的电影名称/ID数据
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
		} else {
			mtimeid = "need_" ++movieid
			titleid[title] = mtimeid
		}
		print mtimeid "\t" $0
	}' $mtimeMovieID $vrMovieID > $output
	LOG "merge hot movie id done"
}


# 合并时光网与上映的老电影的映射关系
function merge_old_movie_id_imp() {
	mtimeMovieID=$1;  output=$2;
	awk -F'\t' ' BEGIN {
		movieid=1;	
	}
	# 归一化电影的名称
	function normalTitle(title) {
		gsub(/[：，？:,]/, "", title)
		gsub(/;.*$/, "", title)
		gsub(/[\s ]/, "", title)
		return tolower(title)
	}
	# 加载时光网的电影名称/ID数据
	ARGIND ==1 {
		id=$1; title=$2;
		title = normalTitle(title)
		titleid[title] = id
	} ARGIND == 2 {
		#need_2  大众点评        577     锦衣卫  0       李仁港
		source=$2;  id=$3;  title=$4;
		if ($1 !~ /need_/) {
			print; next
		}
		mtimeid = ""
		title = normalTitle(title)
		if (title in titleid) {
			mtimeid = titleid[title]
		} else {
			mtimeid = source "_" id
			titleid[title] = mtimeid
		}
		# 去除抠电影(名称总与其他几家不一致)
		if (source~/Kou/ || source~/抠电影/) {
			next
		}

		line = mtimeid
		for (i=2; i<=NF; ++i) {
			line = line "\t" $i
		}
		print line
	}' $mtimeMovieID $output > $output.add

	rm -f $output.bak;  mv $output $output.bak
	cp $output.add $output

	LOG "merge old movie id done"
}





# 计算时光网电影 与 第三方电影 ID的映射关系
function merge_movie_id() {
	# 抽取时光网电影的<id  title>映射关系
	mtime_movie_id="tmp/mtime_movie_id"
	extract_mtime_movie_idtitle $MtimeMovieDetail $mtime_movie_id
	# 合并热映，新电影
	merge_movie_id_imp $mtime_movie_id $CoopMovie $movie_id_merge
	

	# 合并正在上映的老电影
	# 抽取时光网老电影的<id  title>映射关系
	mtime_old_movie_id="tmp/mtime_old_movie_id"
	extract_mtime_movie_idtitle $MtimeBakMovieDetail $mtime_old_movie_id
	# 合并在映的老电影
	merge_old_movie_id_imp $mtime_old_movie_id $movie_id_merge

	# 抠电影错误百出，还是过滤掉吧
	awk -F'\t' '{ if($1~/(Kou|抠电影)/){ next } print}' $movie_id_merge > $movie_id_merge.filter
	rm -f $movie_id_merge;  mv $movie_id_merge.filter $movie_id_merge

	# 添加人工处理的
	cat conf/movie_id_merge_manual >> $movie_id_merge

	LOG "merge movie id done. [$movie_id_merge]"
}


# 下载没有合并成功的电影
function download_merge_failed_movies() {
	sh bin/get_cooperation_nomerged_movies.sh > /fuwu/Spider/Mtime/tmp/movie_titles

	LOG "begin to merge failed movies done......."
	cd /fuwu/Spider/Mtime
		python  bin/getExtraMtimeMovie.py &
	cd -
	LOG "get merge failed movies done."
}


# 给第三方影院分配ID
# 这里需要考虑人工处理的情况
function update_cinema_id() {
	cinema_merge_id=$1

	awk -F'\t' 'BEGIN {
		cinemaMaxID = 0;
	} {
		if (NF != 5) { next }
		leftSrc=$2; leftID=$3; rightSrc=$4; rightID=$5;
		leftKey = leftSrc "\t" leftID;  rightKey = rightSrc "\t" rightID;
		
		if (leftKey in cinemaIDMap && rightKey in cinemaIDMap && cinemaIDMap[leftKey] != cinemaIDMap[rightKey]) {
			leftOldValue = cinemaIDMap[leftKey];  rightOldValue = cinemaIDMap[rightKey]
			++cinemaMaxID
			for ( key in cinemaIDMap) {
				if (cinemaIDMap[key] == leftOldValue || cinemaIDMap[key] == rightOldValue) {
					cinemaIDMap[key] = cinemaMaxID
				}
			}
		} else if (!(leftKey in cinemaIDMap) && !(rightKey in cinemaIDMap)) {
			++cinemaMaxID
			cinemaIDMap[leftKey] = cinemaMaxID
			cinemaIDMap[rightKey] = cinemaMaxID
		} else if (leftKey in cinemaIDMap) {
			cinemaIDMap[rightKey] = cinemaIDMap[leftKey]
		} else if (rightKey in cinemaIDMap) {
			cinemaIDMap[leftKey] = cinemaIDMap[rightKey]
		} 
	} END {
		for (key in cinemaIDMap) {
			print key "\t" cinemaIDMap[key]
		}
	}' $cinema_merge_id > $CINEMA_ID_CONF.update

	# 更新cinema_id_conf文件
	bakFile=$(basename $CINEMA_ID_CONF)
	mv $CINEMA_ID_CONF $BackupPath/conf/$bakFile.$(date +%Y%m%d%H)
	mv $CINEMA_ID_CONF.update $CINEMA_ID_CONF
	LOG "allocate and update $CINEMA_ID_CONF done."
}


# 合并合作方的影院id
function merge_cinema_id() {
	# cinema <1:id, 2:resource, 3:title, 4:brand, 5:address, 6:province, 7:city, 8:area, 9:district, 10:poi>

	# 将时光网与第三方的影院数据合并
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
	}' $MtimeCinemaInfo > $CoopCinema.combine
	cat $CoopCinema >> $CoopCinema.combine

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
	}' $CoopCinema.combine > $CoopCinema.norm

	# 按照地域，名称, poi进行排序后合并
	echo -e "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" > $CoopCinema.title.sort
	echo -e "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" > $CoopCinema.addr.sort
	echo -e "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" > $CoopCinema.poi.sort
	sort -t$'\t' -k7,7 -k3,3 $CoopCinema.norm >> $CoopCinema.title.sort
	sort -t$'\t' -k7,7 -k5,5 $CoopCinema.norm >> $CoopCinema.addr.sort
	sort -t$'\t' -k10,10 $CoopCinema.norm >> $CoopCinema.poi.sort
	
	python bin/ServiceAppCinemaMerge.py -title $CoopCinema.title.sort > tmp/cinema.title.merge
	python bin/ServiceAppCinemaMerge.py -addr $CoopCinema.addr.sort > tmp/cinema.addr.merge
	python bin/ServiceAppCinemaMerge.py -poi $CoopCinema.poi.sort > tmp/cinema.poi.merge
	
	# 专门根据poi的距离 对时光网与商业合作的影院进行合并
	echo -e "id\tresource\ttitle\tbrand\taddress\tprovince\tcity\tarea\tdistrict\tpoi" > $CoopCinema.city.sort
	sort -t$'\t' -k7,7 $CoopCinema.norm >> $CoopCinema.city.sort
	python bin/ServiceAppCinemaMerge.py -distance $CoopCinema.city.sort > tmp/cinema.distance.merge
	

	# 给合并后的第三方影院ID分配统一的ID
	cat tmp/cinema.title.merge tmp/cinema.addr.merge tmp/cinema.poi.merge tmp/cinema.distance.merge | awk -F'\t' 'NF==5{print}' > tmp/cinema.id.merge
	# 没有合并的影院也附到后面，用于分配新的ID
	awk -F'\t' '{
		id=$1; source=$2
		print "null\t" source "\t" id "\t" source "\t" id
	}' $CoopCinema >> tmp/cinema.id.merge


	# 为这些影院重新分配ID
	#update_cinema_id tmp/cinema.id.merge


	# 合作方的影院合并使用人工审核的结果
	# 见Cooperation/bin/build_cinema_merge.sh 
	# 时光网的影院单独处理
	
	cinema_merge_manual="/fuwu/Source/Cooperation/Merge/cinema_merge.uniq"
	
	cat $cinema_merge_manual > $CINEMA_ID_CONF
	fgrep "时光网" tmp/cinema.id.merge > tmp/mtime.cinema.id 	
	awk -F'\t' 'ARGIND==1 {
		gsub("猫眼电影", "Maoyan", $1)
		gsub("大众点评", "Dianping", $1)
		gsub("微信电影票", "Wepiao", $1)
		gsub("抠电影", "Kou", $1)

		cinemaKey = $1 "\t" $2;  cinemaid = $3;
		cinemaID[cinemaKey] = cinemaid
	} ARGIND == 2 {
		cinemaLeft = $2 "\t" $3;  cinemaRight = $4 "\t" $5
		if ($2 == "时光网" && cinemaRight in cinemaID) {
			print cinemaLeft "\t" cinemaID[cinemaRight]
		} else if ($4 == "时光网" && cinemaLeft in cinemaID) {
			print cinemaRight "\t" cinemaID[cinemaLeft]
		}
	}' $cinema_merge_manual tmp/mtime.cinema.id >> $CINEMA_ID_CONF

	sed -i 's/猫眼电影/Maoyan/g' $CINEMA_ID_CONF
	sed -i 's/大众点评/Dianping/g' $CINEMA_ID_CONF
	sed -i 's/微信电影票/Wepiao/g' $CINEMA_ID_CONF
	sed -i 's/抠电影/Kou/g' $CINEMA_ID_CONF

	LOG "merge cooperation cinema id done. [$CINEMA_ID_CONF]"
}




# 拷贝第三方影院/电影数据到本地
function copy_ciname_movie_data() {
	# 拷贝之前应该添加一个检测，检测是否是当前的最新数据

	
	#cinema <1:id, 2:resource, 3:title, 4:brand, 5:address, 6:province, 7:city, 8:area, 9:district, 10:poi>
	# 拷贝第三方电影信息
	cat $MapyanPath/data/*_movie_movie $DianpingPath/data/*_movie_movie $WepiaoPath/data/*_movie_movie $KouPath/data/*_movie_movie $NuomiPath/data/*_movie_movie > $CoopMovie

	# 拷贝第三方影院信息
	cat $CooperationPath/*/data/*_movie_cinema > $CoopCinema

	# 拷贝第三方排片信息 并且过滤 start end 为空的行, 过滤掉老旧数据（由于第三方没有及时更新）
	today=$(today)
	cat $CooperationPath/*/data/*_movie_cm_relation | awk -F'\t' -v TODAY=$today '$6!="" && $7!="" && $4>=TODAY{print}' > $CoopCMRelation
	

	# 拷贝第三方影院的预售信息
	cat $CooperationPath/*/data/*_movie_presale | awk '$NF!=0{print}' > $CoopMoviePresale


	# 拷贝第三方电影详情数据
	cat $CooperationPath/*/data/*_movie_detail > $CoopMovieDetail


	LOG "copy cooperation cinema/movie data done."
}


# 计算影院电影的关系信息数据
function build_cinema_movie_relation() {
	# tmp/movie_id_merge 电影ID的映射关系 <0:innerid  1:source  2:id  3:title  4:presale   5:director>
	# $CINEMA_ID_CONF 影院ID的映射关系  <1:source  2:id  3:inner>

	# $CoopMovie <1:source  2:id  3:title  4:presale   5:director>
	# $CoopCinema <1:id, 2:source, 3:title, 4:brand, 5:address, 6:province, 7:city, 8:area, 9:district, 10:poi>
	# $CoopCMRelation <1:cinemaid 2:movieid 3:resource 4:date 5:week 6:start 7:end 8:price 9:room 10:seat>



	# 影院表 <影院ID  影院title  省份 城市 地址 等信息>
	# 根据时光网中的数据添加 tel hasimx businesstime
	cinema_table="Input/movie_cinema_detail.table.id"
	awk -F'\t' 'BEGIN {
		idRow=-1; cityRow=-1; telRow=-1; imaxRow=-1; btimeRow=-1; scoreRow=-1;
		print "id\tsource\ttitle\tbrand\taddress\tprovince\tcity\tdistrict\tarea\tpoi\ttel\tbusinesstime\thasimax\tscore"
	} ARGIND == 1 {
		if (NF < 3) { next }
		source=$1;  id=$2;  innerid=$3
		cinemaIDMap[source "\t" id] = innerid
		print source "\t" id "\t" innerid > "id.map"
	} ARGIND == 2 {
		source = "时光网"
		# 时光网影院的 <tel  businesstime  hasimax score>
		if (FNR == 1) {
			for(row=1; row<=NF; ++row) {
				if ($row == "cinemaid") { idRow = row; continue }
				if ($row == "city") { cityRow = row; continue }
				if ($row == "tel") { telRow = row; continue }
				if ($row == "hasimax") { imaxRow = row; continue }
				if ($row == "businesstime") { btimeRow = row; continue }
				if ($row == "score") { scoreRow = row; continue }
			} 
		} else {
			id=$idRow; tel=$telRow; hasimax=$imaxRow; businesstime=$btimeRow; score=$scoreRow;
			key = source "\t" id
			if (key in cinemaIDMap) {
				innerid = cinemaIDMap[key]
				mtimeCinemaInfo[innerid] = tel "\t" businesstime "\t" hasimax "\t" score
			}
		}
	} ARGIND == 3 {
		id=$1;  source=$2;
		key = source "\t" id
		if (!(key in cinemaIDMap)) { next }
		innerid = cinemaIDMap[key]
		
		# 合作方的常规数据
		line = innerid
		for (row=2; row<=NF; ++row) {
			line = line "\t" $row
		}
		# 时光网的额外数据(添加了4个字段)
		extraInfo = "\t\t\t"

		if (innerid in mtimeCinemaInfo) {
			extraInfo = mtimeCinemaInfo[innerid]
		}
		print line "\t" extraInfo
	}' $CINEMA_ID_CONF $MtimeCinemaInfo $CoopCinema > $cinema_table #| sort -t$'\t' -k7,7 -k1,1n > $cinema_table
	LOG "create cinema table done. [$cinema_table]"



	# 排片表 <影院ID 电影ID 来源 日期 星期 开始时间  结束时间 包间 价格>
	# 结果按照 <影院id  电影id  日期  开始时间>排序
	cinema_movie_rel_table="Input/movie_cinema_movie_rel.table.id"
	awk -F'\t' 'BEGIN {
		print "id\tcinemaid\tmovieid\tsource\tdate\tweek\tstart\tend\tprice\troom\tlanguage\tdimensional\tseat"
	} ARGIND == 1 {
		# 加载第三方影院之间合并的映射
		if (NF < 3) { next }
		source=$1;  id=$2;  innerid=$3
		cinemaIDMap[source "\t" id] = innerid
	} ARGIND == 2 {
		# 加载几家电影的合并的映射
		if (NF != 6) { next }
		innerid=$1; source=$2;  id=$3
		if (innerid != "") {
			movieIDMap[source "\t" id] = innerid
		}
	} ARGIND == 3 {
		# 根据第三方排片信息，生成最终的排片信息
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
	}' $CINEMA_ID_CONF $movie_id_merge $CoopCMRelation | sort -t$'\t' -k1,1n -k2,2n -k3,3 -k5,5 -k7,7 > $cinema_movie_rel_table
	LOG "create cinema-movie relation table done. [$cinema_movie_rel_table]"





	# 电影-上映影院表 <电影ID  影院ID 城市 poi>
	movie_cinema_rel_table="Input/movie_movie_cinema_rel.table.id"
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
	LOG "create movie-cinema relation table done. [$movie_cinema_rel_table]"



	# 一部电影的在不同地区的预售信息
	movie_presale_table="Input/movie_movie_presale.table.id"
	awk -F'\t' 'BEGIN {
		print "id\tmovieid\tprovince\tcity"
		tuanid = 0;
	} ARGIND == 1 {
		if (NF != 6) { next }
		innerid=$1; source=$2;  id=$3
		if (innerid != "") {
			movieIDMap[source "\t" id] = innerid
		}
	} ARGIND == 2 {
		if (NF != 5) { next }
		id=$1; source=$2;  key = source "\t" id;
		presale=$3; province=$4; city=$5;
		if (province == "") {
			province = city
		}

		if (!(key in movieIDMap)) {
			next
		}
		innerid = movieIDMap[key]
		# innerid \t province \t city 即一个电影在某城市中是否有预售信息
		key = innerid "\t" province "\t" city
		if (!(key in presaleInfo) || presaleInfo[key] < presale) {
			presaleInfo[key] = presale
		} 
	} END {
		lineCnt = 0
		for (key in presaleInfo) {
			print (++lineCnt) "\t" key
		}
	}' $movie_id_merge $CoopMoviePresale | sort -t$'\t' -k1,1n -k2,2 -k3,3 > $movie_presale_table 
	LOG "get movie presale table done. [$movie_presale_table]"


	LOG "build cinema-movie relation tables done."
}



# 召回时光网的数据
function recall_mtime_movie_infos() {
	onlineFile=$1;  mtimeFile=$2;

	#rm -f $onlineFile;  mv $onlineFile.bak $onlineFile

	awk -F'\t' 'BEGIN {
		maxid = 0;
	} ARGIND == 1 {
		# 加载电影ID的映射
		movieids[$1]
	} ARGIND == 2 {
		print
		id=$1;  movieid=$2;
		existMovies[movieid]
		if (FNR != 1) {
			if (id > maxid) { maxid = id }
		}
	} ARGIND == 3 {
		if (FNR == 1) { next }
		movieid=$2;
		if ((movieid in movieids) && !(movieid in existMovies)) {
			line = ++maxid
			for (i=2; i<=NF; ++i) {
				line = line "\t" $i
			}
			print line
		}
	}' $movie_id_merge $onlineFile $mtimeFile > $onlineFile.recall

	rm -f $onlineFile.bak;  mv $onlineFile $onlineFile.bak
	cp $onlineFile.recall $onlineFile
	LOG "recall for [$onlineFile] done."
}



# 从存储的时光网的数据和第三方本身的数据中召回一些老电影
function recall_old_movies() {
	
	#rm -f $MtimeMovieDetail;  mv $MtimeMovieDetail.bak $MtimeMovieDetail

	# 召回一些在映的老电影（从存储的时光网数据 + 第三方数据）
	awk -F'\t' 'ARGIND==1 {
		# 加载电影ID的映射
		movieids[$1]
	} ARGIND == 2 {
		# 属于热映的电影列表
		if (FNR == 1) { print; next }
		if ($1 in movieids) {
			print;  
			existMovie[$1]
		}
	} ARGIND == 3 {
		# 存储的时光网的电影历史记录
		if ($1~/_/) { next }
		if (($1 in movieids) && !($1 in existMovie)) {
			#print "mtime old: " $1
			print; existMovie[$1]
		}
	} ARGIND == 4 {
		# 上映的老电影，但是没在时光网的记录中
		id=$1;  source=$2; title=$3;
		movieid = source "_" id
		if ((movieid in movieids) && !(movieid in existMovie)) {
			if (source~/Kou/ || source~/抠电影/) {
				next
			}
			gsub(/;.*$/, "", title)
			$2="";  $3=title;
			line = movieid;
			for (i=2; i<=NF; ++i) {
				line = line "\t" $i
			}
			print line
			existMovie[movieid] 
		}
	}' $movie_id_merge $MtimeMovieDetail $MtimeBakMovieDetail $CoopMovieDetail > $MtimeMovieDetail.recall
	
	rm -f $MtimeMovieDetail.bak;  mv $MtimeMovieDetail $MtimeMovieDetail.bak
	cp $MtimeMovieDetail.recall $MtimeMovieDetail 
	LOG "recall for $MtimeMovieDetail done."

	# 召回时光网演员表
	recall_mtime_movie_infos $MtimeActors $MtimeBakActors

	# 召回时光网片花
	recall_mtime_movie_infos $MtimeVideos $MtimeBakVideos
	
	# 召回时光网评论
	recall_mtime_movie_infos $MtimeComments $MtimeBakComments
}


# 按道理来说是需要在团购信息完成后，才行
function build_cinema_tuan() {
	cinema_tuan_table="Input/movie_cinema_tuan.table.id"
	# 如果以后有其他团购数据，则先与大众点评合并后再使用
	cat /fuwu/Source/Cooperation/Dianping/tuan/*.tuan > tmp/dianping.tuan

	awk -F'\t' 'BEGIN {
		print "id\tcinemaid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell\tdeadline"
		id = 0
	} ARGIND == 1 {
		if (NF < 3) {
			next
		}
		# 大众点评网的影院ID
		if ($1 ~ /(Dianping|大众点评)/) {
			cinemaid = "dianping_" $2
			cinemaids[cinemaid] = $3
		}
	} ARGIND == 2 {
		# 如果该影院存在团购数据
		if ($2 in cinemaids) {
			line = ++id "\t" cinemaids[$2]
			for (i=3; i<=NF; ++i) {
				line = line "\t" $i
			}
			if (!(line in lines)) {
				lines[line]
				print line
			}
		}
	}' $CINEMA_ID_CONF tmp/dianping.tuan > $cinema_tuan_table
	LOG "create cinema tuan table done. [$cinema_tuan_table]"
}




function main() {
	
	# 拷贝第三方的影院/电影数据拷贝到tmp/下
	 copy_ciname_movie_data

	# 计算时光网与第三方电影的映射 ：tmp/movie_id_merge
	 merge_movie_id

	# 下载合并失败的电影数据
	download_merge_failed_movies

	# 合并合作方的影院ID
	 merge_cinema_id

	# 构建影院与排片电影的关系(使用新ID)
	build_cinema_movie_relation

	# 影院的团购信息
	 build_cinema_tuan

	# 解决第三方当前排的一些老电影的问题, 从备份的时光网数据和第三方中 recall
	recall_old_movies
}

main
