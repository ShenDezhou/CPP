#!/bi/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-06 13:48
# * Filename	 : build_add_dianping_shortreview.sh
# * Description	 : 对于抓取处理完的短评数据 添加到最终baseinfo数据中
# * *****************************************************************************/

. ./bin/Tool.sh

DIANPING_DIR="/fuwu/Merger/Output"

function add_head_for_tuan() {
	type="restaurant"
	for city in $(ls $DIANPING_DIR/); do
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			LOG "movie/other dir, continue."
			continue
		fi

		tuanFile=$DIANPING_DIR/$city/$type/dianping_detail.tuan.table
		output=$tuanFile.addhead
		rm -f $output
		echo -e "tuanid\tresid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell" > $output
		awk -F'\t' '$1!="tuanid"{print}' $tuanFile >> $output
		
		rm -f $tuanFile.bak;  mv $tuanFile $tuanFile.bak;  cp $output $tuanFile

		LOG "add head for $city tuan file done."
	done
}

function change_old_dianping_idmap() {
	type="restaurant"
	for city in $(ls $DIANPING_DIR/); do
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			LOG "movie/other dir, continue."
			continue
		fi

		#if [ "$city" != "zunyi" ]; then
		#	continue
		#fi
		# ll Output/beijing/restaurant/dianping_detail.comment.table.old 1       2       http://www.dianping.com/shop/23089998 

		commentFile=$DIANPING_DIR/$city/$type/dianping_detail.comment.table
		output=$commentFile.changeid
		rm -f $output
		
		awk -F'\t' '{
			if (FNR == 1 || $2~/dianping/) {
				print
			} else {
				restid = $3;
				gsub(/.*\//, "", restid)
				$2 = "dianping_"restid
				line = $1
				for(i=2; i<=NF; ++i) {
					line = line "\t" $i
				}
				print line
			}
		}' $commentFile > $output
		
		rm -f $commentFile.bak;  mv $commentFile $commentFile.bak;  cp $output $commentFile

		LOG "add head for $city tuan file done.[$output]"
	done

}

change_old_dianping_idmap







#CINEMA_MOVIE_INFO=./Input/movie/cinema_movie_rel.table
CINEMA_MOVIE_INFO=Output/movie/cinema_movie_rel.table.bak

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
	echo "uniq done."
	

	# 计算厅名称不一致的
	/usr/bin/python bin/cinemaTingMerger.py $CINEMA_MOVIE_INFO.uniq > tmp/ciname_room_conf
	echo "get ciname_room_conf done."	

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
	echo "map cinema room done."
}

#norm_cinema_room


