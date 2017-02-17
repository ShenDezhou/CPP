#!/bin/bash
#coding=gb2312

Log="logs/dianping_tuan.log"

. ./bin/Tool.sh



# 下载大众点评网商户信息xml文件
function download_xmls() {
	rm -f tmp/*
	

	while read cid cname cenname; do
		#echo $cid" # "$cname" # "$cenname

		for ((page=1; page<=3; ++page)); do
			url=${TUAN_LIST_URL/ID/$cid}
			url=${url/PAGE/$page}
			output=tmp/$cname.tuan.$page

			wget $url -O $output
			if [ $? -ne 0 ]; then
				ERROR "Get $url error!" >> $Log
				exit -1
			else
				INFO "Get $url succ!" >> $Log
			fi
			sleep 1
			

		done

	done < $CITY_LIST_CONF
	LOG "Get all xml files done."
}


function download_parse_citylist() {
	output=tmp/dianping_citylist.xml
	wget $CITY_LIST_URL -O $output
	if [ $? -ne 0 ]; then
		ERROR "Get $CITY_LIST_URL error!" >> $Log
		exit -1
	else
		INFO "Get $CITY_LIST_URL succ!" >> $Log
	fi
	iconv -futf8 -tgbk -c $output | awk 'function trim(str) {
		gsub(/(^[ \t\s\r]+|[ \t\s\r]+$)/, "", str)
		return str
	}{
		if ($0~/<id>/) { gsub(/<[^>]+>/, "", $0); id=$0 }
		if ($0~/<name>/) { gsub(/<[^>]+>/, "", $0); name=$0 }
		if ($0~/<enname>/) { gsub(/<[^>]+>/, "", $0); enname=$0;
			print trim(id) "\t" trim(name) "\t" trim(enname)
		}
	}' > $CITY_LIST_CONF
}





#tuanid  resid   site    type    url     title   photo   price   value   sell
#1       dianping_23089998       大众点评        团      http://t.dianping.com/deal/12362159     代金券  http://i2.s1.dpfile.com/pc/mc/c82e021c1f2883cfeffb299eb49aaf3c(160x100)/thumb.jpg  89      100     已售139


#id	title	longtitle  img  url  value  price sell deadline  resid
#2-19099381      代金券1张       仅售38元！价值50元的代金券1张，全场通用，可叠加使用，提供免费WiFi。     http://p0.meituan.net/deal/03dc8395fffaf8e0faa75ffa3f1de9471891221.jpg%40450w_1024h_1e_1c_1l%7Cwatermark%3D1%26%26object%3DL2RwZGVhbC9hMWQ4Yzc5ZjUxZGVlNWZjOGM5MzkwYTBjZDNjODIyZDgxOTIucG5nQDIwUA%3D%3D%26p%3D9%26x%3D20%26y%3D2      http://lite.m.dianping.com/gCJJq6mY9N   50.0    38.0    0       2016-07-11      22900318



# 将团购信息转成线上做索引使用的格式
function format() {
	input=$1;  output=$2;
	awk -F'\t' 'BEGIN {
		tuanid = 1
		print "id\tresid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell\tdeadline"
	}
	function normImage(image) {
		gsub(/\.jpg.*$/, ".jpg", image)
		return image
	}
	{
		if (NF != 10) {
			next
		}
		id=$1;  title=$2;  detail=$3; image=$4;  url=$5;
		value=$6;  price=$7;  sell=$8;  deadline=$9;  resid=$10;
		
		# 过滤无效的团购ID
		if (!(id~/^[\-0-9]+$/)) {
			next
		}
		
		# 去重
		key = id "\t" resid
		if (key in existTuan) {
			next
		}
		existTuan[key]

		site = "大众点评";  type = "团"; 
		image = normImage(image);  resid = "dianping_" resid

		#line = (++tuanid) "\t" resid "\t" site "\t" type "\t" url "\t" title "\t" image
		line = id "\t" resid "\t" site "\t" type "\t" url "\t" title "\t" image
		line = line "\t" price "\t" value "\t" sell "\t" deadline
		print line
	}' $input > $output

}

# 合并团购信息的全量与增量
function merge_tuan_imp() {
	allFile=$1;  addFile=$2;  output=$3;

	today=$(today)

	# 合并，并去除过期数据
	awk -F'\t' -v TODAY=$today 'BEGIN {
		maxid = -1
	} ARGIND == 1 {
		# 团购全集
		if (FNR == 1) {
			print; next
		}
		tuanid=$1;  resid=$2;  url=$5;  deadline=$NF
		if (tuanid > maxid) {
			maxid = tuanid
		}
		# 去除过期团购消息
		if (deadline <= TODAY) {
			next
		}
		tuanItem = resid "\t" url
		allTuanItem[tuanItem]
		print

	} ARGIND == 2 {
		# 新增的团购
		if (FNR == 1) {
			next
		}
		# 过滤已存在的团购
		resid=$2;  url=$5;
		tuanItem = resid "\t" url
		if (tuanItem in allTuanItem) {
			next
		}
		print
		# 重新赋一个团购ID
		#line = ++maxid
		#for (i=2; i<=NF; ++i) {
		#	line = line "\t" $i
		#}
		#print line
	}' $allFile $addFile > $output
}


# 将每天的团购信息，添加到全局中
function merge_tuan() {
	for allTuanFile in $(ls tuan/*.tuan); do
		dailyTuanFile=${allTuanFile/.tuan/.daily}
		if [ ! -f $dailyTuanFile ]; then
			continue
		fi

		# 转成团购的行格式
		format $dailyTuanFile $dailyTuanFile.format
		# 合并全量与增量
		merge_tuan_imp $allTuanFile $dailyTuanFile.format $allTuanFile.update
		# 备份，更新
		backFile="history/$allTuanFile.$(todayStr)"
		rm -f $backFile;   mv $allTuanFile $backFile
		cp -f $allTuanFile.update $allTuanFile

		LOG "handle $allTuanFile done." >> $Log
	done
}

# 抽取某城市餐馆的团购信息
function extract_restaurant_tuan() {
	local baseinfo=$1;  local tuaninfo=$2;  output=$3
	awk -F'\t' 'BEGIN {
		idRow = -1; residRow = -1;
	} ARGIND==1 {
		# 找到店铺id的列
		if(FNR == 1) {
			for(i=1; i<=NF; ++i) {
				if ($i == "id") {
					idRow = i
				}
			}
		} else {
			if (idRow != -1) {
				resids[$idRow] = 1
			}
		}
	} ARGIND==2 {
		if (FNR == 1) {
			for(i=1; i<=NF; ++i) {
				if ($i == "resid") {
					residRow = i
				}
			}
			print; next
		}		
		if (residRow != -1 && $residRow in resids) {
			print
		}
	}' $baseinfo $tuaninfo > $output
	
	LOG "get $output done."
}


# 将美食相关的最新团购信息分发到线上
function dispatch_restaurant_tuan() {
	# 抽取美食类团购信息
	OnlinePath="/fuwu/Merger/Output"  
	for allTuanFile in $(ls tuan/*.tuan); do
		city=${allTuanFile/.tuan/}
		city=${city/tuan\//}
		baseinfo=$OnlinePath/$city/restaurant/dianping_detail.baseinfo.table		
		tuaninfo=$OnlinePath/$city/restaurant/dianping_detail.tuan.table		
		if [ ! -f $baseinfo ]; then
			continue
		fi
		extractTuanFile=tuan/$city.tuan.restaurant
		extract_restaurant_tuan $baseinfo $allTuanFile $extractTuanFile

		line=$(wc -l $extractTuanFile | awk '{print $1}')
		if [ $line -lt 50 ]; then
			LOG "$extractTuanFile is too small" >> $Log
			continue
		fi

		rm -f $tuaninfo.bak;  mv $tuaninfo $tuaninfo.bak
		cp $extractTuanFile $tuaninfo
		awk -F'\t' '{
			# 除去表头和大众点评之外的其他行
			if (FNR == 1 || $3=="大众点评") {
				next
			}
			print
		}' $tuaninfo.bak >> $tuaninfo

		echo "handle $city done."	
	done

	LOG "dispatch $allTuanFile to online done." >> $Log
}


# 将美食相关的最新团购信息分发到线上
function dispatch_dianping_tuan() {
	type=$1
	# 抽取美食类团购信息
	OnlinePath="/fuwu/Merger/Output"  
	for allTuanFile in $(ls tuan/*.tuan); do
		city=${allTuanFile/.tuan/}
		city=${city/tuan\//}
		baseinfo=$OnlinePath/$city/$type/dianping_detail.baseinfo.table		
		tuaninfo=$OnlinePath/$city/$type/dianping_detail.tuan.table		
		if [ ! -f $baseinfo ]; then
			continue
		fi
		extractTuanFile=tuan/$city.tuan.$type
		extract_restaurant_tuan $baseinfo $allTuanFile $extractTuanFile

		line=$(wc -l $extractTuanFile | awk '{print $1}')
		if [ $line -lt 50 ]; then
			LOG "$extractTuanFile is too small" >> $Log
			continue
		fi

		rm -f $tuaninfo.bak;  mv $tuaninfo $tuaninfo.bak
		cp $extractTuanFile $tuaninfo
		awk -F'\t' '{
			# 除去表头和大众点评之外的其他行
			if (FNR == 1 || $3=="大众点评") {
				next
			}
			print
		}' $tuaninfo.bak >> $tuaninfo

		echo "handle $city done."	
	done

	LOG "dispatch $allTuanFile to online done." >> $Log
}




# 抽取大众点评的足疗店铺的数据
function extract_dianping_foot_shops() {
        input=$1;  output=$2;
        awk -F'\t' '{
                if (FNR == 1) {
                        for(row=1; row<=NF; row++) {
                               if ($row == "subcuisine") { categoryRow = row;}
                        }
                        print; next
                }
                if (NF < 10 || $categoryRow!~/足疗/) {
                        next
                }
		print
        }' $input > $output
        LOG "extract dianping foot shops done[$output]."
}

# 大众点评足疗店铺的评论信息
function extract_dianping_foot_comment() {
	shopFile=$1;  commentFile=$2; destFile=$3;

	awk -F'\t' 'ARGIND==1 {
		shopids[$1]
	} ARGIND==2 {
		if (FNR == 1) {
			print; next
		}
		shopid = $2;
		if (shopid in shopids) {
			print
		}
	}' $shopFile $commentFile > $destFile

	LOG "extract dianping foot comment done."
}





# 将足疗相关的最新团购信息分发到线上
function dispatch_footService() {
	# 抽取足疗类团购信息
	OnlinePath="/fuwu/Merger/Output"
	FootOnlinePath="/fuwu/DataCenter/foot"
	for allTuanFile in $(ls tuan/*.tuan); do
		city=${allTuanFile/.tuan/};  city=${city/tuan\//}

		# 大众点评完整的基本信息
		baseinfo=$OnlinePath/$city/play/dianping_detail.baseinfo.table
		comments=$OnlinePath/$city/play/dianping_detail.comment.table
		if [ ! -f $baseinfo ]; then
			continue
		fi

		# 抽取出足疗的基本信息数据
		footBaseinfoDest=$FootOnlinePath/$city/dianping_detail.baseinfo.table
		if [ ! -d $(dirname $footBaseinfoDest) ]; then
			mkdir -p $(dirname $footBaseinfoDest)
		fi
		extract_dianping_foot_shops $baseinfo $footBaseinfoDest
		
		# 抽取出足疗的评论信息数据
		footCommentDest=$FootOnlinePath/$city/dianping_detail.comment.table
		extract_dianping_foot_comment $footBaseinfoDest $comments $footCommentDest


		# 抽取足疗的团购的信息，与抽取美食的公用一个函数
		footTuanDest=$FootOnlinePath/$city/dianping_detail.tuan.table
		extractTuanFile=tuan/$city.tuan.foot
		extract_restaurant_tuan $footBaseinfoDest $allTuanFile $extractTuanFile


		line=$(wc -l $extractTuanFile | awk '{print $1}')
		if [ $line -lt 10 ]; then
			LOG "$extractTuanFile is too small" >> $Log
			continue
		fi

		rm -f $footTuanDest.bak;  mv $footTuanDest $footTuanDest.bak
		# 直接覆盖，再将非大众点评的数据拷贝出来
		cp $extractTuanFile $footTuanDest
		awk -F'\t' '{
			# 除去表头和大众点评之外的其他行
			if (FNR == 1 || $3=="大众点评") {
				next
			}
			print
		}' $footTuanDest.bak >> $footTuanDest

		echo "handle $city done."	
	done

	LOG "dispatch foot service tuan of $city to online done." >> $Log
}




function main() {
	# 删除历史数据
	find ./history/tuan/ -ctime +7 | xargs rm -f {}
	find ./logs/ -ctime +7 | xargs rm -f {}

	# 下载当天的团购信息
	/usr/bin/python bin/build_dianping_tuan.py -daily

	# 合并到全部团购信息中	
	merge_tuan

	# 分发美食的团购数据到线上
	#dispatch_restaurant_tuan
	dispatch_dianping_tuan restaurant

	# 分发美食的团购数据到线上
	#dispatch_play_tuan
	dispatch_dianping_tuan play

	# 分发足疗的团购数据到线上
	dispatch_footService
}

main

