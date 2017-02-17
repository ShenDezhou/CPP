#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-25 19:07
# * Filename	 : update_dianping_restaurant.sh
# * Description	 : 更新大众点评的restaurant的数据
# * *****************************************************************************/


. ./bin/Tool.sh
. ./bin/KVFileTool.sh



OfflineRestaurantPath=/fuwu/Source/offlinedb/restaurant
OnlineRestaurantPath=/fuwu/Source/Input
RestaurantBackupPath=/fuwu/Source/history/restaurant
# 合并 & 去重restaurant数据
function merge_uniq_restaurant() {
	for offlineFile in $(ls $OfflineRestaurantPath); do
		city=${offlineFile/_result/}	
		onlineFile=${OnlineRestaurantPath}/${city}_restaurant_dianping_detail
		if [ ! -f $onlineFile ]; then
			continue
		fi
		cat $onlineFile $OfflineRestaurantPath/$offlineFile > $onlineFile.merge
		backupFile $onlineFile $RestaurantBackupPath
		uniq_kv_files $onlineFile.merge $onlineFile
		LOG "merge & uniq $city restaurant file done."
	done
}


# 拷贝offline解析的restaurant的数据	
function scp_offline_restaurant() {
	OfflineDBRemoteHost="10.134.96.110"
	OfflineDBPath=/search/fangzi/ServiceApp/Dianping/data/restaurant/result
	LocalOfflineDBPath=/fuwu/Source/offlinedb/restaurant/
	scp $OfflineDBRemoteHost:$OfflineDBPath/*result $LocalOfflineDBPath
	LOG "scp offline restaurant file from $OfflineDBRemoteHost done."
}




# 大众点评的美食(餐馆)数据处理
function build_dianping_restaurant() {
	# 拷贝offline解析的restaurant的数据	
	scp_offline_restaurant

	# 合并 & 去重店铺数据
	merge_uniq_restaurant

	# 分割原始数据
	for file in $(ls Input/*restaurant_dianping_detail); do
		python bin/ServiceAppPartition.py -partition $file
		LOG "partition for [$file] done."
	done

	# 归一化数据
	normConf='conf/dianping_restaurant_norm_conf'
	for file in $(ls Input/*restaurant_dianping_detail.*); do
		python bin/ServiceAppPartition.py -normal $file $normConf
		LOG "normalize for [$file] done."
	done

	# 转单行表格形式之前需要特殊处理(评论和推荐菜)
	# 一个url对应多实例的情况
	for file in $(ls Input/*restaurant_dianping_detail.recomfood.norm); do
		preHandle $file
	done
	for file in $(ls Input/*restaurant_dianping_detail.comment.norm); do
		preHandle $file
	done

	# 转成表格式
	for file in $(ls Input/*restaurant_dianping_detail.*.norm); do
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



# 为美食类实体添加ID
# 需要对每个城市的美食类的 baseinfo; recomfood; comment; tuan 四个文件添加ID
# 此函数需要继续拆分
function updateRestaurantID() {
	URL_ID_CONF="conf/restaurant_url_id_conf"
	URL_ID_CONF_UPDATE="conf/restaurant_url_id_conf.update"
	NOW_DIANPING_ID_MAP="conf/9now_dianping_id_map"		# 美味不用等 大众点评的映射
	
	filePrefix=$1
:<<EOF
	# 为基本信息表添加ID，已存在ID的URL，取所有URL对应ID的最小值
	baseinfoFile=${filePrefix}.baseinfo.table
	awk -F'\t' 'BEGIN {
		MAXID = 1; URLROW = -1; 
	} ARGIND == 1 {
		url=$1; id=$2
		urlIDMap[url] = id
		MAXID = (id > MAXID) ? id : MAXID
	} ARGIND == 2 {
		if (NF != 2) { next }
		id = $1; dianpingid = $2;
		nowDpMap[dianpingid] = id
	} ARGIND == 3 {
		# 添加ID字段，找到URL所在列
		if (FNR == 1) {
			print "id\t" $0 "\twaitid"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					URLROW = row; break;
				}
			}
		} else {
			# 检查URL是否已经有对应ID
			if ($URLROW == "") {
				next
			}

			curUrlID = MAXID + 1;
			waitID = -1;
			urlLen = split($URLROW, urlArray, "@@@")
			for (idx=1; idx<=urlLen; idx++) {
				tmpUrl = urlArray[idx]
				if (tmpUrl in urlIDMap && urlIDMap[tmpUrl] < curUrlID) {
					curUrlID = urlIDMap[tmpUrl]
				}
				gsub(/.*\//, "", tmpUrl)
				if (tmpUrl in nowDpMap) {
					waitID = nowDpMap[tmpUrl]
				}
			}
			
			# 所有URL都不存在映射ID
			if (curUrlID == MAXID + 1) {
				MAXID += 1
			}
			

			# 增加一个innerid 和 美味不用等的ID
			print curUrlID "\t" $0 "\t" waitID

			# 更新URL对应的ID
			for (idx=1; idx<=urlLen; idx++) {
				tmpUrl = urlArray[idx]
				urlIDMap[tmpUrl] = curUrlID
			}
		}
	} END {
		# 更新url-id配置文件
		for (url in urlIDMap) {
			print url "\t" urlIDMap[url] > "'$URL_ID_CONF_UPDATE'"
		}
	}' $URL_ID_CONF $NOW_DIANPING_ID_MAP $baseinfoFile > $baseinfoFile.id
	LOG "add id for [$baseinfoFile] done. output is [$baseinfoFile.id] "
EOF
	# 为基本信息表添加ID，已存在ID的URL，取所有URL对应ID的最小值
	baseinfoFile=${filePrefix}.baseinfo.table
	awk -F'\t' 'BEGIN {
		MAXID = 1; URLROW = -1; 
	}
	# 大众点评的url构建id
	function get_dianping_url_id(url) {
		id = url;  gsub(/.*\//, "", id)
		return "dianping_" id
	}
	function get_url_id(url) {
		if (url~/www.dianping.com/) {
			return get_dianping_url_id(url)
		}
		return ""
	}

	# 加载点评的 url-id的映射
	ARGIND == 1 {
		url=$1; id=$2
		urlIDMap[url] = id
	}
	# 美味不用等与大众点评的映射
	ARGIND == 2 {
		if (NF != 2) { next }
		id = $1; dianpingid = $2;
		nowDpMap[dianpingid] = id
	} 
	# 为没加id的大众点评网的数据添加id
	ARGIND == 3 {
		# 添加ID字段，找到URL所在列
		if (FNR == 1) {
			print "id\t" $0 "\twaitid"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					URLROW = row; break;
				}
			}
			next
		}
		if ($URLROW == "") { next }

		# 如果URL是合并的，取其中之一，生成ID
		urlLen = split($URLROW, urlArray, "@@@")
		curUrlID = get_url_id(urlArray[1])
		if (curUrlID == "") { next }

		waitID = -1;
		gsub(/.*\//, "", tmpUrl)
		if (tmpUrl in nowDpMap) {
			waitID = nowDpMap[tmpUrl]
		}
			
		# 增加一个innerid 和 美味不用等的ID
		print curUrlID "\t" $0 "\t" waitID

		# 更新URL对应的ID
		for (idx=1; idx<=urlLen; idx++) {
			tmpUrl = urlArray[idx]
			urlIDMap[tmpUrl] = curUrlID
		}
		
	} END {
		# 更新url-id配置文件
		for (url in urlIDMap) {
			print url "\t" urlIDMap[url] > "'$URL_ID_CONF_UPDATE'"
		}
	}' $URL_ID_CONF $NOW_DIANPING_ID_MAP $baseinfoFile > $baseinfoFile.id
	LOG "add id for [$baseinfoFile] done. output is [$baseinfoFile.id] "
	

	# 为推荐菜添加ID，并拆分字段
	recomfoodFile=${filePrefix}.recomfood.table
	awk -F'\t' 'BEGIN {
		recomfoodID = 0; urlRow = -1;
	} ARGIND == 1 {
		url = $1;  id = $2;
		urlIDMap[url] = id;	
	} ARGIND == 2 {
		if (FNR == 1) {
			# 添加id字段，拆分推荐菜字段成多个子字段
			print "recomfoodid\tresid\tresUrl\tresName\trecommUrl\trecommName\trecommCount\trecommPhoto\trecommPrice"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					urlRow = row; break;
				}
			}
		} else {
			resUrl = $urlRow
			if (!(resUrl in urlIDMap)) {
				next
			}
			gsub("@@@", "\t", $0)
			print (++recomfoodID) "\t" urlIDMap[resUrl] "\t" $0
		}
	}' $URL_ID_CONF_UPDATE $recomfoodFile > $recomfoodFile.id
	LOG "update tuanid for [$recomfoodFile] to [$recomfoodFile.id] done."


	# 为评论，推荐菜，团购信息添加ID
	commentFile=${filePrefix}.comment.table
	awk -F'\t' 'BEGIN {
		commentID = 0; urlRow = -1;
	} ARGIND == 1 {
		url = $1;  id = $2;
		urlIDMap[url] = id;	
	} ARGIND == 2 {
		if (FNR == 1) {
			print "commentid\tresid\t" $0
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					urlRow = row; break;
				}
			}
		} else {
			resUrl = $urlRow
			if (!(resUrl in urlIDMap)) {
				next
			}
			print (++commentID) "\t" urlIDMap[resUrl] "\t" $0
		}
	}' $URL_ID_CONF_UPDATE $commentFile > $commentFile.id
	LOG "update tuanid for [$commentFile] to [$commentFile.id] done."


	# 为团购信息添加ID
	tuanFile=${filePrefix}.tuan.table
	awk -F'\t' 'BEGIN {
		tuanID = 0; urlRow = -1; tuanRow = -1;
	} ARGIND == 1 {
		url = $1;  id = $2;
		urlIDMap[url] = id;	
	} ARGIND == 2 {
		if (FNR == 1) {
			print "tuanid\tresid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					urlRow = row;
				} else if ($row == "tuanInfo") {
					tuanRow = row;
				}
			}
		} else {
			resUrl = $urlRow; tuanInfo = $tuanRow;
			if (!(resUrl in urlIDMap) || tuanInfo=="") {
				next
			}
			tuanLen = split(tuanInfo, tuanInfoArr, "###")
			for (i=1; i<=tuanLen; i++) {
				info = tuanInfoArr[i]
				gsub("@@@", "\t", info)
				if(info~/^惠\t/) {
					continue
				}
				print (++tuanID) "\t" urlIDMap[resUrl] "\t大众点评\t" info
			}
		}
	}' $URL_ID_CONF_UPDATE $tuanFile > $tuanFile.id
	LOG "update tuanid for [$tuanFile] to [$tuanFile.id] done."

	# 为图集添加ID
	photosetFile=${filePrefix}.photoset.table
	awk -F'\t' 'BEGIN {
		photosetID = 0; urlRow=-1; psetRow=-1;  cPsetRow=-1;
	} ARGIND == 1 {
		url = $1;  id = $2;
		urlIDMap[url] = id;	
	} ARGIND == 2 {
		if (FNR == 1) {
			print "psetid\tresid\tphotoset"
			for (row=1; row<=NF; ++row) {
				if ($row == "url") {
					urlRow = row;
				} else if ($row == "photoSet") {
					psetRow = row;
				} else if ($row == "conditionPhotoSet") {
					cPsetRow = row;
				}
			}
		} else {
			resUrl = $urlRow; photoset = $psetRow; cphotoset = $cPsetRow;
			if (!(resUrl in urlIDMap) || (photoset=="" && cphotoset=="")) {
				next
			}
			# 去除相关文字说明
			len = split(cphotoset, cphotosetArray, ",")
			cphotoset = "";
			for (i=1; i<=len; i++) {
				sublen = split(cphotosetArray[i], array, "@@@")
				if (sublen < 2) { continue; }
				if (cphotoset == "") {
					cphotoset = array[2]
				} else {
					cphotoset = cphotoset "," array[2]
				}
			}

			# 合并图集与环境图集数据
			if (photoset == "") {
				photoset = cphotoset
			} else {
				photoset = photoset "," cphotoset
			}
			print (++photosetID) "\t" urlIDMap[resUrl] "\t" photoset
		}
	}' $URL_ID_CONF_UPDATE $photosetFile > $photosetFile.id
	LOG "update photosetid for [$photosetFile] to [$photosetFile.id] done."



	# 更新url-id配置文件
	oldConfModifyTime=$(ls -l --time-style=long-iso $URL_ID_CONF | awk '{time=$6""$7; gsub(/[\-\.:]/, "", time); print time;}')
	newConfModifyTime=$(ls -l --time-style=long-iso $URL_ID_CONF_UPDATE | awk '{time=$6""$7; gsub(/[\-\.:]/, "", time); print time;}')
	oldConfSize=$(ls -l $URL_ID_CONF | awk '{size=$5; print size;}')
	newConfSize=$(ls -l $URL_ID_CONF_UPDATE | awk '{size=$5; print size;}')
	# 时间有更新，文件大小增加
	if [ $newConfModifyTime -ge $oldConfModifyTime -a $newConfSize -ge $oldConfSize ]; then
		destFile=$(basename $URL_ID_CONF).$(todayStr)
		#echo $destFile
		mv -f $URL_ID_CONF history/conf/$destFile
		mv -f $URL_ID_CONF_UPDATE $URL_ID_CONF
		LOG "update [$URL_ID_CONF] file done."
	fi
}


function updateAllRestaurantID() {
	# 合并 城市/类别/下的文件
	for cityDir in $(ls Crawler/); do
		restaurantFile="Input/${cityDir}_restaurant_dianping_detail"
		updateRestaurantID $restaurantFile
	done

	#updateRestaurantID "Input/beijing_restaurant_dianping_detail"
	#updateRestaurantID "Input/shanghai_restaurant_dianping_detail"

}



# 将处理好的table格式文件分发到不同目录下
# 需要保证输入文件的格式为  city_type_filename  的严格的格式
function dispatch() {
	for srcFile in $(ls Input/*.table.id); do
		basename=$(basename $srcFile)
		destFile=$(echo $basename | awk '{gsub(".id", "", $1); sub("_", "/", $1); sub("_", "/", $1); print "Output/"$1}')
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
	# 大众点评美食数据处理
	# build_dianping_restaurant

	# 更新实体ID
	 updateAllRestaurantID

	# 分发到指定目录下，用于建索引
	# dispatch
}

main








