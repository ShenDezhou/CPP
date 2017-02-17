#!/bin/bash
#coding=gb2312

. ./bin/Tool.sh

if [ $# -lt 1 ]; then
	echo "sh $0 [restaurant|play]"; exit -1;
fi
Type="$1"



# <url  id>
UrlIDMapConf="/search/zhangk/Fuwu/Source/conf/restaurant_url_id_conf"
# <url photoset>
SpidePhotosetPath="/search/zhangk/Fuwu/Spider/Dianping/Result/restaurant/photo/"
# <id  resid  photoset>
OnlinePhotosetPath="/search/zhangk/Fuwu/Merger/Output"

MergePath="/search/zhangk/Fuwu/Merger/data/$Type/photo"
if [ ! -d $MergePath ]; then
	mkdir -p $MergePath
fi

if [ "$Type" = "play" ]; then
	UrlIDMapConf="/search/zhangk/Fuwu/Source/conf/play_url_id_conf"
	SpidePhotosetPath="/search/zhangk/Fuwu/Spider/Dianping/Result/play/photo/"
fi




function merge_photoset_imp() {
	onlinePhotoset=$1;  spidePhotoset=$2; mergePhotoset=$3;
	awk -F'\t' 'BEGIN {
		lastid = ""; lastphotoset =""; idx = 0;
		print "psetid\tresid\tphotoset"
	}
	# 注意这里返回的字符串第一个字符是\t
	function normPhotoset(photos) {
		photoset = ""
		len = split(photos, array, ",")
		for(i=1; i<=len; i++) {
			gsub(/@@@.*/, "", array[i])
			photoset = photoset "," array[i]
		}
		return substr(photoset, 2)
	}
	# 去重
	function uniqPhotoset(photos) {
		uniqPhotos = "";
		len = split(photos, photoArray, ",")
		for (i=1; i<=len; ++i) {
			if (length(photoArray[i]) <= 5) { continue;}
			if (uniqPhotos == "") {
				uniqPhotos = photoArray[i]
			} else {
				uniqPhotos = uniqPhotos "," photoArray[i]
			}
		}
		return uniqPhotos
	}
	# 打印结果
	function printPhotoset() {
		if (length(lastid) > 0 && length(lastphotoset) > 0) {
			# 将线上存在的也加进去
			if (lastid in onlinePhotoMap) {
				lastphotoset = lastphotoset "," onlinePhotoMap[lastid]
				lastphotoset = uniqPhotoset(lastphotoset)
				printedOnlinePhotoset[lastid] = 1
			}
			print (++idx) "\t" lastid "\t" lastphotoset
		}
	}
	# 打印线上与抓取没有合并上的数据
	function printRestOnlinePhotoset() {
		for (id in onlinePhotoMap) {
			if (!(id in printedOnlinePhotoset)) {
				print (++idx) "\t" id "\t" onlinePhotoMap[id]
			}
		}
	}
	# 加载 url-id映射
	ARGIND == 1 {
		url = $1; id = $2
		urlidMap[url] = id;
	} 
	# 加载线上图集
	ARGIND == 2 {
		id = $2;  photoset = $3;
		if (id == "resid") { next }
		onlinePhotoMap[id] = photoset
	} 
	# 线下抓取的图集
	ARGIND == 3 {
		url = $1;  photoset = $2;
		if (!(url in urlidMap)) { next }
		curid = urlidMap[url];  photoset = normPhotoset(photoset)
		if (curid != lastid) {
			printPhotoset()
			lastid = curid;  lastphotoset = photoset;
		} else {
			lastphotoset = lastphotoset "," photoset;
		}	

	} END {
		printPhotoset()
		printRestOnlinePhotoset()
	}' $UrlIDMapConf $onlinePhotoset $spidePhotoset > $mergePhotoset

}

function merge_photoset() {
	for city in $(ls $OnlinePhotosetPath/); do
		onlinePhotosetFile="$OnlinePhotosetPath/$city/$Type/dianping_detail.photoset.table"
		
		#spidePhotosetFile="$SpidePhotosetPath/${city}*photo*"
		ls -l -S $SpidePhotosetPath/${city}[._]photo*
		if [ $? -ne 0 ]; then
			continue
		fi
		spidePhotosetFile=$(ls -l -S $SpidePhotosetPath/${city}[._]photo* | head -n1 | awk '{print $NF}')

		if [ ! -f $onlinePhotosetFile -o ! -f $spidePhotosetFile ]; then
			LOG "[Warn]: $onlinePhotosetFile or $spidePhotosetFile is not exist!"
			continue
		fi
		mergePhotoset=$MergePath/${city}_photoset

		sort -u $spidePhotosetFile > $spidePhotosetFile.sort
		merge_photoset_imp $onlinePhotosetFile $spidePhotosetFile.sort $mergePhotoset
		rm -f $spidePhotosetFile.sort

		LOG "merge photoset of $city done. [$mergePhotoset]"
	done
}


function dispatch_mergephotoset() {
	for city in $(ls $OnlinePhotosetPath/); do
		onlinePhotosetFile="$OnlinePhotosetPath/$city/$Type/dianping_detail.photoset.table"
		mergePhotoset=$MergePath/${city}_photoset
		if [ ! -f $onlinePhotosetFile -o ! -f $mergePhotoset ]; then
			LOG "[Warn]: $onlinePhotosetFile or $mergePhotoset is not exist!"
			continue
		fi
		if [ ! -f $onlinePhotosetFile.old ]; then
			mv $onlinePhotosetFile $onlinePhotosetFile.old
		else
			rm -f $onlinePhotosetFile
		fi
		cp $mergePhotoset $onlinePhotosetFile

		LOG "dispatch merge photoset of $city done. [$mergePhotoset]"
	done
}


#merge_photoset

#dispatch_mergephotoset

# 图集也不再需要了
echo "ignore $0"
