#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-05-18 10:31
# * Filename	 : update_play.sh
# * Description	 : 拷贝美食相关的数据到数据中心
# * *****************************************************************************/


InvalidShopConf=/fuwu/DataCenter/conf/invalid_play_shopid


function copy_file() {
	srcPath=$1;  destPath=$2;  fileName=$3;
	
	if [ ! -d $destPath ]; then
		mkdir -p $destPath
	fi
	
	srcFile=$srcPath/$fileName
	destFile=$destPath/$fileName
	
	srcFileLine=$(cat $srcFile | wc -l)
	if [ $srcFileLine -le 1 ]; then
		echo "$srcFileLine is too small"
		return 1
	fi

	if [ -f $destFile ]; then
		rm -f $destFile.bak;  mv $destFile $destFile.bak
	fi
	
	# 直接复制
	#cp $srcFile $destFile

	# 过滤无效shop ids
	awk -F'\t' 'BEGIN {
		crumbRow = -1
	} ARGIND==1 {
		invalidshops[$1]
	} ARGIND==2 {
		if (FNR==1) { 
			print; 
			for (row=1; row<=NF; ++row) {
				if ($row == "id" || $row == "resid") {
					idRow = row
				}
				if ($row == "breadcrumb") { crumbRow = row }
			}
			next 
		}
		if ($idRow in invalidshops) { next }
		if (crumbRow != -1 && $crumbRow~/餐厅/) { next }
		print
	}' $InvalidShopConf $srcFile > $destFile

	echo "copy $srcFile to $destFile done."
}







# 拷贝基本信息
function scp_play_baseinfo() {
	for city in $(ls /fuwu/Merger/Output/); do
		echo "begin to handle $city....."
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			continue
		fi
		srcPath=/fuwu/Merger/Output/$city/play
		destPath=/fuwu/DataCenter/baseinfo_play/$city
		
		copy_file $srcPath $destPath "dianping_detail.baseinfo.table"
		copy_file $srcPath $destPath "dianping_detail.comment.table"
	done
}

# 拷贝团购
function scp_play_tuan() {
	for city in $(ls /fuwu/Merger/Output/); do
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			continue
		fi
		srcPath=/fuwu/Merger/Output/$city/play
		destPath=/fuwu/DataCenter/tuan_play/$city

		copy_file $srcPath $destPath "dianping_detail.tuan.table"
	done
}





if [ $# -lt 1 ]; then
	echo "[Usage]: sh $0 -[baseinfo|tuan]"
	exit -1
fi



if [ "$1" = "-baseinfo" ]; then
	echo "scp play baseinfo...."
	scp_play_baseinfo
elif [ "$1" = "-tuan" ]; then
	scp_play_tuan
else
	echo "[Usage]: sh $0 -[baseinfo|tuan]"
	exit -1
fi
