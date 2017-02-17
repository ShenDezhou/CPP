#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-05-18 10:31
# * Filename	 : update_play.sh
# * Description	 : 拷贝美食相关的数据到数据中心
# * *****************************************************************************/


InvalidShopConf=/fuwu/DataCenter/conf/invalid_play_shopid


function copy_baseinfo() {
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
	
	# 过滤无效shop ids
	awk -F'\t' 'ARGIND==1 {
		invalidshops[$1]
	} ARGIND==2 {
		if (FNR==1) { 
			print; 
			for (row=1; row<=NF; ++row) {
				if ($row == "id" || $row == "resid") {
					idRow = row
				}
				if ($row == "breadcrumb") {
					crumbRow = row
				}
			}
			next 
		}
		if ($crumbRow~/餐厅/){ next }
		if ($idRow in invalidshops) { next }
		print
	}' $InvalidShopConf $srcFile > $destFile

	echo "copy $srcFile to $destFile done."
}



function copy_comment() {
	srcPath=$1;  destPath=$2;  fileName=$3;  baseinfo=$4;
	
	if [ ! -d $destPath ]; then
		mkdir -p $destPath
	fi
	
	srcFile=$srcPath/$fileName
	destFile=$destPath/$fileName
	baseFile=$srcPath/$baseinfo	

	srcFileLine=$(cat $srcFile | wc -l)
	if [ $srcFileLine -le 1 ]; then
		echo "$srcFileLine is too small"
		return 1
	fi

	if [ -f $destFile ]; then
		rm -f $destFile.bak;  mv $destFile $destFile.bak
	fi

	awk -F'\t' ' ARGIND==1 {
		if (FNR==1) { 
			for (row=1; row<=NF; ++row) {
				if ($row == "id") { idRow = row }
			}
			next
		}
		playids[$idRow]
	} ARGIND==2 {
		if (FNR==1) {
			for (row=1; row<=NF; ++row) {
				if ($row == "resid") { idRow = row }
			}
			print; next;
		}
		if (!($idRow in playids)) { next }
		print
	}' $baseFile $srcFile > $destFile

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
		
		copy_baseinfo $srcPath $destPath "dianping_detail.baseinfo.table"
		copy_comment $srcPath $destPath "dianping_detail.comment.table" "dianping_detail.baseinfo.table"
	done
}




