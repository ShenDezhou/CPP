#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-05-30 18:46
# * Filename	 : build_dianping_invalid_shops.sh
# * Description	 : 过滤出大众点评无效的店铺
# * *****************************************************************************/


BaseinfoPath=/fuwu/Merger/Output
UrlPath=/fuwu/Source/Cooperation/Dianping/url
Type=restaurant






function get_restaurant_urls_imp() {
	local baseinfoFile=$1;  local urlFile=$2

	awk -F'\t' 'BEGIN {
		idCount=0; idLine=""; maxCountPerLine=40;
	}{
		# 根据过滤条件，过滤无效的数据
		if (FNR == 1) {
			for(idx=1; idx<=NF; ++idx) {
				if($idx == "id") { idRow = idx }
				if($idx == "score") { scoreRow = idx }
				if($idx == "avgPrice") { avgPriceRow = idx }
			}
			next
		}
		# 评分为0 && 没有价格
		if ($scoreRow==0 && ($avgPriceRow=="" || $avgPriceRow=="-")) {
			shopid = $idRow
			gsub("dianping_", "", shopid)
			# 如果已经达到一行的个数，则输出
			if (idCount == maxCountPerLine) {
				print idLine;
				idLine = "";  idCount=0;
			}
			# shop ids
			idCount++
			if (idLine == "") {
				idLine = shopid
			} else {
				idLine = idLine "," shopid
			}
		}
	} END {
		if (idLine != "") {
			print idLine
		}
	}' $baseinfoFile > $urlFile
	echo "get shop urls for $baseinfoFile done."

}




function get_restaurant_urls() {

	for city in $(ls $BaseinfoPath/); do
		#city="lasa"
		baseinfoFile="$BaseinfoPath/$city/$Type/dianping_detail.baseinfo.table"
		urlFile=$UrlPath/$Type/$city.urls
		if [ ! -f $baseinfoFile ]; then
			continue
		fi
		get_restaurant_urls_imp $baseinfoFile $urlFile

		invalidIDFile=$BaseinfoPath/$city/$Type/dianping_invalid_ids
		python bin/build_dianping_invalidshop.py $urlFile $invalidIDFile
		echo "get invalid shop ids for $urlFile done."
	done
}



function main() {
	# 计算各个城市可能会停业的店铺id
	get_restaurant_urls	

	# 逐个城市进行计算
	

}

main
