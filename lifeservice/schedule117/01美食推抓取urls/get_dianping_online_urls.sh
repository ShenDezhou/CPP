#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-05-04 10:15
# * Filename	 : get_dianping_urls.sh
# * Description	 : 获取美食/休闲的url列表，用于离线扫库更新
# * *****************************************************************************/


function get_shop_urls() {
	if [ $# -eq 0 ]; then
		echo "lack of argument" && exit -1
	fi
	local Type=$1

	BaseinfoPath=/fuwu/DataCenter/baseinfo_${Type}   #/beijing/dianping_detail.baseinfo.table
	if [ ! -d $BaseinfoPath ]; then
		echo "$BaseinfoPath path is not exist"
		exit
	fi


	for city in $(ls $BaseinfoPath/); do
		#city="beijing"
		baseinfoFile="$BaseinfoPath/$city/dianping_detail.baseinfo.table"
		urlFile="$BaseinfoPath/$city/dianping_${Type}_${city}.urls"
		if [ ! -f $baseinfoFile ]; then
			echo "$baseinfoFile is not exist!"
			continue
		fi

		awk -F'\t' '{
			if (FNR == 1) {
				for(idx=1; idx<=NF; ++idx) {
					if($idx == "url") { urlRow = idx }
				}
				next
			}
			if (!($urlRow in existUrls)) {
				print $urlRow
				existUrls[$urlRow]
			}
		}' $baseinfoFile > $urlFile

		echo "get $city's $Type urls done."
	done

	echo "get all city's $Type online urls done."

	scp $BaseinfoPath/*/dianping_*.urls 10.134.96.110:/search/fangzi/ServiceApp/Dianping/data/$Type/onlineUrl/



}

:<<EOF
if [ $# -gt 0 ]; then
	Type=$1
	get_shop_urls $Type
fi
EOF


get_shop_urls play
get_shop_urls restaurant



