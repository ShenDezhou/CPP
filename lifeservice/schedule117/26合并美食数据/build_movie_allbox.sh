#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-04 18:25
# * Filename	 : build_movie_allbox.sh
# * Description	 : µçÓ°Æ±·¿
# * *****************************************************************************/
#!/bin/bash
#coding=gb2312

Python="/usr/bin/python2.6"

MovieDetail='/search/zhangk/Fuwu/Merger/Output/movie/movie_detail.table'
$Python bin/build_movie_allbox.py $MovieDetail.allbox

lines=$(cat $MovieDetail.allbox | wc -l)

if [ $lines -gt 50 -a -f $MovieDetail.allbox ]; then
	#rm -f $MovieDetail.bak;  mv -f $MovieDetail $MovieDetail.bak
	#cp -f $MovieDetail.allbox $MovieDetail
	awk -F'\t' 'ARGIND==1 {
		if ($0~/^#/ || $0 == "") {
			next
		}
		filterids[$1]
	} ARGIND == 2 {
		if (FNR == 1) {
			print; next
		}
		if ($1 in filterids) {
			next
		}
		print
	}' conf/movie_black_list_conf $MovieDetail.allbox > $MovieDetail
else
	echo "update allbox error"
fi


