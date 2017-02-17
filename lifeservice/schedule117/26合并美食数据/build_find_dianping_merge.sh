#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-05 17:24
# * Filename	 : build_activity_dianping_merge.sh
# * Description	 : 
# * *****************************************************************************/
#!/bin/bash
#coding=gb2312

. ./bin/Tool.sh

Dianping_Shops="tmp/dianping_restaurant_play_shops"
DianpingPath="/fuwu/Merger/Output"
BaseinfoFile="dianping_detail.baseinfo.table"

function extract_dianping_shops_imp() {
	city=$1;  type=$2; output=$3;
	baseinfo=$DianpingPath/$city/$type/$BaseinfoFile
	if [ ! -f $baseinfo ]; then
		LOG "$baseinfo is not exist!"
	fi
	
	awk -F'\t' -v CITY=$city '{
		id=$1; url=$2; title=$3; addr=$15;  tel=$16;
		print CITY "\t" id "\t" title "\t" addr "\t" tel
	}' $baseinfo >> $output
}



function extract_dianping_shops() {
	rm -f $Dianping_Shops
	for city in $(ls $DianpingPath/); do
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			LOG "movie or other dir, continue."
			continue
		fi
		
		extract_dianping_shops_imp $city "restaurant" $Dianping_Shops
		extract_dianping_shops_imp $city "play" $Dianping_Shops

		LOG "get dianping restaurant/play shops of $city done."
	done
	LOG "get dianping restaurant/play shops done. [$Dianping_Shops]"
}


function merge_find_shops() {
	for articleFile in $(ls /fuwu/Merger/Output/other/*_article.table); do
		basename=$(basename $articleFile)
		
		# id      url     category        title   reason  shop    addr    time    price   tel     content city
		# city id title addr tel
		# city id category title 
		awk -F'\t' '$NF != ""{print $NF"\t"$1"\t"$6"\t"$7"\t"$10}' $articleFile > tmp/$basename.extract
		output=$articleFile.merge
		
		# 美食类  和restaurant合并
		# 非美食类  与play合并

		#python bin/build_article_shops.py $Dianping_Restaurant_Shops tmp/$basename.extract $output
		echo "merge article shop done. [$output]"
	done
	echo "merge all article shops done."
}


function main() {
	extract_dianping_shops

	# merge_activity_venues

}


main

#awk -F'\t' '{print $6"\t"$13}' Damai/data/damai_activity.table | head
