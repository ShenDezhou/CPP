#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-05 17:24
# * Filename	 : build_activity_dianping_merge.sh
# * Description	 : 
# * *****************************************************************************/
#!/bin/bash
#coding=gb2312

. ./bin/Tool.sh

Dianping_Play_Shops="tmp/dianping_play_shops"

function extract_dianping_play() {
	DianpingPlayPath="/fuwu/Merger/Output"
	BaseinfoFile="play/dianping_detail.baseinfo.table"
	
	rm -f $Dianping_Play_Shops
	for city in $(ls $DianpingPlayPath/); do
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			LOG "movie or other dir, continue."
			continue
		fi
		
		baseinfo=$DianpingPlayPath/$city/$BaseinfoFile
		if [ ! -f $baseinfo ]; then
			LOG "$baseinfo is not exist!"
		fi
		
		awk -F'\t' -v CITY=$city '{
			id=$1; url=$2; title=$3; poi=$5;
			print CITY "\t" id "\t" title "\t" poi
		}' $baseinfo >> $Dianping_Play_Shops
		LOG "get dianping play shops of $city done."
	done
	LOG "get dianping play shops done. [$Dianping_Play_Shops]"
}


function merge_activity_venues() {
	for activityFile in $(ls /fuwu/Merger/Output/other/*_activity.table); do
		basename=$(basename $activityFile)
		
		awk -F'\t' '{print $1"\t"$6"\t"$13"\t"$4}' $activityFile > tmp/$basename.extract
		output=$activityFile.merge
		
		#python bin/build_activity_venues.py $Dianping_Play_Shops tmp/$basename.extract $output
		echo "merge activity venues done. [$output]"
	done
	echo "merge all activity venues done."
}


function main() {
	 extract_dianping_play

	# merge_activity_venues

}


main

#awk -F'\t' '{print $6"\t"$13}' Damai/data/damai_activity.table | head
