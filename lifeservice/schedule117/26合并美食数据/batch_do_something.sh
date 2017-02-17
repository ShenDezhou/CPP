#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-06-14 10:15
# * Filename	 : batch_do_something.sh
# * Description	 : 批量处理结果
# * *****************************************************************************/

RestaurantOnlinePath=/fuwu/Merger/Output
DianpingAreaConf=/fuwu/Source/conf/dianping_city_business_cook_conf

Type="restaurant"


# 批量处理美味不用等的id有效性
function handle_invalid_waitid() {
	validWaitid=/fuwu/Source/conf/9now_dianping_id_map
	baseinfo=$1;
	if [ -f $validWaitid -a -f $baseinfo ]; then
		awk -F'\t' 'ARGIND==1 {
			if(NF >= 2) {
				waitid["dianping_" $2] = $1
			}
		} ARGIND==2 {
			if (FNR == 1) {
				for (row=1; row<=NF; ++row) {
					if ($row == "id") { idRow = row }
					if ($row == "waitid") { waitRow = row }
				}
				print; next
			}
			$waitRow = -1
			if ($idRow in waitid) {
				$waitRow = waitid[$idRow]
			}
			line = $1
			for (row=2; row<=NF; ++row) {
				line = line "\t" $row
			}
			print line
		}' $validWaitid $baseinfo > $baseinfo.waitid
		
		rm -f $baseinfo; mv $baseinfo.waitid $baseinfo
	fi

}




function test_so_something() {
	#handle_invalid_waitid /fuwu/Merger/Output/beijing/restaurant/dianping_detail.baseinfo.table
	handle_invalid_waitid /fuwu/Merger/Output/zunyi/restaurant/dianping_detail.baseinfo.table
}


function batch_do_something() {
	for city in $(ls $RestaurantOnlinePath/); do
		baseinfoFile="$RestaurantOnlinePath/$city/$Type/dianping_detail.baseinfo.table"
		handle_invalid_waitid $baseinfoFile
		echo "handle $city $Type done."
	done
}


if [ $# -gt 0 ]; then
	Type=$1
fi

batch_do_something


#test_so_something
