#!/bin/bash

mtime_city_id_name_conf="conf/mtime_citys_conf"

sort -t$'\t' -k1,1n $1 > $1.sort
awk -F'\t' 'BEGIN {
	print "city\tcityid\tcinemaid\ttitle\taddr\ttel\tpoi\tbusinesstime\thasimax\tscore"
} ARGIND == 1 {
	if(NF != 2) { next }
	cityID = $1;  cityName = $2;
	cityIDNameMap[cityID] = cityName
} ARGIND == 2 {
	if (NF != 9) { next }
	cityID = $1
	if (cityID in cityIDNameMap) {
		cityName = cityIDNameMap[cityID]
		print cityName "\t" $0
	}
}' $mtime_city_id_name_conf $1.sort
