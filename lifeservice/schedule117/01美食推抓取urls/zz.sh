

awk -F'\t' 'ARGIND==1 {
	filterids[$1]
} ARGIND==2 {
	if (FNR == 1) { print; next}
	if ($1 in filterids) {
		next
	}
	print

}' /fuwu/DataCenter/conf/invalid_restaurant_shopid baseinfo_restaurant/beijing/dianping_detail.baseinfo.table > baseinfo_restaurant/beijing/dianping_detail.baseinfo.table.f
