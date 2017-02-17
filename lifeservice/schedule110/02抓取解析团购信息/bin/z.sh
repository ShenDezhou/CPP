awk -F'\t' 'ARGIND==1{
	#º£ÄÏ    Çíº£    qionghai
	if (NF == 3) {
		cityPinyin[$2] = $3
	}
} ARGIND==2 {
	if ($2 in cityPinyin) {
		print $2 "\t" $1 "\t" cityPinyin[$2]
	}
}' conf/dianping_city_pinyin_conf conf/dianping_city_code_name_conf
