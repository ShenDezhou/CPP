

iconv -futf8 -tgbk -c /search/liubing/spiderTask/result/system/task-151/* | sort -u > tmp/city_cinema

cityUrlConf="conf/city_url_name_conf";  cityCinemaConf="tmp/city_cinema";


awk -F'\t' 'ARGIND == 1 {
	if (NF != 2) {
		next
	}
	url = $1 "/cinema"; city = $2;
	urlName[url] = city
} ARGIND == 2 {
	cityName = ""	
	if ($1 in urlName) {
		cityName = urlName[$1]
	}
	print cityName "\t" $0
}' $cityUrlConf $cityCinemaConf > conf/city_cintma_conf
