awk -F'\t' 'ARGIND==1 {
	if (NF != 6) {
		next
	}
	poi = $4;  address = $5
	pois[address] = poi

} ARGIND==2 {
	if (NF != 5) {
		next
	}
	city=$2; url=$3; title=$4; address=$5;
	
	gsub(/#.*$/, "", url)
	url = "https://mdianying.baidu.com" url
	
	cinemaid = url
	gsub(/.*=/, "", cinemaid)

	poi = ""
	if (address in pois) {
		poi = pois[address]
	}
	
	print cinemaid "\tÅ´Ã×µçÓ°\t" title "\t" title "\t" address "\tprovince\t" city "\tarea\tregion\t" poi   

}' tmp/cinema.sort data/nuomi_cinema_list 
