



function build_dianping_city_area_cook_conf() {
	awk -F'\t' '{
		if ($1 == "business" || $1 == "landmark") {
			#landmark        北京    朝阳区  /search/category/2/10/r14       /search/category/2/10/r2185     新光天地
			cityid=$4;  areaUrl=$4;  districtUrl=$5;  district=$6;
			
			gsub("/search/category/", "", cityid)
			gsub(/\/.*$/, "", cityid)
		
			gsub(/.*\//, "", areaUrl)
			gsub(/.*\//, "", districtUrl)
			
			print $1 "\t" $2 "\t" cityid "\t" $3 "\t" areaUrl "\t" district "\t" districtUrl
		} else if ($1 == "cook"){
			# cook    北京    /search/category/2/10/g3243     新疆菜
			cityid=$3;  cuisineid=$3

			gsub("/search/category/", "", cityid)
			gsub(/\/.*$/, "", cityid)

			gsub(/.*\//, "", cuisineid)
			print $1 "\t" $2 "\t" cityid "\t" $4 "\t" cuisineid
		} else {
			print
		}

	}' $1 
}

#build_dianping_city_area_cook_conf conf/dianping_city_business_cook_conf 



function split_city_play_shop_num_conf() {
	awk -F'\t' '{
		if (NF == 2) {
			print $1
		}
		if (NF > 2) {
			len = split($0, arr, "\t")
			for (i=1; i<=(len-2); ++i) {
				print arr[i] "p1"
			}
			print arr[len-1]
		}
	}' $1
}

split_city_play_shop_num_conf $1

