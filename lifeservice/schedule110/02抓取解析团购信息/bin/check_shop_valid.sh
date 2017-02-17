


CheckUrl="http://www.dianping.com/poi/assistance/getshoppower.action?shopId="


function check_shop_valid() {
	shopid=$1
	url=${CheckUrl}${shopid}
	wget $url -O tmp/check_result -o tmp/log
	
	statusid=$(grep -P -o '"power":[0-9]+' tmp/check_result)
	statusid=${statusid/\"power\":/}
	echo -e "$shopid\t$statusid"
}


function get_check_shop_ids() {
	type=$1
	resultPath=data/$type/result
	checkUrlPath=tmp/checkurl/$type

	for urlFile in $(ls $resultPath/); do
		city=${urlFile/_url*/}
		checkUrlFile=$checkUrlPath/${city}_checkurls

		awk -F'\t' '{
			if (NF==2 && $1=="url") {
				shopid=$2
				gsub("http://www.dianping.com/shop/", "", shopid)	
				print shopid
			}
		}' $resultPath/$urlFile > $checkUrlFile

		echo "get $city check url file done."
	done
}



function check_city_shops() {
	cityCheckUrlFile=$1; statusFile=$2
	count=0
	for id in $(cat $cityCheckUrlFile); do
		check_shop_valid $id >> $statusFile
		count=$(( count + 1))
		if [ $(( count % 100 )) -eq 0 ]; then
			echo "has handle $count ."
			sleep 1
		fi
	done
	echo "handle $cityCheckUrlFile done"
}


function check_shops() {
	type=$1
	urlPath=tmp/checkurl/$type
	for urlFile in $(ls $urlPath); do

		city=${urlFile/_checkurls/}
		statusFile=data/$type/checkurl/${city}_id_status

		mv $statusFile.bak; mv -f $statusFile $statusFile.bak
		check_city_shops $urlPath/$urlFile $statusFile
	done
}


# 抽取url
#get_check_shop_ids restaurant

#check_shops restaurant



function multi_wget_shops_status_imp() {
	idFile=$1;  resultFile=$2; 

	#echo "$idFile  $resultFile"
	for shopid in $(cat $idFile); do
		url=${CheckUrl}${shopid}
		wget $url -o ${resultFile}.log -O ${resultFile}.tmp
		
		cat ${resultFile}.tmp >> $resultFile
		echo "" >> $resultFile
	done
}


function multi_wget_shops_status() {
	type=$1
	rm -f tmp/all_${type}_urls_part*.result
	for file in $(ls tmp/all_${type}_urls_part*); do
		output=$file.result
		rm -f $output
		multi_wget_shops_status_imp $file $output &
	done

	wait
	echo " all done."
}




function parse_shop_status() {
	type=$1
	cat tmp/all_${type}_*.result | awk -F',' '{
		if ($0~"invalid shopId") {
			next
		}
		shopid=$(NF-1); status=$NF;   
		gsub(/.*:/, "", shopid);  
		gsub("}", "", status);   
		gsub(/.*:/, "", status);
		print shopid"\t"status
	}' > tmp/all_${type}_shopids_status

	awk -F'\t' '{
		if (NF == 2 && $2 != 5) {
			print "dianping_" $1
		}
	}' tmp/all_${type}_shopids_status > tmp/invalid_${type}_shopids

	echo "parse done."
}


# 批量抓取店铺的状态
#multi_wget_shops_status "play"

# 抓取完毕后 解析一下
parse_shop_status "play"




