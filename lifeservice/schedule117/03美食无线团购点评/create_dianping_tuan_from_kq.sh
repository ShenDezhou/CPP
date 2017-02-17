. ./bin/Tool.sh

Log="logs/dianping_extrainfo.log"

function create_tuan_items_from_kq_imp() {
	input=$1;  output=$2;
	if [ ! -f $input ]; then
		LOG "$input is not exists, create_tuan_items failed!" >> $Log
	fi

	awk -F'\t' 'BEGIN {
		print "id\tresid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell\tdeadline"
	} 
	function printItem() {
		if (itemUrl == "" || title == "") {
			return
		}
		id = itemUrl;  gsub(/.*\//, "", id)
		itemLine = id "\t" resid "\t大众点评\t团\t" itemUrl "\t" title "\t" photo 
		itemLine = itemLine "\t" price "\t" oprice "\t\t\t"
		print itemLine
	}

	{
		if (NF != 3) {
			next
		}
		url=$1;  key=$2;  value=$3;
		if (key == "url") {
			printItem()
			
			resid = url; gsub(/.*\//, "", resid);  resid = "dianping_" resid
			itemUrl = "http://m.dianping.com" value
		}
		if (key == "pic") {
			photo = value;  gsub(/\.jpg.*$/, ".jpg", photo)
		}
		if (key == "title") {
			title = value
		}
		if (key == "price") {
			price = value
		}
		if (key == "oprice") {
			oprice = value;  gsub("￥", "", oprice);
		}
	}' $input > $output
}


function create_tuan_items_from_kq() {
	HuiPath=/fuwu/DataCenter/conf
	TuanPath=/fuwu/Source/Cooperation/Tuan/Input/dianping

	restHuiFile=$HuiPath/shop.hui
	restTuanFile=$TuanPath/restaurant_tuan_from_kq
	create_tuan_items_from_kq_imp $restHuiFile $restTuanFile
	LOG "create restaurant tuan items done." >> $Log

	playHuiFile=$HuiPath/play.hui
	playTuanFile=$TuanPath/play_tuan_from_kq
	create_tuan_items_from_kq_imp $playHuiFile $playTuanFile
	LOG "create play tuan items done." >> $Log
}



function extract_dianping_tuan_imp() {
	baseinfoFile=$1;  tuanFile=$2;  output=$3
	
	awk -F'\t' 'BEGIN {
		idRow = -1; residRow = -1;
	} ARGIND==1 {
		# 找到店铺id的列
		if(FNR == 1) {
			for(i=1; i<=NF; ++i) {
				if ($i == "id") {
					idRow = i
				}
			}
		} else {
			if (idRow != -1) {
				resids[$idRow] = 1
			}
		}
	} ARGIND==2 {
		if (FNR == 1) {
			for(i=1; i<=NF; ++i) {
				if ($i == "resid") {
					residRow = i
				}
			}
			print; next
		}		
		if (residRow != -1 && $residRow in resids) {
			print
		}
	}' $baseinfoFile $tuanFile > $output
}



# 抽取各城市的团购
function extract_dianping_tuan() {
	type=$1
	TuanPath=/fuwu/Source/Cooperation/Tuan/Input/dianping
	tuanFile=$TuanPath/${type}_tuan_from_kq 
	extractPath=$TuanPath/${type}_kq
	baseinfoPath=/fuwu/DataCenter/baseinfo_${type}   #/beijing/dianping_detail.baseinfo.table

	for city in $(ls $baseinfoPath/); do
		baseinfoFile=$baseinfoPath/$city/dianping_detail.baseinfo.table
		extractFile=$extractPath/$city
		if [ -f $baseinfoFile -a -f $tuanFile ]; then
			extract_dianping_tuan_imp $baseinfoFile $tuanFile $extractFile
			LOG "handle $city $type tuan items done." >> $Log
		fi
	done
}



function dispatch_tuan_to_citys() {
	extract_dianping_tuan restaurant
	
	extract_dianping_tuan play
}


function scp_dianping_extrainfo_from_117() {
	Host="10.134.96.110"
	ScpPath="/search/kangq/dianping/Output"
	LocalPath=/fuwu/DataCenter/conf
	scp $Host:$ScpPath/shop.hui $LocalPath/
	scp $Host:$ScpPath/play.hui $LocalPath/
	LOG "scp shop.hui play.hui from 117 done." >> $Log
}


function extract_hui_infos() {
	LocalPath=/fuwu/DataCenter/conf

	restHuiFile=$LocalPath/shop.hui
	restHuiFileSize=$(ls -l $restHuiFile | awk '{print $5}')  
	if [ $restHuiFileSize -gt 500000000 ]; then
		awk -F'\t' 'NF>5{print}' $restHuiFile > $LocalPath/dianping_restaurant_hui_wai_conf
	else
		LOG "Error: $restHuiFile is too small" >> $Log
	fi

	playHuiFile=$LocalPath/play.hui
	playHuiFileSize=$(ls -l $playHuiFile | awk '{print $5}')  
	if [ $playHuiFileSize -gt 30000000 ]; then
		awk -F'\t' 'NF>5{print}' $playHuiFile > $LocalPath/dianping_play_hui_wai_conf
	else
		LOG "Error: $playHuiFile is too small" >> $Log
	fi
	

}


function main() {
	# 从117上拷贝数据过来
	scp_dianping_extrainfo_from_117

	# 抽取出优惠买单等信息,配合点评的基本信息使用
	extract_hui_infos

	# 将抓取的数据转成团购的格式
	create_tuan_items_from_kq

	# 将团购数据拆分到各个城市中
	dispatch_tuan_to_citys
}

main
