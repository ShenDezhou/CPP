
# hosts with public ip 
publicHostList=(
  10.142.105.210
  10.143.9.182
  10.143.21.160
  10.143.22.226
  10.134.79.188
  10.134.79.154
  10.134.96.152
  10.142.104.204
  10.142.47.173
)

hostSize=$(echo ${publicHostList[*]} | awk '{print NF}')
Passwd="Tupu@2015"

# 批量执行命令
function batch_command() {
	fileNum=0;
	for host in ${publicHostList[@]}; do
		echo -e "==========  $host ============"
		fileNum=$((fileNum + 1));
		#./expect/mkdir.exp $host
		./expect/scp_file.exp "bin/parser.py" $host":/search/odin/kangq/bin/" "$Passwd"
		./expect/scp_file.exp "bin/build.sh" $host":/search/odin/kangq/" "$Passwd"
		./expect/scp_file.exp "data/shop.$fileNum" $host":/search/odin/kangq/tmp/" "$Passwd"
		./expect/execute_script.exp "$host" "/search/odin/kangq/" "sh -x build.sh tmp/shop.$fileNum 1>std 2>err &" "$Passwd"
		#./expect/scp_file.exp $host":/search/odin/kangq/tmp/shop.hui" "data/shop.hui.$fileNum" "$Passwd"
		#./expect/execute_script.exp "$host" "/search/odin/kangq/" "ps -ef|fgrep build.sh | fgrep -v fgrep |awk '{print $2}' | xargs kill -9 -g" "$Passwd"
		echo -e "\n\n"
		#sleep 2
	done
}

function backup() {
	today=$(date +%Y%m%d%H%M)
	cp Output/shop.hui history/shop/$today
	filelist=`ls -r history/shop/`
	cnt=0
	for file in $filelist
	do
        	if [ $cnt -ge 5 ]
	        then
        	        rm history/shop/$file -rf
	        fi
        	let cnt=cnt+1
	done
        
	fileNum=0;
        for host in ${publicHostList[@]}; do
                echo -e "==========  $host ============"
                fileNum=$((fileNum + 1));
                ./expect/scp_file.exp $host":/search/odin/kangq/tmp/shop.hui" "data/shop.hui.$fileNum" "$Passwd"
        done
	cat data/shop.hui.* | iconv -futf8 -tgbk -c > tmp/shop.hui
	line1=`wc -l tmp/shop.hui | awk '{print $1}'`
	line2=`wc -l Output/shop.hui | awk '{print $1}'`
	flag="0"
	if [ $line1 -ne 0 ];then
		if [ $line1 -gt $line2 ];then
                	line=`echo "scale=0;$line1-$line2" | bc`
             	else
                 	line=`echo "scale=0;$line2-$line1" | bc`
                fi
             	diff_ratio=`echo "$line $line2" |  awk '{printf("%g",$1/$2)}'`
		echo $diff_ratio
		if [ `echo "$diff_ratio < 0.2"|bc` -eq 1 ];then
			flag="1"
		fi
	fi
	if [ $flag == "1" ];then
		mv -f tmp/shop.hui Output/shop.hui
	else
    sendmail_s="http://mail.portal.sogou/portal/tools/send_mail.php?uid=shendezhou@sogou-inc.com&fr_name=shendezhou&fr_addr=shendezhou@sogou-inc.com&title=点评抓取失败&body=food！&mode=txt&maillist=shendezhou@sogou-inc.com;liubing@sogou-inc.com"
		echo $sendmail_s
		curl $sendmail_s	
	fi
}

fileSize=$hostSize
cat /search/kangq/fuwu/OfflineData/baseinfo_restaurant/*/dianping_detail.baseinfo.table | awk '$2!="" && $2!="url"{sub(/www/,"m",$2);print $2}' > tmp/shop
sh bin/fileSplit.sh tmp/shop $fileSize

backup
batch_command
