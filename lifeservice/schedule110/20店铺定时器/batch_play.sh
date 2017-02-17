
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


# 批量执行命令
function batch_command() {
	Passwd="Tupu@2015"
	fileNum=0;
	for host in ${publicHostList[@]}; do
		echo -e "==========  $host ============"
		fileNum=$((fileNum + 1));
		#./expect/mkdir.exp $host
		./expect/scp_file.exp "bin/parser_play.py" $host":/search/odin/kangq/bin/" "$Passwd"
		./expect/scp_file.exp "bin/build_play.sh" $host":/search/odin/kangq/" "$Passwd"
		./expect/scp_file.exp "data/play.$fileNum" $host":/search/odin/kangq/tmp/" "$Passwd"
		./expect/execute_script.exp "$host" "/search/odin/kangq/" "sh -x build_play.sh tmp/play.$fileNum 1>std.play 2>err.play &" "$Passwd"
		#./expect/scp_file.exp $host":/search/odin/kangq/tmp/play.hui" "data/play.hui.$fileNum" "$Passwd"
		#./expect/execute_script.exp "$host" "/search/odin/kangq/" "ps -ef|fgrep build_play.sh | fgrep -v fgrep |awk '{print $2}' | xargs kill -9 -g" "$Passwd"
		echo -e "\n\n"
		#sleep 2
	done
}

function backup() {
        today=$(date +%Y%m%d%H%M)
        cp Output/play.hui history/play/$today
        filelist=`ls -r history/play/`
        cnt=0
        for file in $filelist
        do
                if [ $cnt -ge 5 ]
                then
                        rm history/play/$file -rf
                fi
                let cnt=cnt+1
        done

        fileNum=0;
        for host in ${publicHostList[@]}; do
                echo -e "==========  $host ============"
                fileNum=$((fileNum + 1));
                ./expect/scp_file.exp $host":/search/odin/kangq/tmp/play.hui" "data/play.hui.$fileNum" "$Passwd"
        done
        cat data/play.hui.* | iconv -futf8 -tgbk -c > tmp/play.hui

	line1=`wc -l tmp/play.hui | awk '{print $1}'`
        line2=`wc -l Output/play.hui | awk '{print $1}'`
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
                mv -f tmp/play.hui Output/play.hui
        else
                sendmail_s="http://mail.portal.sogou/portal/tools/send_mail.php?uid=shendezhou@sogou-inc.com&fr_name=shendezhou&fr_addr=shendezhou@sogou-inc.com&title=点评抓取失败&body=food！&mode=txt&maillist=shendezhou@sogou-inc.com;liubing@sogou-inc.com"
                echo $sendmail_s
                curl $sendmail_s
        fi
}

fileSize=$hostSize
cat /search/kangq/fuwu/OfflineData/baseinfo_play/*/dianping_detail.baseinfo.table | awk '$2!="" && $2!="url"{sub(/www/,"m",$2);print $2}' > tmp/play
sh bin/fileSplit_play.sh tmp/play $fileSize

backup
batch_command
