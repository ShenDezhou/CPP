Path=/fuwu/DataCenter/baseinfo_play
beijingLine=$( cat $Path/beijing/dianping_detail.baseinfo.table | wc -l)
if [ $beijingLine -lt 10000 ]; then
	echo "beijing play baseinfo lines is too small" && exit
fi

shanghaiLine=$(cat $Path/shanghai/dianping_detail.baseinfo.table | wc -l)
if [ $shanghaiLine -lt 10000 ]; then
	echo "shanghai play baseinfo lines is too small" && exit
fi

zunyiLine=$(cat $Path/zunyi/dianping_detail.baseinfo.table | wc -l)
if [ $shanghaiLine -lt 1000 ]; then
	echo "zunyi play baseinfo lines is too small" && exit

fi
echo "begin to index play service....."

# 只做基本信息的索引
#ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/build_food.sh baseinfo_play 1>tmp/play.std 2>tmp/play.err &"

# 做团购 + 基本信息的索引
ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/index_play.sh 1>>tmp/index.play 2>>tmp/index.play &"
