Path=/fuwu/DataCenter/tuan_play
beijingLine=$( cat $Path/beijing/dianping_detail.tuan.table | wc -l)
if [ $beijingLine -lt 10000 ]; then
	echo "beijing play tuan lines is too small" && exit
fi

shanghaiLine=$(cat $Path/shanghai/dianping_detail.tuan.table | wc -l)
if [ $shanghaiLine -lt 10000 ]; then
	echo "shanghai play tuan lines is too small" && exit
fi

zunyiLine=$(cat $Path/zunyi/dianping_detail.tuan.table | wc -l)
if [ $shanghaiLine -lt 1000 ]; then
	echo "zunyi play tuan lines is too small" && exit
fi

echo "begin to index play tuan service....."

ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/build_food.sh tuan_play 1>tmp/play.tuan.std 2>tmp/play.tuan.err &"
