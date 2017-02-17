Path=/fuwu/DataCenter/tuan_restaurant
beijingLine=$( cat $Path/beijing/dianping_detail.tuan.table | wc -l)
if [ $beijingLine -lt 10000 ]; then
	echo "beijing restaurant tuan lines is too small" && exit
fi

shanghaiLine=$(cat $Path/shanghai/dianping_detail.tuan.table | wc -l)
if [ $shanghaiLine -lt 10000 ]; then
	echo "shanghai restaurant tuan lines is too small" && exit
fi

zunyiLine=$(cat $Path/zunyi/dianping_detail.tuan.table | wc -l)
if [ $shanghaiLine -lt 1000 ]; then
	echo "zunyi restaurant tuan lines is too small" && exit
fi

echo "begin to index restaurant tuan service....."

ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/build_food.sh tuan_restaurant 1>tmp/food.tuan.std 2>tmp/food.tuan.err &"
