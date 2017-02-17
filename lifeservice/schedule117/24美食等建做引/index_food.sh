

Path=/search/odin/fuwu/DataCenter/baseinfo_restaurant
beijingLine=$( cat $Path/beijing/dianping_detail.baseinfo.table | wc -l)
if [ $beijingLine -lt 10000 ]; then
	echo "beijing restaurant baseinfo lines is too small" && exit
fi

shanghaiLine=$(cat $Path/shanghai/dianping_detail.baseinfo.table | wc -l)
if [ $shanghaiLine -lt 10000 ]; then
	echo "shanghai restaurant baseinfo lines is too small" && exit
fi

zunyiLine=$(cat $Path/zunyi/dianping_detail.baseinfo.table | wc -l)
if [ $shanghaiLine -lt 1000 ]; then
	echo "zunyi restaurant baseinfo lines is too small" && exit
fi


echo "begin to index restaurant service....."

# 只做基本信息的索引
#ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/build_food.sh baseinfo_restaurant 1>tmp/food.std 2>tmp/food.err &"


# 做团购 + 基本信息的索引
ssh 10.134.96.110 "cd /search/kangq/fuwu; nohup sh -x bin/index_restaurant.sh 1>>tmp/index.restaurant 2>>tmp/index.restaurant &"

