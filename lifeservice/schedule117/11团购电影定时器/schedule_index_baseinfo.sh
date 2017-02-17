# 给基本信息做索引

cd /fuwu
	sh index_food.sh 1>logs/index_restaurant.log 2>&1 &

	sleep 300

	sh index_play.sh 1>logs/index_play.log 2>&1 &
cd -
