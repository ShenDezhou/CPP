
# 每天 4:00 开始做


# 下载百度糯米的团购数据
cd /search/zhangk/Fuwu/Source/Cooperation/Nuomi/
	sh bin/build_tuan.sh 1>>log/nuomi_tuan.std 2>>log/nuomi_tuan.err
cd -


# 与大众点评的店铺进行合并
cd /fuwu/Source/Cooperation/Tuan/

	sh bin/build_nuomi_play_tuan.sh 1>logs/nuomi_play_tuan.std 2>&1 &
	sh bin/build_nuomi_restaurant_tuan.sh 1>logs/nuomi_restaurant_tuan.std 2>&1 &

cd -


