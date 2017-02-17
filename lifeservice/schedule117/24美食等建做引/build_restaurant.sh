
cd Source/
	# 全部更新时调用,暂时不再需要
	#sh bin/build_dianping_restaurant.sh 1>restaurant.std 2>restaurant.err
	# 更新时调用
	sh bin/update_dianping_restaurant.sh 1>restaurant.std 2>restaurant.err
cd -

cd Merger/
	# 大众点评单抓的图集；暂时不再需要
	#sh bin/build_dianping_photoset.sh restaurant 1>restaurant.photo.std 2>restaurant.photo.err
	# 大众点评单抓的评论；暂时不再需要
	#sh bin/build_dianping_review.sh restaurant 1>restaurant.reiew.std 2>restaurant.review.err

	# 拷贝
	sh bin/dispatch_city_type.sh ./Input/ restaurant
	
	# 清理面包线错误的
	sh bin/correct_dianping_restaurant_area.sh restaurant
	
	# 更新外卖和优惠买单信息 (配置文件需要定期)
	sh bin/build_update_wai_ding.sh restaurant
cd -



# 拷贝数据到数据中心
cd /fuwu/DataCenter/
	sh update_restaurant.sh -baseinfo
cd -
