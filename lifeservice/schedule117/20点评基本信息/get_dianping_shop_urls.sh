

# 扫库获取点评shop urls
cd /search/odin/PageDB/GetUrl
	
	sh -x bin/get_pattern_url.sh conf/pattern_dianping_shop_conf 1>log/dianping.shop.std 2>log/dianping.shop.err

cd -


# 扫库获取点评店铺的crumb
sh -x bin/get_dianping_shop_crumb.sh 1>log/dianping.shop.crumb.std 2>log/dianping.shop.crumb.err


