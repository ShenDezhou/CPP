
# 每天 6:00 开始做

cd /fuwu/Spider/Huatuojiadao
	# 华佗驾到的团购数据
	sh bin/get_huatuojiadao_tuan.sh 1>logs/huatuojiadao.std 2>&1
	# 华夏良子的团购数据
	sh bin/get_liangzi_tuan.sh 1>logs/liangzi.std 2>&1
cd -

