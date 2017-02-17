
cd Source/
	#sh bin/build_dianping_play.sh  1>play.std 2>play.err
	sh bin/update_dianping_play.sh  1>play.std 2>play.err
cd -

cd Merger/
	sh bin/build_dianping_play.sh 1>play.std 2>play.err
	#sh bin/build_dianping_photoset.sh play 1>>play.std 2>>play.err
cd -

cd /fuwu/Merger/
	sh bin/correct_dianping_restaurant_area.sh play
cd -

# 拷贝数据到数据中心
cd /fuwu/DataCenter/
        sh update_play.sh -baseinfo
cd -
