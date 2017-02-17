
echo $(date "+%Y-%m-%d %H:%M:%S")" begin"


cd Dianping
	sh build.sh 1>>std 2>err &
cd -

cd Maoyan
	sh build.sh 1>>std 2>err &
cd -

cd Wepiao
	sh build.sh 1>>std 2>err &
cd -

cd KouMovie
	sh build.sh 1>>std 2>err &
cd -

# 糯米网需要近三个小时才能抓取完毕
#cd /fuwu/Spider/Nuomi/
#	sh bin/get_nuomi_cinema_movie.sh 1>movie.std 2>movie.err &
#cd 

wait

echo $(date "+%Y-%m-%d %H:%M:%S")" all done!"
