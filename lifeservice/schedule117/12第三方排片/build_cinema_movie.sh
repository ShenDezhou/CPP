
echo $(date "+%Y-%m-%d %H:%M:%S")" begin"

# 顺道把糯米的数据也接入一遍
cd Nuomi
	sh bin/build_tuan.sh 1>std 2>err &
cd -

cd Dianping
	sh build.sh 1>std 2>err &
cd -

cd Maoyan
	sh build.sh 1>std 2>err &
cd -

cd Wepiao
	sh build.sh 1>std 2>err &
cd -

cd KouMovie
	sh build.sh 1>std 2>err &
cd -


wait

echo $(date "+%Y-%m-%d %H:%M:%S")" all done!"
