. ./Tool.sh

LOG "begin to get movie cooperation data..."
# 服务搜索，电影类 第三方影院排片信息接入
cd /search/zhangk/Fuwu/Source/Cooperation/
	sh build_download_cooperation_data.sh 1>>std 2>>err &
cd -


LOG "begin to get movie mtime data..."
# 服务搜索 电影类 时光网电影基本信息抓取
cd /fuwu/Spider/Mtime/
	/usr/bin/python bin/getMtimeMovie.py 1>>std 2>>err &
cd -

wait

LOG "get movie datas done"


LOG "begin to index movie info..."
# 在上面两个抓取任务完毕后执行数据制作
cd /fuwu/
        sh build_movie.sh 1>>logs/movie.std 2>>logs/movie.err
cd -
LOG "index movie info done."


