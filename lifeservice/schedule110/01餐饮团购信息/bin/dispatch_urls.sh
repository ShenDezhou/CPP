#!/bin/bash
#coding=gb2312
# creater: liubing@sogou-inc.com
# date: 2015-12-17
# 分发待抓取的URL到Spider Client Host上去

. /search/zhangk/Fuwu/Tool/SSH/bin/HostConfig.sh
. ./bin/Tool.sh

# 待分发的url文件，url所在目录，url文件正则表达式
if [ $# -lt 2 ]; then
	echo "Usage: sh $0 urlFile|urlDir|urlFileRegex destDir [-public]"
	exit -1
fi
URLFile="$1";  DestDir="$2";


# 待分发的机器列表
HostArray=${NatHostList[*]}
if [ $# -ge 3 ];  then
	HostOpt="$3"
fi

if [ "$HostOpt" = "-public" ]; then
	HostArray=${PublicHostList[*]}
fi
HostArraySize=$(echo $HostArray | awk '{print NF}')


# 拷贝一个目录下的URL文件到各个Spider Client
function dispatch_urls_dir() {
	fileNum=0;
	for file in $(ls -S $URLFile); do
		fileNum=$((fileNum + 1));  hostIdx=$((fileNum % $HostArraySize))
		
		host=$(echo $HostArray | awk -v IDX=$hostIdx '{print $(IDX+1)}')
		srcFile="${URLFile}/${file}";  destPath="$DestDir/${file}"

		./expect/scp_file.exp "$srcFile" "$host:$destPath" $PASSWD
		LOG "scp $srcFile to $host:$destPath done"
	done
}


# 拷贝一个文件/正则表示的文件到各个Spider Client
function dispatch_urls_file() {
	fileNum=0;  srcPath=$(dirname $URLFile)
	for file in $(ls -S $URLFile); do
		fileNum=$((fileNum + 1));  hostIdx=$((fileNum % $HostArraySize))
		host=$(echo $HostArray | awk -v IDX=$hostIdx '{print $(IDX+1)}')
		srcFile="${file}";  destPath="$DestDir/$(basename $file)"

		./expect/scp_file.exp "$srcFile" "$host:$destPath" $PASSWD
		LOG "scp $srcFile to $host:$destPath done"
	done
}



if [ -d $URLFile ]; then
	dispatch_urls_dir
else
	dispatch_urls_file
fi

