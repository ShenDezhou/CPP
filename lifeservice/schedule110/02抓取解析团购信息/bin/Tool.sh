#!/bin/bash
#coding=gb2312

# 当前时间，用于记录日志等
function now() {
	echo $(date "+%Y-%m-%d %H:%M:%S")
}

# 当前时间，用于记录日志等
function nowStr() {
	echo $(date "+%Y%m%d%H%M")
}

# 当前时间，用于记录日志等
function todayStr() {
	echo $(date "+%Y%m%d")
}

# 当前时间，用于记录日志等
function today() {
	echo $(date "+%Y-%m-%d")
}

# 打印日志
function LOG() {
    	now=`date +"%Y-%m-%d %H:%M:%S"`
	echo $now" "$1
}

function INFO() {
    	now=`date +"%Y-%m-%d %H:%M:%S"`
	echo $now" [INFO] "$1
}

function ERROR() {
    	now=`date +"%Y-%m-%d %H:%M:%S"`
	echo $now" [ERROR] "$1
}


# 判断文件是否是今天生成的
function isFileTodayCreate() {
	file=$1
	fileCreateDate=$(ls -l --time-style=long-iso $file | awk '{print $6}')
	today=$(today)
	if [ "$fileCreateDate" = "$today" ]; then
		echo "true"
	else
		echo "false"
	fi
}


# 判断文件是否够大
function isFileSizeEnough() {
	file=$1;  sizeThreshold=$2
	fileCreateDate=$(ls -l --time-style=long-iso $file | awk '{print $5}')
	if [ $fileCreateDate -ge $sizeThreshold ]; then
		echo "true"
	else
		echo "false"
	fi
}




# 备份文件
function backupFile() {
    today=$(date +%Y%m%d)
    srcFile=$1;
    if [ ! -f $srcFile ]; then
         echo "$srcFile is not exist;"
        return 0
    fi
    destFile="./history/"$(basename $srcFile)".$today"
    rm -f $destFile; mv -f $srcFile $destFile
    echo "baskup $srcFile into $destFile done."
}


# 将文件转成一行
function trans2line() {
    input=$1
    awk 'BEGIN{ line = ""} {
        gsub(/\n\r/, "", $0)
        line = line "" $0
    } END {
        print line
    }' $input
}


# 发送报警邮件
# arg1: report project name
# arg2: report message
function sendReportMail() {
	reportProj="$1";  message="$2"
	echo -e "<html><body><p><h1><font color=\"red\">$message</font></h1></p></body></html>" > /tmp/report.html
    	/usr/bin/sogou-mds-syncfile -m $reportProj -l /tmp/report.html
}




# 打印一个目录下各个文件的状态信息
function getFileInfoOfDirectory() {
        directory=$1;
        listInfo=$(ls -l $directory | awk '{print $0"<br>"}')
        echo  $listInfo
}





