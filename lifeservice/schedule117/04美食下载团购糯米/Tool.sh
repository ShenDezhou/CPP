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
