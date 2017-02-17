#!/bin/bash
#coding=gb2312

. ./bin/Tool.sh

# 处理休闲娱乐类的活动的数据
BaseDir="/search/zhangk/Fuwu/Spider"

# 今天玩什么网站的活动数据
JtwsmActivityData="${BaseDir}/Jtwsm/data/jtwsm_activity"

# 周末去哪儿网站的活动数据
WanzhoumoActivityData="${BaseDir}/Wanzhoumo/data/wanzhoumo_activity"




function mergeActivityData() {
	mergeActivityData="Input/activity_data"
	cat $JtwsmActivityData $WanzhoumoActivityData > $mergeActivityData

	LOG "merge activity data done.[$mergeActivityData]"
}



function main() {
	# 合并
	mergeActivityData



}

main

