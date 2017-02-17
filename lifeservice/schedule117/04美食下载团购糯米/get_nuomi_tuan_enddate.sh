#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-06-08 19:55
# * Filename	 : get_nuomi_tuan_enddate.sh
# * Description	 : 计算糯米网团购数据的实际到期时间
# * *****************************************************************************/
. ./bin/Tool.sh

LOG "begin to check expired...."
for expiredFile in $(ls expiredtuan/*_expired_tuan); do
	fileSize=$(ls -l $expiredFile | awk '{print $5}' )
	if [ $fileSize -eq 0 ]; then
		continue
	fi

	output=$expiredFile.result

	if [ -f $output ]; then
		continue
	fi

	/usr/bin/python bin/getTuanExpired.py $expiredFile > $output

done
LOG "check all expired done."
