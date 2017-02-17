#!/bin/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-04-25 15:55
# * Filename	 : get_nuomi_cinema_movie.sh
# * Description	 : 合并
# * *****************************************************************************/


. ./bin/Tool.sh

find ./history/ -ctime +5 | xargs rm -f {}

#backupFile data/nuomi_movie_cinema
backupFile data/nuomi_movie_cm_relation
backupFile data/nuomi_movie_movie


/usr/bin/python bin/getNuomiCinemaMovie.py


if [ ! -f data/nuomi_movie_cm_relation ]; then
	sendReportMail "ServiceAppSpideErrorReport" "[Error]: get nuomi cinema movie info error"
fi



