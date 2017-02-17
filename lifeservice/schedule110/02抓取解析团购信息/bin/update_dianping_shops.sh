#!/bin/bash
#coding=gb2312

. ./bin/Tool.sh

Numbers=10000
ScanUrls=Scan/data/scan_dianping_urls

function scan_get_shopurls() {
	# 将扫库得到的URL 过滤出shop urls
	LOG "begin to scan shop urls"
	/usr/bin/python bin/split_dianping_scan_urls.py $ScanUrls > Scan/data/shop_urls
	LOG "scan shop urls done"


	# 线上的所有URL
	cat data/*/onlineUrl/* | awk -F'\t' '{
		if (!($1 in urls)) {
			print $1;
			urls[$1]
		}
	}' > Scan/data/online_shop_urls

	# 过滤已经抓取过的Urls
	awk -F'\t' 'ARGIND==1 {
		onlineUrls[$1]
	} ARGIND==2 {
		if (!($1 in onlineUrls)) {
			print $1
		}
	}' Scan/data/online_shop_urls Scan/data/shop_urls > Scan/data/shop_urls.filte
	LOG "filte scan shop urls done"
}


input=Scan/data/shop_urls.filte
lines=$(cat $input | wc -l)
LOG "total $lines shop urls"

# 每相隔$Numbers做一次处理
#rm -f Scan/data/urlSplit/*
#split -l$Numbers $input Scan/data/urlSplit/shop_urls_


# 是否可以先扫一遍面包线获取数据？

# 第二遍就不再需要了


# 多线程抓取数据










