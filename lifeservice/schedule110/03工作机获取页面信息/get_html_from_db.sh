#!/bin/bash
#coding=gb2312

if [ $# -lt 2 ]; then
	urlFile=$1;  htmlFile=$2
	echo "[Usage]: sh $0 urlFile htmlFile" && exit -1
fi


urlFile=$1;  htmlFile=$2

# 1. 扫库得到xpage
echo "begin to get page..."
xpageFile=${htmlFile}_page;
sh bin/get_page.sh $urlFile $xpageFile


# 2. 解析出HTML页面
echo "begin to parse html..."
/usr/bin/python  bin/get_html_from_xpage.py $xpageFile > $htmlFile.raw

sh bin/convert_html_to_line.sh $htmlFile.raw $htmlFile

rm -f $xpageFile
rm -f $htmlFile.raw
echo "done."
