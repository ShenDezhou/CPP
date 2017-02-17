#!/bin/bash
#coding=gb2312


# 将团购信息转成线上做索引使用的格式
function format() {
	input=$1;  output=$2;
	awk -F'\t' 'BEGIN {
		print "id\tresid\tsite\ttype\turl\ttitle\tphoto\tprice\tvalue\tsell\tdeadline"
	}
	function normImage(image) {
		gsub(/\.jpg.*$/, ".jpg", image)
		return image
	}
	{
		if (NF != 10) {
			next
		}
		id=$1;  title=$2;  detail=$3; image=$4;  url=$5;
		value=$6;  price=$7;  sell=$8;  deadline=$9;  resid=$10;
		
		# 过滤无效的团购ID
		if (!(id~/^[\-0-9]+$/)) {
			next
		}
		
		site = "大众点评";  type = "团"; 
		image = normImage(image);  resid = "dianping_" resid

		line = id "\t" resid "\t" site "\t" type "\t" url "\t" title "\t" image
		line = line "\t" price "\t" value "\t" sell "\t" deadline
		print line
	}' $input > $output
}
