#!/bin/bash
#coding=gb2312
# 将抓取的html数据，转成一行url 一行content的格式


function convert_html_to_line() {
	input=$1; output=$2;
	awk -F'\t' 'BEGIN {
		url=""; content=""
	}
	function trim(line) {
		gsub(/(^[ \s]+|[ \s]+$)/, "", line)
		gsub(/\r/, "", line)
		return line
	}
	{
		if ($0~/^http/) {
			if (content != "") {
				print content
				content = ""
			}
			url = $0
			print url
		} else {
			content = content "" trim($0)
		}
	} END {
		if (content != "") {
			print content
		}
	}' $input > $output
	echo "convert done."
}

convert_html_to_line $1 $2
