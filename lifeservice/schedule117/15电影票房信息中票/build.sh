#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-04 15:32
# * Filename	 : build.sh
# * Description	 : 
# * *****************************************************************************/
#!/bin/bash
#coding=gb2312

cbohour="tmp/cbohoursogou.xml"
allbox="data/cbo_allbox"
movieNameMap="conf/movie_name_map_conf"


. ./bin/XmlTool.sh
. ./bin/Tool.sh


# parse xml files
function parse_xml_file() {
	input=$1;  output=$2
	awk -F'\t' '{
		if ($0!~/.*>.*<\//) { next }
		tag = $0
		sub(/.*<\//, "", tag)
		sub(/>.*$/, "", tag)

		val = $0
		sub(/<\/.*$/, "", val)
		sub(/^[^<]*<[^>]*>/, "", val)
		sub(/<!\[CDATA\[/, "", val)
		sub(/\]\]>/, "", val)

		gsub(/(^[\s]*|[\s]*$)/, "", val)
		if (length(tag)>0 && length(val)>0) {
			print tag"\t"val
		}
	}' $input > $output
	LOG "parse xml file [$input] to K-V file [$output]."
}


function download_and_parse_xml() {
	url="http://www.cbooo.cn/PcCboXml/cbohoursogou.xml"
	wget $url -O $cbohour
	LOG "download cbohoursogou.xml done [$cbohour]"

	parse_xml_file $cbohour $cbohour.parse
}

function transfer_to_line() {
	input=$1;  output=$2;
	awk -F'\t' ' BEGIN {
		name=""; alias=""; time=""; allbox=""; daynumbers=""
	}
	function printItem() {
		if (name == "") { return }
		if (daynumbers > 60) { return }
		print name "\t" alias "\t" time "\t" allbox
	}
	{
		if (NF != 2) { next }
		key = $1;  value = $2;
		if (key == "name") { 
			printItem()
			name = value
		}
		if (key == "synonym") { alias = value }
		if (key == "time") { time = value }
		if (key == "allbox") { allbox = value }
		if (key == "daynumbers") { 
			daynumbers = value 
			gsub(/Ìì$/, "", daynumbers)
		}
	} END {
		printItem()
	}' $input > $output
	LOG "parse allbox for [$input] to [$output]."

	awk -F'\t' 'ARGIND==1 {
		if (NF < 2) {
			continue
		}
		title=$1; alias=$2
		titleMap[title] = alias
	} ARGIND==2 {
		title = $1
		if (title in titleMap) {
			$1=titleMap[title]; $2=titleMap[title];
			line = $1
			for(i=2; i<=NF; i++) {
				line = line "\t" $i
			}
			print line
		}
	}' $movieNameMap $output >> $output

}


function main() {
	download_and_parse_xml

	transfer_to_line $cbohour.parse $allbox

}


main

