#!/bi/bash
#coding=gb2312
#/*******************************************************************************
# * Author	 : liubing@sogou-inc.com
# * Last modified : 2016-01-06 13:48
# * Filename	 : build_add_dianping_shortreview.sh
# * Description	 : 对于抓取处理完的短评数据 添加到最终baseinfo数据中
# * *****************************************************************************/

. ./bin/Tool.sh

BASEINFO_NF=29
DIANPING_DIR="/fuwu/Merger/Output"

function add_short_review_imp() {
	baseinfo=$1;  shortreview=$2;  output=$3;
	if [ ! -f $baseinfo -o ! -f $shortreview ]; then
		LOG "$baseinfo is not exist or $shortreview is not exist";  return
	fi

	NF=$(head -n1 $baseinfo | awk -F'\t' '{print NF}')
	Mode="insert"
	if [ $NF -eq $BASEINFO_NF ]; then
		Mode="replace"
	fi

	awk -F'\t' -v MODE=$Mode 'ARGIND == 1{
		url = $1; shortreview = $2;
		shortreviews[url] = shortreview
	} ARGIND == 2 {
		if (FNR == 1) {
			if (MODE == "replace") { print; next }
			if (MODE == "insert") { print $0 "\tshortDesc"; next }
		} else {
			url = $2;
			if (MODE == "replace") {
				$NF = shortreviews[url]
				line = $1;
				for (row=2; row<=NF; ++row) {
					line = line "\t" $row
				}
				print line
			} else if (MODE == "insert") {
				print $0 "\t" shortreviews[url]
			}
		}
	}' $shortreview $baseinfo > $output

	LOG "add/replace short review for [$baseinfo] done. [$output]"

}



function add_short_review() {
	type="restaurant"
	for city in $(ls $DIANPING_DIR/); do
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			LOG "movie/other dir, continue."
			continue
		fi

		baseinfo=$DIANPING_DIR/$city/$type/dianping_detail.baseinfo.table
		shortreview=$DIANPING_DIR/$city/$type/dianping_detail.shortreview.table
		output=$baseinfo.addshortdesc
		rm -f $output
		add_short_review_imp $baseinfo $shortreview $output

		if [ -f $output -a -f $baseinfo ]; then
			rm -f $baseinfo.bak;  mv $baseinfo $baseinfo.bak;  cp $output $baseinfo
		fi

		LOG "add short review for $city done."
	done
}

add_short_review
