#!/bin/bash
#coding=gb2312

# Creator : liubing@sogou-inc.com
# Date : 2015-12-22

# /search/zhangk/Fuwu/Spider/Dianping/Result/restaurant/review/zhaoyuan.review.10.142.86.132

#抓取默认评论前三页，依次按照：
#1. 字数＜50，有赞数且赞数最多的
#2. 字数＜50，有用户等级且等级最高的
#3. 字数＜50，最新一条评论

. ./bin/Tool.sh

if [ $# -lt 1 ]; then
	echo "sh $0 [restaurant|play]"; exit -1;
fi
Type="$1"



# <url  id>
UrlIDMapConf="/search/zhangk/Fuwu/Source/conf/restaurant_url_id_conf"
# <url photoset>
SpideReviewPath="/search/zhangk/Fuwu/Spider/Dianping/Result/restaurant/review/"
# <id  resid  photoset>
OnlineReviewPath="/search/zhangk/Fuwu/Merger/Output"

MergePath="/search/zhangk/Fuwu/Merger/data/$Type/review"
if [ ! -d $MergePath ]; then
	mkdir -p $MergePath
fi

if [ "$Type" = "play" ]; then
	UrlIDMapConf="/search/zhangk/Fuwu/Source/conf/play_url_id_conf"
	SpideReviewPath="/search/zhangk/Fuwu/Spider/Dianping/Result/play/review/"
fi




function merge_review_imp() {
	onlineReview=$1;  spideReview=$2; mergeReview=$3;
	awk -F'\t' 'BEGIN {
		maxCommentID = -1;
		lastid = ""; lastphotoset =""; idx = 0;
	}
	# 注意这里返回的字符串第一个字符是\t
	function normReview(photos) {
		photoset = ""
		len = split(photos, array, ",")
		for(i=1; i<=len; i++) {
			gsub(/@@@.*/, "", array[i])
			photoset = photoset "," array[i]
		}
		return substr(photoset, 2)
	}
	# 加载 url-id映射
	ARGIND == 1 {
		url = $1; id = $2
		urlidMap[url] = id;
	} 
	# 线上评论
	ARGIND == 2 {
		if ($1 == "psetid") { next }
		if ($1 == "commentid") { print; next; }
		id = $1;  comment = $12
		if (id > maxCommentID) { maxCommentID = id }
		if (length(comment) == 0) { next }
		print 
	} 
	# 线下抓取的评论
	ARGIND == 3 {
		url = $1;
		if (!(url in urlidMap)) { next }
		resid = urlidMap[url]	
		# 日期参数
		date = $12
		if (length(date) >= 6) {
			$12 = substr(date, 0, 5)
		}

		line = (++maxCommentID) "\t" resid "\t" url
		for (row=2; row<=NF-1; ++row) {
			line = line "\t" $row
		}
		print line
	}' $UrlIDMapConf $onlineReview $spideReview > $mergeReview

}

function merge_review() {
	for city in $(ls $OnlineReviewPath/); do
		onlineReviewFile="$OnlineReviewPath/$city/$Type/dianping_detail.comment.table.old"
		
		ls -l -S $SpideReviewPath/${city}*review*
		if [ $? -ne 0 ]; then
			continue
		fi
		spideReviewFile=$(ls -l -S $SpideReviewPath/${city}*review* | head -n1 | awk '{print $NF}')

		if [ ! -f $onlineReviewFile -o ! -f $spideReviewFile ]; then
			LOG "[Warn]: $onlineReviewFile or $spideReviewFile is not exist!"
			continue
		fi
		mergeReview=$MergePath/${city}_review

		sort -u $spideReviewFile | iconv -futf8 -tgbk -c > $spideReviewFile.sort
		merge_review_imp $onlineReviewFile $spideReviewFile.sort $mergeReview
		rm -f $spideReviewFile.sort
		LOG "merge review of $city done. [$mergeReview]"
		# 计算一句话短评
		shortReview=$MergePath/${city}_shortreview
		python bin/ReviewTool.py -short-comment $spideReviewFile $shortReview
		onlineShortReview="$OnlineReviewPath/$city/$Type/dianping_detail.shortreview.table"
		awk -F'\t' 'BEGIN {
			print "id\turl\tshortreview"
		} ARGIND==1 {
			url = $1;  id = $2;  urlidMap[url] = id;
		} ARGIND==2 {
			url = $1; shortreview = $2;
			if (!(url in urlidMap)) { next }
			print urlidMap[url] "\t" url "\t" shortreview
		}' $UrlIDMapConf $shortReview > $onlineShortReview
		LOG "merge short-review of $city done. [$shortReview]"
:<<EOF		
EOF
	done
}


function dispatch_mergereview() {
	for city in $(ls $OnlineReviewPath/); do
		onlineReviewFile="$OnlineReviewPath/$city/$Type/dianping_detail.comment.table"
		mergeReview=$MergePath/${city}_review
		if [ ! -f $onlineReviewFile -o ! -f $mergeReview ]; then
			LOG "[Warn]: $onlineReviewFile or $mergeReview is not exist!"
			continue
		fi
		if [ ! -f $onlineReviewFile.old ]; then
			mv $onlineReviewFile $onlineReviewFile.old
		else
			rm -f $onlineReviewFile
		fi
		cp $mergeReview $onlineReviewFile

		LOG "dispatch merge review of $city done. [$mergeReview]"

		onlineShortReview="$OnlineReviewPath/$city/$Type/dianping_detail.shortreview.table"
		shortReview=$MergePath/${city}_shortreview
		if [ -f $shortReview ]; then
			rm -f $onlineShortReview;  cp $shortReview $onlineShortReview;
		fi
	done
}


function main() {
	merge_review

	dispatch_mergereview

	sh bin/build_add_dianping_shortreview.sh
}

#main

echo "ignore this step"
