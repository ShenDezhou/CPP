var cheerio = require("cheerio");
var fs = require("fs");
var common = require("../lib/common.js");

var pushResult = common.pushResult;
var normalStr = common.normalStr;
var normalMultiLineStr = common.normalMultiLineStr;
var takeFirst = common.takeFirst;
var trim = common.trim;

function normalStar(star) {
	if (!star) {
        	return '';
   	 }
    	star = normalStr(star);
    	star = star.replace('user-rank-rst urr-rank', '');
    	star = star.replace('sml-rank-stars sml-str', '');
    	star = star.replace('item-rank-rst irr-star', '');
    	if (star && star.length > 0){
        	return (parseFloat(star)/10).toString();
   	 }
    	return star;
}

function parseComments(url, content){
	var $ = cheerio.load(content);
	var title = trim($('h1 > a').text());
	url = url.replace(/\/review.*/, "")

    	// user comments
    	$("div.comment-list li").each(function(){
            	var userName = trim($(this).find('div.pic p.name').text());
            	var userPhoto = $(this).find('div.pic img').attr('src');
		var userStar = normalStar(trim($(this).find('div.pic p.contribution > span').attr('class')));
			
            	var commentStar = normalStar($(this).find('div.content div.user-info>span').attr('class'));
            	var cTaseStar = $(this).find('div.comment-rst span:contains("口味")').text().replace(/[^0-9]/g, '');
            	var cConditionStar = $(this).find('div.comment-rst span:contains("环境")').text().replace(/[^0-9]/g, '');
            	var cServiceStar = $(this).find('div.comment-rst span:contains("服务")').text().replace(/[^0-9]/g, '');



		var comment = normalStr($(this).find('div.J_brief-cont').text()).replace(/收起$/, '');
		// 图集
            	var photoArray = [];
            	$(this).find('div.shop-photo img').each(function() {
                    var photo = trim($(this).attr('src'));
                    photoArray.push(photo);
                });

            	// 发表日期，点赞数，回复数
            	var commentDate = trim($(this).find('div.misc-info span.time').text());
            	var commentZanCnt = trim($(this).find('span.heart-num').text().replace(/[赞\(\)]/g, ''));
            	var commentReplyCnt = trim($(this).find('div.misc-info span.J_rtl').text());


		if (comment.length > 3) {
			userInfo = userName + '\t' + userPhoto + '\t' + userStar;
			commentStarInfo = commentStar + '\t' + cTaseStar + '\t' + cConditionStar + '\t' + cServiceStar;
			commentInfo = comment + '\t' + photoArray.toString() + '\t' + commentDate + '\t' + commentZanCnt + '\t' + commentReplyCnt;
			info = title + '\t' + userInfo + '\t' + commentStarInfo + '\t' + commentInfo
			pushResult(url, info);
		}

			
		/*
            	//pushResult('userName', userName);
            	//pushResult('userPhoto', userPhoto);
		//pushResult('userStar', userStar);
            	pushResult('commentStar', commentStar);
            	pushResult('cTasteStar', cTaseStar);
            	pushResult('cConditionStar', cConditionStar);
            	pushResult('cServiceStar', cServiceStar);
            	pushResult('comment', normalStr(comment));
            	//pushResult('commentPhoto', photoArray.join());
            	pushResult('commentDate', commentDate);
            	pushResult('commentZanCnt', commentZanCnt);
            	pushResult('commentReplyCnt', commentReplyCnt);
		*/
    });
    
}


function parseBaseInfo(url, content) {
	var $ = cheerio.load(content);
	
	var payUrl = $('section#sg-pay dd>a').attr('href');
	if (payUrl) {
		payUrl = "http://m.jtwsm.cn" + payUrl;
	}
	pushResult('url', url);
	pushResult('payUrl', payUrl);
}

// 根据url分发给不同的解析函数
parse = function(url, content) {
    //console.log(url);
    parseComments(url, content);
}

// 对外开放接口
exports.parse = parse;
