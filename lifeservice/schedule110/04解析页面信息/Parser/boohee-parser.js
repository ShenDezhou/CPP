var cheerio = require("cheerio");
var fs = require("fs");
var common = require("../lib/common.js");

var pushResult = common.pushResult;
var normalStr = common.normalStr;
var normalMultiLineStr = common.normalMultiLineStr;
var takeFirst = common.takeFirst;
var trim = common.trim;

function parseComments(url, content) {
	var $ = cheerio.load(content);

            // user comments
            $('.comment_single').each(function() {
                var userPhoto = $(this).find('.userimg > a > img').attr('src');
                var userName = $(this).find('.ellipsis > a').text();
                var commentStar = $(this).find('.title.cf span.starlist > span').attr('style');
                var commentDetailStar = $(this).find('.title.cf span.sblockline').text();
                var comment = trim($(this).find('.main_con > span.heightbox').text());

                var photoArray = [];
                var photos = $(this).find('.comment_piclist.cf > a').each(function() {
                    var photo = trim($(this).find('img').attr('src'));
                    photoArray.push(photo);
                });
                var commentDate = $(this).find('.time_line > em').text();

                pushResult('userName', userName);
                pushResult('userPhoto', userPhoto);
                pushResult('commentStar', normalCtripStar(commentStar));
                pushResult('commentDetailStar', normalStr(commentDetailStar));
                pushResult('comment', normalStr(comment));
                pushResult('commentDate', normalStr(commentDate));
                if (photoArray.length > 0) {
                    pushResult('commentPhoto', photoArray.toString());
                }
            });
}

function normalCtripStar(ctripStar) {
    if (!ctripStar) {
        return '';
    }
    matchGroup = ctripStar.match(/:([0-9]+)%/);
    if (matchGroup && matchGroup.length > 1) {
        ctripStar = matchGroup[1];
    }
    if (ctripStar === '') {
        return 0;
    }
    return parseInt((parseInt(ctripStar) + 10) / 20).toString();
}

function normalCrumb(crumb) {
    if (!crumb) {
        return '';
    }
    // 旅游攻略社区>目的地>日本>冲绳>那霸购物>
    crumb = normalStr(crumb);
    crumb = crumb.replace(/旅游攻略社区\>目的地\>/, '');
    crumb = crumb.replace(/\>$/, '');
    return crumb;
}

// 归一化分数
function normalScore(score) {
    if (score === '') {
        return 0;
    }
    return parseInt((score + 10) / 20);
}

function getGooglePoi(mapUri) {
    if (!mapUri) {
        return '';
    }
    // 这是
    //&center=26.2142925262451,127.682273864746&
    matchGroup = mapUri.match(/&center=([0-9\.,]+)&/);
    if (matchGroup && matchGroup.length > 1) {
        return matchGroup[1];
    }

    return mapUri;
}

function parseBaseInfo(url, content) {
	var $ = cheerio.load(content);
	   
            var breadcrumb = trim($('div.crumb').text());

	    var alias = trim($('div.size14.round5 span:contains("别名")').parent().text());
	    if (alias) {
		alias = alias.replace("别名：", "");
	    }
	    var hot = trim($('div.size14.round5 span:contains("热量")').next().text());
	    var comment = trim($('div.size14.round5 b:contains("评价")').parent().text());
	    if (comment) {
		comment = comment.replace(/^评价：/, "");
	    }
	    if (comment.length === 0) {
		comment = trim($('div.size14.round5 > p').last().text());
	    }

	    pushResult('url', url)
	    pushResult('breadcrumb', breadcrumb);
	    pushResult('alias', alias);
	    pushResult('hot', hot);
	    pushResult('comment', comment);
}

// 根据url分发给不同的解析函数
parse = function(url, content) {
    //console.log(url);
    parseBaseInfo(url, content);
}

// 对外开放接口
exports.parse = parse;
