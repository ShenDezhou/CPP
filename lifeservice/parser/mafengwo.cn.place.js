var timeoutPid=null;
var result=[];
var MAX_NEXTPAGE_NUM = 3;
var current_nextpage_num = 0;

function main()
{
    parseFunc();
    window.setTimeout(clickFunc, 1*1000);
}

// max get 5 next pages 
function clickFunc(){
    // get every page's comments
    getAllComments();
    var nextPage = $('#comment_pagebar .ti._j_pageitem:contains("Next")');
    if (nextPage.length > 0 && ++current_nextpage_num <= MAX_NEXTPAGE_NUM) {
        nextPage[0].click();
		window.setTimeout(clickFunc, 1*1000);
		return;
    }
    sendMessage(result);
}

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalCrumb(crumb) {
    // >日本>九州>那霸>那霸购物
    crumb = normalStr(crumb);
    crumb = crumb.replace(/^\>/, "");
    return crumb;
}


function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function takeFirst(valueArray) {
    for (var idx in valueArray) {
        if (valueArray[idx].length !== 0) {
            return valueArray[idx];
        }
    }
    return '';
}


function getAllComments() {
    // user comments
    $('.comment-item').each(
        function(){
            var userPhoto = $(this).find('.user-bar > .user-avatar > a > img').attr('src');
            var userName = $(this).find('.comment-info > div.info > a.user-name').text();
            var userStar = $(this).find('.user-bar > .user-level > a').attr('src');
            
			var commentStar = $.trim($(this).find('.comment-info>div.info>span.rank-star>span').attr('class')).replace('star', '');
			var commentDetailStar = $(this).find('.title.cf span.sblockline').text();
            var comment = $.trim($(this).find('.comment-info > div.info > div.c-content > p').text());

            var photoArray = [];
            var photos = $(this).find('.comment-info > div.info > div.c-content > div.pic > span').each(
                function() {
                    var photo = $.trim($(this).find('a > img').attr('src'));
                    photoArray.push(photo);
                }
            );
            var commentDate = $.trim($(this).find('.comment-info > div.info > div.add-reply span.time').text());
            
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
			pushResult('commentStar', commentStar);
            pushResult('comment', normalStr(comment));
            pushResult('commentDate', normalStr(commentDate));
            pushResult('commentPhoto', photoArray.toString());
        }
    );
    
}


function parseFunc(){
    // basic info
    var url = window.location.href;
    var enName = $.trim($('.m-box.m-content div.poi-info > div.bd > h3:contains("英文名称")').next().text());
    var cnName = $.trim($('.title.clearfix h1').text());
    var localName = $.trim($('div.poi-info > div.bd > h3:contains("当地名称")').next().text());
	var photo = $('.col-main div.pic-r > a > img').attr('src');
	//var tel = $.trim($('.m-box.m-info > ul > li > i.icon-tel').parent().text());
	var telList = [];
    telList.push( $.trim($('.m-box.m-info > ul > li > i.icon-tel').parent().text()));
    telList.push( $.trim($('div.poi-info h3:contains("电话")').next().text()));
	var score = $.trim($('div.score > span.score-info>em').text());
	var rank = $.trim($('.col-main div.ranking > em').text()).replace('No.', '');
	var addressList = [];
	addressList.push($.trim($('.m-box.m-content div.poi-info > div.bd > h3:contains("地址")').next().text()));
	addressList.push($.trim($('.m-box.m-info > ul > li > i.icon-location').parent().text()).replace('地址：', ''));
    var description = $.trim($('.m-box.m-content div.poi-info > div.bd > h3:contains("简介")').next().text());
    var breadcrumb = $.trim($('.crumb > div.item > em, div.drop > span.hd').text());	
    var site = $.trim($('div.poi-info h3:contains("网址")').next().text());
    var traffic = $.trim($('div.poi-info h3:contains("交通")').next().text());
    var openDate = $.trim($('div.poi-info h3:contains("开放时间")').next().text());
    var avgConsume = $.trim($('div.poi-info h3:contains("人均消费")').next().text().replace('人均：', ''));
    
	pushResult('url', url);
	pushResult('title', cnName);
	pushResult('enName', enName);
	pushResult('localName', localName);
	pushResult('breadcrumb', normalCrumb(breadcrumb));
    pushResult('photo', photo);
	pushResult('tel', normalStr(takeFirst(telList)));
    pushResult('address', normalStr(takeFirst(addressList)));
    pushResult('score', score);
    pushResult('rank', rank);
    pushResult('site', site);
    pushResult('traffic', normalStr(traffic));
    pushResult('openDate', normalStr(openDate));
    pushResult('avgConsume', normalStr(avgConsume));
    pushResult('description', normalStr(description));

    // google poi
    var poi = "";
    $('script').each(
        function() {
            var text = $(this).text();
            if (text.search('lat')>0 && text.search('lng')>0) {
                // "lat":26.214668,"lng":127.683184,
                matchGroup = text.match(/"lat":([0-9\.]+),"lng":([0-9\.]+),/);
                if (matchGroup && matchGroup.length > 2) {
                    poi = matchGroup[1] + ',' + matchGroup[2];
                }
            }
        }
    );
    pushResult('poi', poi);
    
    
}


