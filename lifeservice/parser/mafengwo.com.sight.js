var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;
var MAX_SCROLL_NUM = 5;

function main()
{
//    debugger;
    parseFunc();
   // sendMessage(result);
    window.setTimeout(clickFunc, 1 * 1000);
}

// scroll page
function scrollPage() {
    // scroll 
	var scrollHeight = $(document).scrollTop();
	var clientHeight = 400;
	var totalHeight = $(document).height();
    var current_scroll_num = 0;
	while (scrollHeight + clientHeight + 100 < totalHeight && ++current_scroll_num < MAX_SCROLL_NUM) {
			window.scrollBy(0, clientHeight);
	}
    // end scroll
}



function clickFunc(){
    // get every page's comments
    getAllComments();
    var nextPage = $('.wrapper > div#comment_pagebar > a.pg-next:contains("Next")');
    if (++current_nextpage_num <= MAX_NEXTPAGE_NUM && nextPage.length > 0) {
        nextPage[0].click();
		window.setTimeout(clickFunc, 1 * 1000);
		return;
    }
    sendMessage(result);
}




function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function normalCrumb(crumb) {
    // >日本>九州>那霸>那霸购物
    crumb = normalStr(crumb);
    crumb = crumb.replace(/^\>/, "");
    return crumb;
}

// 归一化需要保留换行符号的字符串
function normalMultiLineStr(str) {
    str = str.replace(/[\r\n]/g, '###').replace(/\s+/g,' ');
    str = str.replace(/######/g, '###');
    return str;
}


function takeFirst(valueArray) {
    for (var idx in valueArray) {
        if (valueArray[idx].length !== 0) {
            return valueArray[idx];
        }
    }
    return '';
}


function parseFunc(){
    // basic info
    var url = window.location.href;
    var breadcrumb = $.trim($('.top-info>div.crumb>div.item span.hd, .top-info > div.crumb > div.item>em').text());
    var title = $.trim($('.banner>a.photo div.s-title > h1').text());
    // cover photos
    coverPhoto = [];
    $('.banner>a.photo>div.cover>div.sc').each( function() {
        var photo = $(this).find('img').attr('src');
        if (photo && photo.length>0) {
            coverPhoto.push(photo);
        }
    });
    var address = $.trim($('.row.row-location.row-bg div.r-title >h2:contains("景点位置")').next().text());
    var descriptionArray = [];
    descriptionArray.push($.trim($('dl.intro div.simrows:first').text()));
    descriptionArray.push($.trim($('.wrapper > dl.intro div.simrows span').text()));
    var tel = $.trim($('.wrapper > dl.intro > dd > span.label:contains("电话")').next().text());
    var site = $.trim($('.wrapper > dl.intro > dd > span.label:contains("网址")').next().text());
    var traffic = $.trim($('.wrapper > dl.intro > dd > span.label:contains("交通")').next().text());
    var ticket = $.trim($('.wrapper > dl.intro > dd > span.label:contains("门票")').next().text());
    var openDate = $.trim($('.wrapper > dl.intro > dd > span.label:contains("开放时间")').next().text());
    var adviceDate = $.trim($('.wrapper > dl.intro > dd > span.label:contains("用时参考")').next().text());
    
    
    // push basic infos into result array
    pushResult('url', url);
    pushResult('title', title);
    pushResult('photo', coverPhoto.toString());
    pushResult('address', normalStr(address));
    pushResult('tel', tel);
    pushResult('site', site);
    pushResult('traffic', normalStr(traffic));
    pushResult('ticket', normalStr(ticket));
    pushResult('openDate', normalStr(openDate));
    pushResult('adviceDate', normalStr(adviceDate));
    pushResult('breadcrumb', normalCrumb(breadcrumb));
    pushResult('description', normalStr(takeFirst(descriptionArray)));

    // google poi (lat, lng)
    var poi = "";
    /* "POIADM":0,"lat":26.694156875211,"lng":127.87793143436,"zoom":15, */
    $('script').each(
        function() {
            var text = $(this).text();
            if (text.search('POIADM')>0 && text.search('zoom')>0) {
                matchGroup = text.match(/"lat":([0-9\.]+),"lng":([0-9\.]+),/);
                if (matchGroup && matchGroup.length > 2) {
                    poi = matchGroup[1] + ',' + matchGroup[2];
                }
            }
        }
    );
    pushResult('poi', poi);
    
    // comments tags
    commentTags = [];
    $('.wrapper > div.rev-tags li.filter-word').each(function(){
        tag = $.trim($(this).find('strong').text());
        tagWeight = $.trim($(this).find('em').text());
        if (tag.length>0 && tagWeight>0) {
            commentTags.push(tag + '@@@' + tagWeight);
        }
    });
    pushResult('commentTag', commentTags.toString());

}

function getAllComments() {
    // user comments
    $('.wrapper > ul.rev-lists > li.rev-item').each( function(){
        var userPhoto = $(this).find('div.avatar > a > img').attr('data-original');
        var userName = $.trim($(this).find('a.name').text());
        var commentStar = $(this).find('span.rating-star > span').attr('class').replace('star', '');
        var comment = $.trim($(this).find('p.rev-txt').text());
			
        var photoArray = [];
        var photos = $(this).find('div.rev-img > a').each( function() {
            var photo = $.trim($(this).find('img').attr('data-original'));
            photoArray.push(photo);
        });
        var commentDate = $(this).find('div.info > a.btn-comment').next().text();
        
        pushResult('userName', userName);
        pushResult('userPhoto', userPhoto);
        pushResult('commentStar', commentStar);
        pushResult('comment', normalStr(comment));
        pushResult('commentDate', normalStr(commentDate));
        pushResult('commentPhoto', photoArray.toString());
    });
}