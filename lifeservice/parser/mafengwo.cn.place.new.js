var timeoutPid=null;
var result=[];
var MAX_NEXTPAGE_NUM = 3;
var current_nextpage_num = 0;
var sep = '@@@';


function main()
{
    parseFunc();
	var commentSchema = 'userName' + sep + 'userPhoto' + sep + 'commentStar' + sep + 'comment' + sep + 'commentDate' + sep + 'commentPhoto';
	pushResult('comment_schema', commentSchema);
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
            

			comment = normalStr(comment);
			commentPhoto = photoArray.toString();
			var commentVal = userName + sep + userPhoto + sep + commentStar + sep + comment + sep + commentDate + sep + commentPhoto;
			pushResult('comment_value', commentVal);
			


			/*
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
			pushResult('commentStar', commentStar);
            pushResult('comment', normalStr(comment));
            pushResult('commentDate', commentDate);
            pushResult('commentPhoto', photoArray.toString());
			*/
        }
    );
    
}


function parseFunc(){
    // basic info
    var url = window.location.href;
    var enName = $.trim($('.m-box.m-content div.poi-info > div.bd > h3:contains("英文名称")').next().text());
    var title = $.trim($('.title.clearfix h1').text());
	var photo = $('.col-main div.pic-r > a > img').attr('src');
	var tel = $.trim($('.m-box.m-info > ul > li > i.icon-tel').parent().text());
	var rank = $.trim($('.col-main div.ranking > em').text()).replace('No.', '');
	var address = $.trim($('.m-box.m-content div.poi-info > div.bd > h3:contains("地址")').next().text());
	var address2 = $.trim($('.m-box.m-info > ul > li > i.icon-location').parent().text()).replace('地址：', '');
    var description = $.trim($('.m-box.m-content div.poi-info > div.bd > h3:contains("简介")').next().text());
    var breadcrumb = $.trim($('.crumb > div.item > em, div.drop > span.hd').text());	
    
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

	breadcrumb = normalCrumb(breadcrumb);
	address = normalStr(address);
	if (address.length <= 0) {
		address = address2;
	}
	description = normalStr(description);



	pushResult('url', url);
	var schema = 'title' + sep + 'enName' + sep + 'breadcrumb' + sep + 'rank' + sep + 'photo' + sep + 'tel' + sep + 'address';
	schema = schema + sep + 'poi' + sep + 'description';
	pushResult('baseInfo_schema', schema);
	var baseInfo = title + sep + enName + sep + breadcrumb + sep + rank + sep + photo + sep + tel + sep + address;
	baseInfo = baseInfo + sep + poi + sep + description;
	pushResult('baseInfo_value', baseInfo);
	


    
}


