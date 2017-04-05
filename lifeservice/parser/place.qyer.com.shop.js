var timeoutPid=null;
var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

function main()
{
    parseFunc();
    window.setTimeout(clickFunc,500);
}

function clickFunc(){
    // get every page's comments
    getAllComments();
    var nextPage = $('.ui_page_item.ui_page_next');
    if (++current_nextpage_num<=MAX_NEXTPAGE_NUM && nextPage && nextPage.length > 0) {
        nextPage[0].click();
		window.setTimeout(clickFunc, 1000);
		return;
    }
    sendMessage(result);
}

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function getAllComments() {
    // user comments
    $('#commentlist > li').each(
        function(){
            var userPhoto = $(this).find('.largeavatar > img').attr('src');
            var userName = $(this).find('.largeavatar > span').text();
			var commentStar = $(this).find('.comment.clearfix span.poiDet-stars > em.singlestar.full').length;
			var commentDetailStar = $(this).find('.title.cf span.sblockline').text();
            var comment = $.trim($(this).find('.comment.clearfix > .content').text());

            var photoArray = [];
            var photos = $(this).find('.comment.clearfix > .album > span').each(
                function() {
                    var photo = $.trim($(this).find('a > img').attr('datalarge'));
                    photoArray.push(photo);
                }
            );
            var commentDate = $.trim($(this).find('.comment.clearfix a.date').text());
            
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
			pushResult('commentStar', commentStar.toString());
            pushResult('comment', normalStr(comment));
            pushResult('commentDate', commentDate);
            pushResult('commentPhoto', photoArray.toString());
        }
    );    
}



function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function normalMultiLineStr(str) {
    str = str.replace(/[\r\n]/g, '###').replace(/\s+/g,' ');
    str = str.replace(/######/g, '###');
    return str;
}


function normalBreadCrumb(breadCrumb) {
    if (!breadCrumb) {
        return '';
    }
    breadCrumb = normalStr(breadCrumb);
    breadCrumb = breadCrumb.replace(/\>目的地\>/g, "");
    return breadCrumb; 
}

// 归一化分数
function normalScore(score) {
    if (score === '') {
        return 0;
    }
    return parseInt((parseInt(score) +1)/2).toString();    
}

function getGooglePoi(mapUri) {
    if (!mapUri) {
        return '';
    }
    //png|26.215769,127.687141&
    matchGroup = mapUri.match(/png\|([0-9\.,]+)&/);
    if (matchGroup && matchGroup.length > 1) {
        return matchGroup[1];
    }
    return mapUri;
}


function parseFunc(){
    var url = window.location.href;
    var enName = $.trim($('.poiDet-largeTit > h1.en').text());
    var cnName = $.trim($('.poiDet-largeTit > h1.cn').text());
	var photo = $('.poiDet-placeinfo p.coverphoto > img').attr('src');
    var score = $.trim($('.points > span.number').text());	
	var rank = $('li.rank > span').text().replace(/[第名]/g, "");
	var address = $.trim($('.poiDet-tips > li > span.title:contains("地址")').nextAll().text()).replace('\(查看地图\)', '');
	var openDate = $.trim($('.poiDet-tips > li > span.title:contains("开放时间")').nextAll().text());
	var traffic = $.trim($('.poiDet-tips > li > span.title:contains("到达方式")').nextAll().text());
	var businessDate = $.trim($('.poiDet-tips > li > span.title:contains("营业时间")').nextAll().text());
	var avgConsume = $.trim($('.poiDet-tips > li > span.title:contains("人均消费")').nextAll().text());
	var site = $.trim($('.poiDet-tips > li > span.title:contains("网址")').nextAll().text());
	var tel = $.trim($('.poiDet-tips > li > span.title:contains("电话")').nextAll().text());
	var ticket = $.trim($('.poiDet-tips > li > span.title:contains("门票")').nextAll().text());
	var type = $.trim($('.poiDet-tips > li > span.title:contains("所属分类")').nextAll().text()).replace(/\｜/g, ',').replace(/\//g, '');
    var description = $.trim($('.poiDet-detail').text());
    var tips = $.trim($('.poiDet-tipContent > div.content').text());
    var breadcrumb = $('.qyer_head_crumb > span.text.drop, span.space').text();	
    var googleMapUri = $('.map > img').attr('src');
	
	if (cnName === ""){
	    if (enName === "") {
	        return;
	    } else {
	        cnName = enName;
	    }
	}
	pushResult('url', url);
	pushResult('title', cnName);
	pushResult('enName', enName);
    pushResult('photo', photo);
    pushResult('breadcrumb', normalBreadCrumb(breadcrumb) + cnName);
    pushResult('address', normalStr(address));
    pushResult('openDate', normalStr(openDate));
    pushResult('avgConsume', normalStr(avgConsume));
    pushResult('traffic', normalMultiLineStr(traffic));
    pushResult('businessDate', normalStr(businessDate));
    pushResult('site', site);
    pushResult('tel', normalStr(tel));
    pushResult('ticket', normalStr(ticket));
    pushResult('type', normalStr(type));
    pushResult('rank', rank);
    pushResult('score', normalScore(score));
    pushResult('description', normalStr(description));
    pushResult('tips', normalMultiLineStr(tips));
    pushResult('poi', getGooglePoi(googleMapUri));
}

