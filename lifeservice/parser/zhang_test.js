var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

function main()
{
    parseBaseInfos();
    //window.setTimeout(clickFunc,500);
    sendMessage(result);
}
function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    
    var url = window.location.href;
    var title = $.trim($('h1#HEADING').text());
    var enTitle = $.trim($('span.altHead').text());
    var chTitle = title.replace(enTitle, '');
    var breadcrumb = $('.topLevel > li > ul > li').text();
	var photo = $('#HERO_PHOTO').attr('src');
	var score = $.trim($('#HEADING_GROUP > div > div.heading_ratings > div:nth-child(1) > div > span > img').attr('alt'));
	if (score) {
	    score = score.replace("分","");
	}
	var rank = $.trim($('#HEADING_GROUP > div > div.heading_ratings > div:nth-child(2) > span > div > b > span').text());
	if (rank) {
	    rank = rank.replace("第","");
	}
	var address = $.trim($('#ABOVE_THE_FOLD > div.map_and_listing > div.main_section.listingbar > div > div.above_fold_listing_details > div > div.detail_section.info > div > address > span > span').text());
	var openDate = $.trim($('#HOUR_OVERLAY_CONTENTS').text());
	if (openDate) {
	       openDate = openDate.replace("经营时间:","");
	}
	var tel = $.trim($('#ABOVE_THE_FOLD > div.map_and_listing > div.main_section.listingbar > div > div.above_fold_listing_details > div > div.detail_section.info > div > div.contact_info > div > div:nth-child(1) > div').text());
	var description = $('div.listing_details').text();
	var ticket = $('#ABOVE_THE_FOLD > div.map_and_listing > div.main_section.listingbar > div > div.above_fold_listing_details > div > div:nth-child(5) > div > div:nth-child(2)').text();
	if (ticket) {
	    ticket = ticket.replace("收费:","");
	}
	var adviceDate = $('#ABOVE_THE_FOLD > div.map_and_listing > div.main_section.listingbar > div > div.above_fold_listing_details > div > div:nth-child(5) > div > div:nth-child(1)').text();
	if (adviceDate) {
	    adviceDate = adviceDate.replace("建议的造访时间:","");
	}
	pushResult('url', url);
	pushResult('breadcrumb', normalStr(breadcrumb));
	pushResult('title', normalStr(chTitle));
	pushResult('enTitle', enTitle);
	pushResult('photo', photo);
	pushResult('tel', tel);
	pushResult('openDate', normalStr(openDate));
	
	pushResult('ticket' ,ticket);
	pushResult('adviceDate' ,adviceDate);
	pushResult('rank', rank);
	pushResult('address', normalStr(address));
    pushResult('score', score);
	pushResult('description', description);
	
    /*
	
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
	*/
}


/*
 *  本函数内容需要您填充
 *  即抽取在每次翻页时，需要解析的字段内容
 *  常见情况是抽取翻页的评论信息
*/
function parseTurnPageInfos() {
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取评论列表（urserName  userPhoto）为例
    // 与parseBaseInfos抽取基本信息不同， 抽取的是列表型信息，所以使用了.each 
    /*
    // user's comments
    $('#commentlist > li').each(
        function(){
            var userPhoto = $(this).find('.largeavatar > img').attr('src');
            var userName = $(this).find('.largeavatar > span').text();
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
        }
    );
    */
}


/* 本函数内容需要您填充
 * 即找到页面的翻页按钮，没翻一页调用parseTurnPageInfos进行解析
 * 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中翻页为例，代码如下
 * 您需要修改的是 var nextPage = $('.ui_page_item.ui_page_next'); 到您实际的需求
*/
function clickFunc(){
    parseTurnPageInfos();

    // 根据您的需替换下面这一行即可
    var nextPage = $('.ui_page_item.ui_page_next');
    
    if (++current_nextpage_num<=MAX_NEXTPAGE_NUM && nextPage && nextPage.length > 0) {
        nextPage[0].click();
		window.setTimeout(clickFunc, 1000);
		return;
    }
    sendMessage(result);
}


function pushResult(key, val) {
//result.push("ok");
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}



