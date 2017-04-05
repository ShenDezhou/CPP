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
    var nextPage = $('.pager_v1 > a.nextpage');
    if (++current_nextpage_num <= MAX_NEXTPAGE_NUM && nextPage && nextPage.length>0 && !(nextPage.hasClass('nextpage disabled'))) {
        nextPage[0].click();
		window.setTimeout(clickFunc, 500);
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
    $('.comment_single').each(
        function(){
            var userPhoto = $(this).find('.userimg > a > img').attr('src');
            var userName = $(this).find('.ellipsis > a').text();
			var commentStar = $(this).find('.title.cf span.starlist > span').attr('style');
			var commentDetailStar = $(this).find('.title.cf span.sblockline').text();
            var comment = $.trim($(this).find('.main_con > span.heightbox').text());

            var photoArray = [];
            var photos = $(this).find('.comment_piclist.cf > a').each(
                function() {
                    var photo = $.trim($(this).find('img').attr('src'));
                    photoArray.push(photo);
                }
            );
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
        }
    );    
}


function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
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
    return parseInt((parseInt(ctripStar) +10)/20).toString();
}

function normalCrumb(crumb) {
    if (!crumb) {
        return '';
    }
    // 旅游攻略社区>目的地>日本>冲绳>那霸购物>
    crumb = normalStr(crumb)
    crumb = crumb.replace(/旅游攻略社区\>目的地\>/, '');
    crumb = crumb.replace(/\>$/, '');
    return crumb;
}

// 归一化需要保留换行符号的字符串
function normalMultiLineStr(str) {
    str = str.replace(/[\r\n]/g, '###').replace(/\s+/g,' ');
    str = str.replace(/######/g, '###');
    return str;
}


// 归一化分数
function normalScore(score) {
    if (score === '') {
        return 0;
    }
    return parseInt((score +10)/20);    
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


function parseFunc(){
    var url = window.location.href;
	var title = $('body > div.content.cf > div.breadbar_v1.cf > ul > li:last > h1').text();
	var photo = $("#detailCarousel div.item > a > img").attr('src');
	var score = $.trim($('.detailtop.cf.normalbox > ul.detailtop_r_info > li > span.score').text()).replace('分', '');
	var address = $.trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("址：")').next('span').text());
    var address2 = $.trim($('.s_sight_infor > p.s_sight_addr').text()).replace('地址：', '');	
	var tel = $.trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("话：")').next('span').text());
	// 菜系
	var cuisineArray = [];
	$('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("系：")').next('span').find('dd>a').each(
	    function() {
	        cuisineArray.push($.trim($(this).text()));
	});
	var site = $.trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("网站：")').next('span').text());
	var adviceDate = $.trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("游玩时间：")').next('span').text());
	var businessDate_food = $.trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("营业时间：")').next('span').text());
	var avgConsume = $.trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("均：")').next('span').text());
	var businessDate = $.trim($('.s_sight_infor > dl.s_sight_in_list dt:contains("营业时间")').next().text());
	var ticket = $.trim($('.s_sight_infor > dl.s_sight_in_list dt:contains("门票信息")').next().text());
	var openDate = $.trim($('.s_sight_infor > dl.s_sight_in_list dt:contains("开放时间")').next().text());
    var recommandFood = $.trim($('.normalbox > div.detailcon >h2:contains("本店特色美食")').next().text());	
    var breadcrumb = $('.breadbar_v1.cf > ul > li > a,i[class="icon_gt"]').text();	
    var description = $('.toggle_s > div[itemprop="description"]').text();
    var description_food = $.trim($('.normalbox > div.detailcon > div[itemprop="description"] ').text());
    var traffic = $.trim($('div.detailcon h2:contains("交通")').next().text());
    var special_tips = $.trim($('div.detailcon h2:contains("特别提示")').next().text());
    var bright_spot = $.trim($('.detailcon.bright_spot > ul').text());
    var googleMapUri = $('.s_sight_check_infor > div.s_sight_map > a > img').attr('src');
	
    pushResult('url', url);
    // pushResult('schema@t1', 'title@photo......')

    
	pushResult('title', title);
    pushResult('photo', photo);
    pushResult('score', score);
    pushResult('avgConsume', normalStr(avgConsume));             // food only
    pushResult('cuisine', cuisineArray.toString());   // food only
    pushResult('tel', normalStr(tel));
    pushResult('address', normalStr(address));
    pushResult('address', normalStr(address2));
    pushResult('businessDate', normalStr(businessDate));
    pushResult('businessDate', normalStr(businessDate_food));
    pushResult('adviceDate', normalStr(adviceDate));
    pushResult('site', normalStr(site));
    pushResult('traffic', normalStr(traffic));
    pushResult('special_tips', normalMultiLineStr(special_tips));
    pushResult('ticket', normalStr(ticket));
    pushResult('openDate', normalStr(openDate));
    pushResult('bright_spot', normalMultiLineStr(bright_spot));
    pushResult('recommandFood', normalStr(recommandFood));
    pushResult('breadcrumb', normalCrumb(breadcrumb));
    pushResult('description', normalStr(description));
    pushResult('description', normalStr(description_food));
    pushResult('poi', getGooglePoi(googleMapUri));
    
    // pushResult('schema@t2', '@photo......')
        
    // food google map poi
	var poi = "";
    /* "POIADM":0,"lat":26.694156875211,"lng":127.87793143436,"zoom":15, */
    $('script').each(
        function() {
            var mapUri = $(this).text();
            // "lng":139.772583007812, "lat":35.7059631347656,
            matchGroup = mapUri.match(/"lng":([0-9\.]+), "lat":([0-9\.]+),/);
            if (matchGroup && matchGroup.length > 2) {
                 pushResult('poi', matchGroup[2] + ',' + matchGroup[1]);
            }
        }
    );
}


