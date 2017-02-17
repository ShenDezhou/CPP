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
	   
            var breadcrumb = trim($('#body > div.body-content.clearfix > div.breadcrumb').text());
            var score = $('div.brief-info>span.mid-rank-stars').attr('class');
    	    if (score && score.length > 0) {
        	score = (parseFloat(score.replace("mid-rank-stars mid-str", "")) / 10).toString();
    	    }
	    pushResult('url', url)
	    pushResult('breadcrumb', breadcrumb);
	    pushResult('score', score);
	    

	    // 团购信息
	    $('div.item.big').each(function(){
		var tTitle = trim($(this).find('p.title').text()); 
		var tPic = trim($(this).find('img.pic').attr('src'));
		var tRawPrice = trim($(this).find('del.del-price').text());
		var tPrice = trim($(this).find('span.price').text());
		var tSoldCnt = trim($(this).find('span.sold-count').text());
		var tTag = trim($(this).find('i.tag').text());
		var tUrl = trim($(this).find('a').attr('href'));
		if (tTag === '') {
		    tTag = '惠';
		}
		var tuanInfo = tTag + '@@@' + tUrl + '@@@' + tTitle + '@@@' + tPic + '@@@' + tPrice + '@@@' + tRawPrice + '@@@' + tSoldCnt;
		pushResult("tuanInfo", tuanInfo);          
	    });	

	    $('div.item.small').each(function(){
		var tTitle = trim($(this).find('p.title').text()); 
		var tPic = trim($(this).find('img.pic').attr('src'));
		var tRawPrice = trim($(this).find('del.del-price').text());
		var tPrice = trim($(this).find('span.price').text());
		var tSoldCnt = trim($(this).find('span.sold-count').text());
		var tTag = trim($(this).find('i.tag').text());
		var tUrl = trim($(this).find('a').attr('href'));
		if (tTag === '') {
		    tTag = '惠';
		}
		var tuanInfo = tTag + '@@@' + tUrl + '@@@' + tTitle + '@@@' + tPic + '@@@' + tPrice + '@@@' + tRawPrice + '@@@' + tSoldCnt;
		pushResult("tuanInfo", tuanInfo);          
	    });

	    $('a.item.small').each(function(){
		var tTitle = '';
		var tTag = trim($(this).find('i.tag').text());
		if (tTag === '团') {
		    tTitle = normalStr(trim($(this).text())); 
		    var tRawPrice = trim($(this).find('del.del-price').text());
		    var tPrice = trim($(this).find('span.price').text());
		    var tUrl = trim($(this).attr('href'));
		    
		    tTitle = normalStr(tTitle.replace(/^团/, '').replace(tRawPrice, '').replace(tPrice, ''));
		    var tuanInfo = tTag + '@@@' + tUrl + '@@@' + tTitle + '@@@@@@' + tPrice + '@@@' + tRawPrice + '@@@';
		    pushResult("tuanInfo", tuanInfo);
		} else if (tTag === '活'){
		    tTitle = normalStr($(this).text()).replace(/^活/, '');
		    pushResult("huoInfo", '活@@@' + tTitle);
		}
	    });

	/*
            var title = trim($('body > div.content.cf > div.breadbar_v1.cf > ul > li').last().text());
            var photo = $("#detailCarousel div.item > a > img").attr('src');
            var score = trim($('.detailtop.cf.normalbox > ul.detailtop_r_info > li > span.score').text()).replace('分', '');

            var addressList = [];
            addressList.push(trim($('.s_sight_infor>ul.s_sight_in_list>li>span.s_sight_classic:contains("址：")').next('span').text()));
            addressList.push(trim($('.s_sight_infor > p.s_sight_addr').text()).replace('地址：', ''));
            var address = takeFirst(addressList);

            var tel = trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("话：")').next('span').text());
            // 菜系
            var cuisineArray = [];
            $('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("系：")').next('span').find('dd>a').each(function() {
                cuisineArray.push(trim($(this).text()));
            });
            var site = trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("网站：")').next('span').text());
            var type = trim($('.s_sight_infor>ul.s_sight_in_list>li>span.s_sight_classic:contains("型：")').next('span').text());
            var adviceDate = trim($('.s_sight_infor>ul.s_sight_in_list>li>span.s_sight_classic:contains("游玩时间：")').next('span').text());
            var avgConsume = trim($('.s_sight_infor > ul.s_sight_in_list > li > span.s_sight_classic:contains("均：")').next('span').text());

            var businessDateList = [];
            businessDateList.push(trim($('.s_sight_infor > dl.s_sight_in_list dt:contains("营业时间")').next().text()));
            businessDateList.push(trim($('.s_sight_infor>ul.s_sight_in_list>li>span.s_sight_classic:contains("营业时间：")').next('span').text()));
            var businessDate = takeFirst(businessDateList);

            var ticket = trim($('.s_sight_infor > dl.s_sight_in_list dt:contains("门票信息")').next().text());
            var openDate = trim($('.s_sight_infor > dl.s_sight_in_list dt:contains("开放时间")').next().text());
            var recommandFood = trim($('.normalbox > div.detailcon >h2:contains("本店特色美食")').next().text());
            var breadcrumb = $('.breadbar_v1.cf > ul > li > a,i[class="icon_gt"]').text();

            var descriptionList = [];
            descriptionList.push($('.toggle_s > div[itemprop="description"]').text());
            descriptionList.push(trim($('.normalbox > div.detailcon > div[itemprop="description"] ').text()));
            var description = takeFirst(descriptionList);

            var traffic = trim($('div.detailcon h2:contains("交通")').next().text());
            var special_tips = trim($('div.detailcon h2:contains("特别提示")').next().text());
            var bright_spot = trim($('.detailcon.bright_spot > ul').text());

            var poiList = [];
	    
	    var lat = trim($('input#Lat').attr('value'));
	    var lng = trim($('input#Lon').attr('value'));
	    if (lat.length > 5 && lng.length > 5) {
		poiList.push(lat + ',' + lng);
	    }
	

            // food google map poi
            $('script').each(function() {
                var mapUri = $(this).text();
                // "lng":139.772583007812, "lat":35.7059631347656,
                matchGroup = mapUri.match(/"lng":([0-9\.]+), "lat":([0-9\.]+),/);
                if (matchGroup && matchGroup.length > 2) {
                    poiList.push(matchGroup[2] + ',' + matchGroup[1]);
                }
            });
            poiList.push(getGooglePoi($('.s_sight_check_infor > div.s_sight_map > a > img').attr('src')));
            var poi = takeFirst(poiList);

            pushResult('url', url);
            pushResult('title', title);
            pushResult('breadcrumb', normalCrumb(breadcrumb));
            pushResult('photo', photo);
            pushResult('score', score);
            pushResult('avgConsume', normalStr(avgConsume)); // food only
            pushResult('cuisine', cuisineArray.toString()); // food only
            pushResult('tel', normalStr(tel));
            pushResult('address', normalStr(address));
            pushResult('poi', poi);
            pushResult('businessDate', normalStr(businessDate));
            pushResult('adviceDate', normalStr(adviceDate));
            pushResult('site', normalStr(site));
            pushResult('type', normalStr(type));
            pushResult('traffic', normalStr(traffic));
            pushResult('special_tips', normalMultiLineStr(special_tips));
            pushResult('ticket', normalStr(ticket));
            pushResult('openDate', normalStr(openDate));
            pushResult('bright_spot', normalMultiLineStr(bright_spot));
            pushResult('recommandFood', normalStr(recommandFood));
            pushResult('description', normalStr(description));
	*/
}

// 根据url分发给不同的解析函数
parse = function(url, content) {
    //console.log(url);
    parseBaseInfo(url, content);
    //parseComments(url, content);
}

// 对外开放接口
exports.parse = parse;
