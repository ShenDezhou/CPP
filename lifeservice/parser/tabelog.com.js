var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

function main()
{
//    debugger;
    parseFunc();
    sendMessage(result);
   // window.setTimeout(clickFunc, 1 * 1000);
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

function normalcrumb(str) {
    return str.replace(/[\r\n]/g, '>').replace(/\s+/g,' ').replace(/[\s>]+/g, '>');
}


// 名称(title)、英文名称(enName)、图片(photo)、POI、地址(address)、排名(rank)、点评(网友名称(userName)、头像(userPhoto)、星级(commentStar)、评论(comment)、图片(commentPhoto)、发布时间(commentDate))、简介(description)、所在地归属(breadcrumb)、电话(tel)、营业时间、官网(site)、人均消费、分类(菜系)、推荐菜品、标签(本店入选榜单、大众点评)、特色(大众点评)、交通

function parseFunc(){
    // basic info
    var url = window.location.href;
    var breadcrumb = $.trim($('.l-column2__main li.c-breadcrumb__item.is-current').text());   // need handle
    var title = $.trim($('.rd-header__headline > h2.rd-header__rst-name > small.rd-header__rst-name-ja').text()).replace(/[\(\)]/g, '');
    var enName = $.trim($('.rd-header__headline > h2.rd-header__rst-name > a.rd-header__rst-name-main').text());

    // cover photos
    coverPhoto = [];
    $('.c-grids.rd-grids li.rd-grids__photo').each( function() {
        var photo = $(this).find('img').attr('src');
        if (photo && photo.length>0) {
            coverPhoto.push(photo);
        }
    });

    var site = $.trim($('.c-table.rd-detail-info  tr > th:contains("首页")').next().text());
    var tel = $.trim($('.c-table.rd-detail-info  tr > th:contains("电话")').next().text());
    var address = $.trim($('.c-table.rd-detail-info  tr > th:contains("地址")').next().text()).replace('看大地图', '');
    var traffic = $.trim($('.c-table.rd-detail-info  tr > th:contains("交通方式")').next().text());
    var type = $.trim($('.c-table.rd-detail-info  tr > th:contains("类型")').next().text());
    var businessDate = $.trim($('.c-table.rd-detail-info  tr > th:contains("营业时间")').next().text());
    var avgConsume = $.trim($('.c-table.rd-detail-info  tr > th:contains("预算")').next().text());
    var score = $.trim($('.rd-header__rst-rate b.c-rating__val.c-rating__val--strong').text());

    // push basic infos into result array
    pushResult('url', url);
    pushResult('title', title);
    pushResult('enName', enName);
    pushResult('photo', coverPhoto.toString());
    pushResult('score', score);
    pushResult('address', normalStr(address));
    pushResult('tel', normalStr(tel));
    pushResult('site', site);
    pushResult('traffic', normalStr(traffic));
    pushResult('type', type);
    pushResult('businessDate', normalStr(businessDate));
    pushResult('avgConsume', normalStr(avgConsume));
    pushResult('breadcrumb', normalcrumb(breadcrumb));

    // google poi (lat, lng)
    var poi = "";
    // "Staticmap?client=gme-kakakucominc&channel=tabelog.com&sensor=false&hl=ja&center=35.702148522095904,139.76620752825878&markers=color:red%7c35.702148522095904,139"
    $('.rd-header__rst-map > a > img').each(
        function() {
            var text = $(this).attr('alt');
            matchGroup = text.match(/&center=([0-9\.,]+)&/);
            if (matchGroup && matchGroup.length > 1) {
                poi = matchGroup[1];
            }
        }
    );
    pushResult('poi', poi);

/*
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
	*/
}

function getAllComments() {
    // user comments
    $('.wrapper > ul.rev-lists > li.rev-item').each( function(){
        var userPhoto = $(this).find('div.avatar > a > img').attr('src');
        var userName = $.trim($(this).find('a.name').text());
        var commentStar = $(this).find('span.rating-star > span').attr('class').replace('star', '');
        var comment = $.trim($(this).find('p.rev-txt').text());

        var photoArray = [];
        var photos = $(this).find('div.rev-img > a').each( function() {
            var photo = $.trim($(this).find('img').attr('src'));
            photoArray.push(photo);
        });
        var commentDate = $(this).find('div.info > a.btn-comment').next().text();

        pushResult('userName', userName);
        pushResult('userPhoto', userPhoto);
        pushResult('commentStar', commentStar);
        pushResult('comment', comment);
        pushResult('commentDate', commentDate);
        pushResult('commentPhoto', photoArray.toString());
    });
}
                                           
