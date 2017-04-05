var result=[];

function main() {
    try {
        parseFun();
    } catch(e) {
        //alert(e.toString());
    }
}


function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

// 从数组中获取第一个不为空的值
function takeFirst(valueArray) {
    for (var idx in valueArray) {
        if (valueArray[idx] && valueArray[idx].length > 0) {
            return valueArray[idx];
        }
    }
    return '';
}

function normalStar(star) {
    if (!star) {
        return '';
    }
    star = normalStr(star);
    star = star.replace('user-rank-rst urr-rank', '');
    star = star.replace('sml-rank-stars sml-str', '');
    if (star && star.length > 0){
        return (parseFloat(star)/10).toString();
    }
    return star;
}


function parseFun(){
    // basic info
    var url = window.location.href;
    // alert(url);
    var title = $('#body > div.body-content.clearfix > div.breadcrumb > span').text();
    var breadcrumb = $('#body > div.body-content.clearfix > div.breadcrumb').text();
    
    var photoArray = [];
    photoArray.push($('div.photo-thumb>div.photo-header>a.J_main-photo>img').attr('src'));
    photoArray.push($('div.photos-container > div.photos img:first').attr('src'));
    
    var photoCountArray = [];
    photoCountArray.push($.trim($('div.photo-thumb>div.photo-header>div.photo-info span.photo-count').text()));
    photoCountArray.push(normalStr($('div.photos-container a#pic-count').text()));
    
    var score = $('div.brief-info>span.mid-rank-stars').attr('class');
    if (score && score.length > 0) {
        score = (parseFloat(score.replace("mid-rank-stars mid-str", "")) / 10).toString();
    }
    var commentCount = $.trim($('div.brief-info>span:contains("条评论")').text().replace("条评论", ""));
    var avgPrice = $.trim($('div.brief-info>span:contains("人均")').text().replace(/(人均|消费|费用|元|：)/g, ""));
    var scoreTaste = $.trim($('div.brief-info>span:contains("口味")').text().replace(/(口味|：)/g, ""));
    var scoreCondition = $.trim($('div.brief-info>span:contains("环境")').text().replace(/(环境|：)/g, ""));
    var scoreService = $.trim($('div.brief-info>span:contains("服务")').text().replace(/(服务|：)/g, ""));
    var address = $.trim($('#basic-info > div.expand-info.address >span.info-name').nextAll().text());
    var telArray = [];
    $('#basic-info > p.expand-info.tel > span.item').each(function(){
        telArray.push($.trim($(this).text()));
    });
    
    var busiessDate = $.trim($('#basic-info > div.other.J-other > p.info > span.info-name:contains("营业时间")').next().text());
    var description = $.trim($('#basic-info > div.other.J-other > p.info:contains("商户简介")').text()).replace('商户简介：', '');
    
    // 详情页图集
    var photoSet = "";
    $('div.photo-carousel>ul>li>a').each(function(){
        var tmpPhoto = $(this).attr('data-picsrc');
        if (photoSet === "") {
            photoSet = tmpPhoto;
        } else {
            photoSet = photoSet + ',' + tmpPhoto;
        }
    });
        
    
    
    // 推荐商品
    var recomCommoditys = '';
    $('p.info.info-indent span.info-name:contains("推荐产品")').siblings().each(function() {
        var recomCommodity = $.trim($(this).text()); 
        if (recomCommodity && recomCommodity.length > 0) {
            if (recomCommoditys === '') {
                recomCommoditys = recomCommodity;
            } else {
                recomCommoditys = recomCommoditys + ',' + recomCommodity;
            }
        }
    });
    // 分类标签
    var tags = "";
    $('p.info.info-indent span.info-name:contains("分类标签")').siblings().each(function() {
        var tag = $.trim($(this).text()); 
        if (tag && tag.length > 0) {
            if (tags === '') {
                tags = tag;
            } else {
                tags = tags + ',' + tag;
            }
        }
    });
    
    // 无图推荐菜品 <foodName  foodUrl  recommCount>
    var recommNoPhotoFood = '';
    var recommFoodSet = [];
    var dianpingBaseUrl = 'http://www.dianping.com';
    $('p.recommend-name > a').each(function(){
        var foodName = $.trim($(this).attr('title'));
        var foodUrl = $.trim($(this).attr('href'));
        var recommCount = $.trim($(this).find('em.count').text().replace(/[\(\)]/g, ""));
        if (foodUrl && foodUrl.length > 5) {
            foodUrl = dianpingBaseUrl + foodUrl;
        }
        
        if (foodUrl && foodUrl.length > 0) {
            recommFoodSet[foodUrl] = foodName + '@@@' + recommCount;    
        }
    });
    
    
    // 有图推荐菜品 <foodName  foodUrl  foodPhoto  foodPrice>
    var recommPhotoFood = '';
    var recommPhotoFoodSet = [];
    $('.recommend-photo li.item').each(function() {
        var foodPhoto = $(this).find('img').attr('src');
        var foodName = $.trim($(this).find('p.name').text());
        var foodPrice = $.trim($(this).find('span.price').text());
        var foodUrl = $.trim($(this).find('a').attr('href'));
        if (foodUrl && foodUrl.length > 5) {
            foodUrl = dianpingBaseUrl + foodUrl;
        }
        
        if (foodUrl in recommFoodSet) {
            recommFoodSet[foodUrl] = recommFoodSet[foodUrl] + '@@@' + foodPhoto + '@@@' + foodPrice;
        } else {
            recommFoodSet[foodUrl] = foodName + '@@@@@@' + foodPhoto + '@@@' + foodPrice;
        }
    });
    
    // google poi
    var poi = "";
    $('script').each(
        function() {
            var text = $(this).text();
            if (text.search('lat')>0 && text.search('lng')>0) {
                // {lng:127.690937,lat:26.229545}
                matchGroup = text.match(/{lng:([0-9\.]+),lat:([0-9\.]+)}/);
                if (matchGroup && matchGroup.length > 2) {
                    poi = matchGroup[2] + ',' + matchGroup[1];
                }
            }
        }
    );
    
    
    // 基本信息保存
	pushResult('url', url);
    pushResult('title', title);
    pushResult('breadcrumb', normalStr(breadcrumb));
    pushResult('poi', poi);
    pushResult('photo', takeFirst(photoArray));
    //pushResult('photo', photoArray.toString());
    pushResult('photoSet', photoSet);
    pushResult('photoCount', takeFirst(photoCountArray));
    pushResult('commentCount', commentCount);
    pushResult('avgPrice', avgPrice);
    pushResult('score', score);
    pushResult('scoreTaste', scoreTaste);
    pushResult('scoreCondition', scoreCondition);
    pushResult('scoreService', scoreService);
    
    pushResult('address', normalStr(address));
    pushResult('tel', telArray.toString());
    pushResult('businessDate', normalStr(busiessDate));
    pushResult('description', normalStr(description));
    pushResult('recomCommoditys', normalStr(recomCommoditys));
    pushResult('tags', tags);
    
    // 有图，无图的推荐菜
    //pushResult('recommNoPhotoFood', recommNoPhotoFood);
    //pushResult('recommPhotoFood', recommPhotoFood);
    
    for(var foodUrl in recommFoodSet) {
        pushResult('recommFood', foodUrl + '@@@' + recommFoodSet[foodUrl]);
    }
    

    // 团购，外卖，订餐标志
    var serviceTuan = $('p.J-service > a.J-service-tuan').attr('href');
    var serviceDing = $('p.J-service > a.tag-ding-b').attr('class');
    var serviceWai = $('p.J-service > a.tag-wai-b').attr('href');
    if (serviceTuan && serviceTuan.length > 5) {
        pushResult('serviceTuan', '团@@@' + serviceTuan);
    }
    if (serviceDing && serviceDing.length > 5) {
        pushResult('serviceDing', '订');
    }
    if (serviceWai && serviceWai.length > 5) {
        pushResult('serviceWai', '外@@@' + serviceWai);
    }


    // 团购信息
    $('div.item.big').each(function(){
        var tTitle = $.trim($(this).find('p.title').text()); 
        var tPic = $.trim($(this).find('img.pic').attr('src'));
        var tRawPrice = $.trim($(this).find('del.del-price').text());
        var tPrice = $.trim($(this).find('span.price').text());
        var tSoldCnt = $.trim($(this).find('span.sold-count').text());
        var tTag = $.trim($(this).find('i.tag').text());
        var tUrl = $.trim($(this).find('a').attr('href'));
        if (tTag === '') {
            tTag = '惠';
        }
        var tuanInfo = tTag + '@@@' + tUrl + '@@@' + tTitle + '@@@' + tPic + '@@@' + tPrice + '@@@' + tRawPrice + '@@@' + tSoldCnt;
        pushResult("tuanInfo", tuanInfo);          
    });

    $('div.item.small').each(function(){
        var tTitle = $.trim($(this).find('p.title').text()); 
        var tPic = $.trim($(this).find('img.pic').attr('src'));
        var tRawPrice = $.trim($(this).find('del.del-price').text());
        var tPrice = $.trim($(this).find('span.price').text());
        var tSoldCnt = $.trim($(this).find('span.sold-count').text());
        var tTag = $.trim($(this).find('i.tag').text());
        var tUrl = $.trim($(this).find('a').attr('href'));
        if (tTag === '') {
            tTag = '惠';
        }
        var tuanInfo = tTag + '@@@' + tUrl + '@@@' + tTitle + '@@@' + tPic + '@@@' + tPrice + '@@@' + tRawPrice + '@@@' + tSoldCnt;
        pushResult("tuanInfo", tuanInfo);          
    });

    $('a.item.small').each(function(){
        var tTitle = '';
        var tTag = $.trim($(this).find('i.tag').text());
        if (tTag === '团') {
            tTitle = normalStr($.trim($(this).text())); 
            var tRawPrice = $.trim($(this).find('del.del-price').text());
            var tPrice = $.trim($(this).find('span.price').text());
            var tUrl = $.trim($(this).attr('href'));
            
            tTitle = normalStr(tTitle.replace(/^团/, '').replace(tRawPrice, '').replace(tPrice, ''));
            var tuanInfo = tTag + '@@@' + tUrl + '@@@' + tTitle + '@@@@@@' + tPrice + '@@@' + tRawPrice + '@@@';
            pushResult("tuanInfo", tuanInfo);
        } else if (tTag === '活'){
            tTitle = normalStr($(this).text()).replace(/^活/, '');
            pushResult("huoInfo", '活@@@' + tTitle);
        }
    });

    // 促销信息
    $('a.J_short-promo').each(function(){
        var cuTitle = normalStr($.trim($(this).text())).replace(/^促/, ''); 
        var cuTag = $.trim($(this).find('i.tag').text());
        var cuInfo = cuTag + '@@@' + cuTitle;
        pushResult("cuInfo", cuInfo); 
    });

    // 订座拂去
    var serviceDingText = $.trim($('a#booking').text());
    var serviceVIP = $.trim($('span.vip-desc').text());
    if (serviceDingText && serviceDingText.length > 0) {
        pushResult("dingInfo", '订@@@' + serviceDingText); 
    }
    if (serviceVIP && serviceVIP.length > 0) {
        pushResult("vipInfo", '卡@@@' + serviceVIP); 
    }
    // 刷卡服务
    

    // 环境图集
    var conditionPhotoSet = "";
    $('div.shop-tab-photos a.item').each(function(){
        var tPicTitle = $.trim($(this).attr('title'));
        var tPic = $.trim($(this).find('img').attr('src'));
        var tConditionPhoto = tPicTitle + '@@@' + tPic;
        if (conditionPhotoSet === '') {
            conditionPhotoSet = tConditionPhoto;
        } else {
            conditionPhotoSet = conditionPhotoSet + ',' + tConditionPhoto;
        }
    });
    pushResult('conditionPhotoSet', conditionPhotoSet);
    
    // 网友点评总结
    var commentSummaryArray = [];
    $('div.comment-condition > div.content > span.J-summary').each(function(){
        commentSummaryArray.push($.trim($(this).text()));
    });
    pushResult('commentSummary', commentSummaryArray.toString());
    
    parseComments();

	//sendMessage(result);
	sendResult(result);
}


function parseComments(){
    // user comments
    $("li.comment-item").each(
        function(){
            var userName = $.trim($(this).find('p.user-info > a.name').text());
            var userPhoto = $(this).find('a.J-avatar > img').attr('src');
			var userStar = normalStar($.trim($(this).find('p.user-info > span').attr('class')));
			
            var commentStar = normalStar($(this).find('p.shop-info > span:first').attr('class'));
            var cTaseStar = $(this).find('p.shop-info > span:contains("口味")').text().replace(/(口味：)/g, '');
            var cConditionStar = $(this).find('p.shop-info > span:contains("环境")').text().replace(/(环境：)/g, '');
            var cServiceStar = $(this).find('p.shop-info > span:contains("服务")').text().replace(/(服务：)/g, '');
            var commentArray = [];
            commentArray.push(normalStr($(this).find('div.J-info-all').text()).replace(/收起$/, ''));
            commentArray.push(normalStr($(this).find('p.desc').text()));
			// 图集
            var photoArray = [];
            var photos = $(this).find('a.J-photo > img').each(
                function() {
                    var photo = $.trim($(this).attr('src'));
                    photoArray.push(photo);
                }
            );
            // 发表日期，点赞数，回复数
            var commentDate = $.trim($(this).find('div.misc-info span.time').text());
            var commentZanCnt = $.trim($(this).find('a.J-praise').text().replace(/[赞\(\)]/g, ''));
            var commentReplyCnt = $.trim($(this).find('.content > .misc-info span.J_rtl').text());
            
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
			pushResult('userStar', userStar);
            pushResult('commentStar', commentStar);
            pushResult('cTasteStar', cTaseStar);
            pushResult('cConditionStar', cConditionStar);
            pushResult('cServiceStar', cServiceStar);
            pushResult('comment', takeFirst(commentArray));
            pushResult('commentPhoto', photoArray.join());
            pushResult('commentDate', commentDate);
            pushResult('commentZanCnt', commentZanCnt);
            pushResult('commentReplyCnt', commentReplyCnt);
        }
    );
    
}
