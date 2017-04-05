var result=[];

function main() {
    parseFunc();
}


function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalUserstar(userStar) {
    if (!userStar) {
        return '';
    }
    userStar = normalStr(userStar);
    matchGroup = userStar.match(/star([0-9]+)/);
    if (matchGroup && matchGroup.length > 1) {
        return (parseFloat(matchGroup[1])/10).toString();
    }
    return '';
}

function normalStar(star) {
    if (!star) {
        return '';
    }
    star = normalStr(star);
    
    star = star.replace('item-rank-rst irr-star', '');
    star = star.replace('user-rank-rst urr-rank', '');
    star = star.replace('sml-rank-stars sml-str', '');
    if (star && star.length > 0){
        return (parseFloat(star)/10).toString();
    }
    return star;
}



function parseFunc(){
	// basic info
	var url = window.location.href;
    var title = $('.revitew-title > h1 > a').attr('title');
    pushResult('url', url);
    pushResult('title', title);

    // user comments
    $(".comment-list > ul > li").each(
        function(){
            var userName = $(this).find('.name > a').text();
            var userPhoto = $(this).find('.pic > a > img').attr('src');
			//var userStar = normalUserstar($(this).find('.content > div.user-info > span').attr('class'));
			var userStar = normalStar($(this).find('p.contribution > span').attr('class'));
            //var commentStar = $(this).find('.content > div.user-info > div.comment-rst > span').text();
            var commentStar = normalStar($(this).find('.content > div.user-info > span').attr('class'));
            var cTaseStar = $(this).find('.content > div.user-info > div.comment-rst > span:contains("口味")').text().replace(/(口味|\(.*\))/g, '');
            var cConditionStar = $(this).find('.content > div.user-info > div.comment-rst > span:contains("环境")').text().replace(/(环境|\(.*\))/g, '');
            var cServiceStar = $(this).find('.content > div.user-info > div.comment-rst > span:contains("服务")').text().replace(/(服务|\(.*\))/g, '');
            
            
            var comment = $(this).find('.content > div.comment-txt').text();
			// 图集
            var photoArray = [];
            var photos = $(this).find('.content > div.shop-photo img').each(
                function() {
                    var photo = $.trim($(this).attr('src'));
                    photoArray.push(photo);
                }
            );
            // 发表日期，点赞数，回复数
            var commentDate = $.trim($(this).find('.content > .misc-info span.time').text());
            var commentZanCnt = $.trim($(this).find('.content > .misc-info span.heart-num').text().replace(/[\(\)]/g, ''));
            var commentReplyCnt = $.trim($(this).find('.content > .misc-info span.J_rtl').text());
            
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
			pushResult('userStar', userStar);
            pushResult('commentStar', normalStr(commentStar));
            //pushResult('commentStar', cTaseStar + '@@@' + cConditionStar + '@@@' + cServiceStar);
            pushResult('cTasteStar', cTaseStar);
            pushResult('cConditionStar', cConditionStar);
            pushResult('cServiceStar', cServiceStar);
            pushResult('comment', normalStr(comment));
            pushResult('commentPhoto', photoArray.join());
            pushResult('commentDate', commentDate);
            pushResult('commentZanCnt', commentZanCnt);
            pushResult('commentReplyCnt', commentReplyCnt);
        }
    );
	sendMessage(result);
}
