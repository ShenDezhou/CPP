var result=[];

function main() {
    parseFun();
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
    
    //url = url.replace('http://www.dianping.com/search/category/', '').replace(/\//g, '\t');
    result.push('url\t' + url);
    result.push('fetchtime\t' + formatDate());
    

    baseUrl = "http://www.dianping.com";
    $('div.shop-list li').each(function(){
        var shopurl = baseUrl + $(this).find('div.pic a').attr('href');
        var title = $.trim($(this).find('h4').text());
        var photo = $(this).find('img').attr('data-src');
        var star = normalStar($(this).find('div.comment > span.sml-rank-stars').attr('class'));
        var reviewcnt = normalStr($(this).find('div.comment a.review-num').text());  
        if (reviewcnt) {
            reviewcnt = reviewcnt.replace(' 条点评', '');
        }
        
        result.push(shopurl + '\t' + title + '\t' + photo + '\t' + star + '\t' + reviewcnt);
    });

	sendResult(result);
}



function formatDate() {
    var today = new Date();
    var dateStr = today.toISOString().substring(0, 10);
    var timeStr = today.getHours() + ":" + today.getMinutes() + ":" + today.getSeconds();
    return dateStr + " " + timeStr;
}
