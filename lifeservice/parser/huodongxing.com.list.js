var result=[];


function main()
{
    $('a#pagination_next_link').click();
    //parseBaseInfos();
    window.setTimeout(parseBaseInfos,1000);
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    /* 
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    var url = window.location.href;
    var title = $.trim($('.poiDet-largeTit > h1.cn').text());
	var photo = $('.poiDet-placeinfo p.coverphoto > img').attr('src');

	pushResult('url', url);
	pushResult('title', normalStr(title));
    pushResult('photo', photo);
    */
    
    var baseUrl = 'http://www.huodongxing.com';
    
    $('ul.event-horizontal-list-new li').each(function(){
        var photo = $(this).find('img').attr('src');
        var url = $.trim($(this).find('h3 > a').attr('href'));
        if (url) {
            url = baseUrl + url;
        }
        var title = $.trim($(this).find('h3').text());
        var date = $.trim($(this).find('span.icon-time').parent().text());
        var address = $.trim($(this).find('span.icon-place').parent().text());
        var org = $.trim($(this).find('span.name').text());
        var orgicon = $.trim($(this).find('img.face').attr('src'));
        org = org + '\t' + orgicon;
        
        var count = $.trim($(this).find('span.pull-right').text());
        var like_count = '';
        var signup_count = '';
        if (count && count.indexOf('|') !== -1) {
            count = count.replace('|', "\t");
        } else {
            count = "0\t0";
        }
        
        value = title + '\t' + photo + '\t' + date + '\t' + address + '\t' + org + '\t' + count;
        pushResult(url, value);
        
    });
 
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

