var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

function main()
{
    parseBaseInfos();
    sendMessage(result);
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
    
    var url = window.location.href;
    $('dl.terms').each(function(){
        var province = $.trim($(this).find('dt').text());
        $(this).find('a').each(function(){
            var city = $.trim($(this).text());
            var url = $(this).attr('href');
            url = 'http://www.dianping.com' + url + '/food';
            pushResult(province + '\t' + city, url);
        });
    });
    
}



function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

