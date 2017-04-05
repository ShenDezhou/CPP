var result=[];

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
    var city = $.trim($('span.city_ding').text());
    var area = $.trim($('div.hotle_address > p.p_one > span.span_one').text());
    var title = $.trim($('div.hotle_address > p.p_one > span.span_three').text());
    var addr = $.trim($('div.hotle_address span.span_seven:contains("地址")').next().text());
    
    var baseInfo = city + "\t" + area + "\t" + title + "\t" + addr;
    
    $('div.hottest_dishes').each(function(){
        var dish = $.trim($(this).find('p').text());
        $(this).find('tr').each(function(){
            var coffee = $.trim($(this).find('td.td_one1').text());
            var price = $.trim($(this).find('td.td_two').text());
            
            dishInfo = dish + "\t" + coffee + "\t" + price.replace("/", "\t");
            if (title.indexOf("星巴克") !== -1) {
                pushResult(url, baseInfo + "\t" + dishInfo);
            }
        })
    })
}



function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

