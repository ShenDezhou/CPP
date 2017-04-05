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
    var cityName = $.trim($('p.plcTopBarNameCn').text());
    var itemName = $.trim($('div.entry_main > h1').text());
    var content = $('div.entry_main').html();
    content = normContent(content);
    pushResult(url + '\t' + cityName + '\t' + itemName, content);
}


// 归一化，净化内容
function normContent(content) {
    content = content.replace('这里还没有信息，快来补充~', '暂无');
    content = content.replace(/<div class="entry_bottom".*$/, '</div>');
    content = content.replace(/\t/g, '  ');
    return normalStr(content);
}


function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

