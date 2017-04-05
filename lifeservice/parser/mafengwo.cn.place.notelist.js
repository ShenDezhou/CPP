var result=[];
var baseUrl = "http://www.mafengwo.cn";

function main()
{
    parseBaseInfos();
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
    var place = $.trim($('div.crumb > div.cur').text().replace('游记', ''));
    
    
    $('li.post-item').each(function(){
       var itemTitle = $.trim($(this).find('h2.post-title').text());
       var itemUrl = $.trim($(this).find('h2.post-title > a').attr('href')); 
       var itemLike = $.trim($(this).find('div.post-ding > span').text());
       //var itemReply = $.trim($(this).find('span.bbsreply').text());
       //var itemView = $.trim($(this).find('span.bbsview').text());
       
       var key = url + '\t' + place;
       if (itemUrl.length > 0) {
            var value = baseUrl + itemUrl+'\t'+itemLike+'\t'+itemTitle;
            pushResult(key,  value);
       }
       
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

