var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

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
    var place = $.trim($('p.pl_topbox_cn').text());
    
    $('ul.pla_travellist > li.item').each(function(){
       var itemTitle = $.trim($(this).find('h3.title > a').text());
       var itemUrl = $.trim($(this).find('h3.title > a').attr('href'));
       var itemAuthor = $.trim($(this).find('span:contains("作者")').find("a").attr("href").replace("http://www.qyer.com/u/", ""));
       var itemLike = $.trim($(this).find('span.bbslike').text());
       var itemReply = $.trim($(this).find('span.bbsreply').text());
       var itemView = $.trim($(this).find('span.bbsview').text());
       var date = $.trim($(this).find('span.time').text());
       
       var key = url + '\t' + place;
       var value = itemUrl+'\t'+itemLike+'\t'+itemTitle + '\t' + itemAuthor + '\t' + date;
       pushResult(key,  value);
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

