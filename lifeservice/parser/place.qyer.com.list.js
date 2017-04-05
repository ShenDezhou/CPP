var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

function main()
{
    parseBaseInfos();
    //window.setTimeout(clickFunc,500);
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
    
    var baseUrl = window.location.href;
    var countryCn = $.trim($('p.plcTopBarNameCn').text());
    var countryEn = $.trim($('p.plcTopBarNameEn').text());
    
    $('.plcCitylist > li').each(function(){
        var url = $.trim( $(this).find('h3.title>a').attr('href') );
        var cityFull = $.trim( $(this).find('h3.title>a').text() );
        var cityEn = $.trim( $(this).find('h3.title>a>span').text() );
        var city = cityFull.replace(cityEn, '');
        
        
        
        
        var recommandSights = '';
        $(this).find('p.pois > a').each(function(){
           var sightUrl = $(this).attr('href');
           var sight = $.trim( $(this).text() );
           var recommandSight = sight + '@@@' + sightUrl;
           
           if (recommandSights === '') {
               recommandSights = recommandSight;
           } else {
               recommandSights = recommandSights + ',' + recommandSight;
           }
        });
        
    	if (url && url.length > 5) {
    	    url = url.replace('http://place.qyer.com/', '');
    	    url = url.replace('/', '');
    	    
            //var value = url + "\t" + city + '\t' + recommandSights;
            //pushResult(baseUrl, value);
    	    
    	    var key = countryCn + '\t' + countryEn + '\t' + city;
    	    var value = url;
    	    pushResult(key, value);
    	    
    	}
    });
 
     sendMessage(result);
 
}


/*
 *  本函数内容需要您填充
 *  即抽取在每次翻页时，需要解析的字段内容
 *  常见情况是抽取翻页的评论信息
*/
function parseTurnPageInfos() {
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取评论列表（urserName  userPhoto）为例
    // 与parseBaseInfos抽取基本信息不同， 抽取的是列表型信息，所以使用了.each 
    /*
    // user's comments
    $('#commentlist > li').each(
        function(){
            var userPhoto = $(this).find('.largeavatar > img').attr('src');
            var userName = $(this).find('.largeavatar > span').text();
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
        }
    );
    */
}


/* 本函数内容需要您填充
 * 即找到页面的翻页按钮，没翻一页调用parseTurnPageInfos进行解析
 * 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中翻页为例，代码如下
 * 您需要修改的是 var nextPage = $('.ui_page_item.ui_page_next'); 到您实际的需求
*/
function clickFunc(){
    parseTurnPageInfos();

    // 根据您的需替换下面这一行即可
    var nextPage = $('.ui_page_item.ui_page_next');
    
    if (++current_nextpage_num<=MAX_NEXTPAGE_NUM && nextPage && nextPage.length > 0) {
        nextPage[0].click();
		window.setTimeout(clickFunc, 1000);
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

