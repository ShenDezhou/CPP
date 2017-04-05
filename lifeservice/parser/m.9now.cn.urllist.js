var result=[];
var MAX_SCROLL_NUM = 500;
var current_scroll_num = 0;
var urlPrefix = 'http://m.9now.cn';

function main()
{
   window.setTimeout(scrollPage, 1 * 1000);
}


function scrollPage() {
	var scrollHeight = $(document).scrollTop();
	var clientHeight = 400;
	var totalHeight = $(document).height();
    
    //if (scrollHeight + clientHeight + 100 < totalHeight && ++current_scroll_num < MAX_SCROLL_NUM) {
	//if (scrollHeight < totalHeight && ++current_scroll_num < MAX_SCROLL_NUM) {
	if (++current_scroll_num < MAX_SCROLL_NUM) {
			window.scrollBy(0, clientHeight);
		    window.setTimeout(scrollPage,300);
		    return;
	} 
	parseBaseInfos();
}


function parseBaseInfos(){
    /* 
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    */
    var url = window.location.href;
    
    $('li.shop-item').each(function() {
        shopUrl = urlPrefix + $(this).find('a').attr('href');
       	pushResult(url, shopUrl);     
    });
	
    	   pushResult("scrollHeight", scrollHeight.toString());
	    pushResult("clientHeight", clientHeight.toString());
	    pushResult("totalHeight", totalHeight.toString());
	    pushResult("current_scroll_num", current_scroll_num.toString());
	    sendMessage(result);
}


function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

