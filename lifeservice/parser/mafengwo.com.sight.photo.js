var result=[];
var MAX_SCROLL_NUM = 20;
var current_scroll_num = 0;

function main()
{
   window.setTimeout(scrollPage, 1 * 1000);
}


function scrollPage() {
	var scrollHeight = $(document).scrollTop();
	var clientHeight = 400;
	var totalHeight = $(document).height();

	if (scrollHeight + clientHeight + 100 < totalHeight && ++current_scroll_num < MAX_SCROLL_NUM) {
			window.scrollBy(0, clientHeight);
		    window.setTimeout(scrollPage,500);
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
    url = url.replace('photo/', '');
    var photos = '';
    //$('.album-list > ul').each(function() {
     $('#container > ul').each(function() {
        var photo = $(this).find('img').attr('src');
        if (photos === '') {
            photos = photo;
        } else {
            photos = photos + ',' + photo;
        }
    });
	pushResult(url, photos);
    sendMessage(result);
    
}


function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

