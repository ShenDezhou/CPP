var result=[];
var MAX_NEXTPAGE_NUM = 10;
var current_nextpage_num = 0;

function main()
{
    window.setTimeout(clickFunc, 1*1000);
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){

    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    var url = window.location.href;
    var photos = '';
    $('.photowrap > ul li').each(function(){
	    var photo = $(this).find('img').attr('src');
	    photo = photo.replace('C_70_70', 'R_1600_10000');
        if (photos === '') {
            photos = photo;
        } else {
            photos = photos + ',' + photo;
        }
    });
    url = normalUrl(url);
    // url: http://
    if (url.length > 7) {
	    pushResult(url, photos);
    }

}




/* 本函数内容需要您填充
 * 即找到页面的翻页按钮，没翻一页调用parseTurnPageInfos进行解析
 * 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中翻页为例，代码如下
 * 您需要修改的是 var nextPage = $('.ui_page_item.ui_page_next'); 到您实际的需求
*/
function clickFunc(){
    // 根据您的需替换下面这一行即可
    var nextPage = $('.right_btn');
    
    if (++current_nextpage_num<=MAX_NEXTPAGE_NUM && nextPage && nextPage.length > 0) {
        nextPage[0].click();
		window.setTimeout(clickFunc, 1000);
		return;
    }
    parseBaseInfos();
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

function normalUrl(url) {
    // http://you.ctrip.com/photos/sight/tokyo294/r13243-7790789.html
    // http://you.ctrip.com/sight/tokyo294/13243.html
    url = url.replace('photos/', '');
    
    matchGroup = url.match(/^(.*)\/r([0-9]+)\-[0-9]+.html/);
    if (matchGroup && matchGroup.length > 2) {
        return matchGroup[1] + '/' + matchGroup[2] + '.html';
    }
    return '';
}
