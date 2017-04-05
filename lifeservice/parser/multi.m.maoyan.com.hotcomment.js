var result=[];
var url="", filmUrl="", title="";
current_scroll_num = 0;
MAX_SCROLL_NUM = 3;

function main()
{
    // 向下滚动一下
    var scrollHeight = $(document).scrollTop();
    var clientHeight = 400;
    var totalHeight = $(document).height();
    while (scrollHeight + clientHeight + 200 < totalHeight && ++current_scroll_num < MAX_SCROLL_NUM) {
        window.scrollBy(0, clientHeight);
        scrollHeight = $(document).scrollTop();
        totalHeight = $(document).height();
    }
    
    
    window.setTimeout(parseBaseInfos, 1 * 1000);
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    url = window.location.href;
    
    /*
    title = $('div.navbar-title').text();
    pushResult('url', url);
    pushResult('title', title);
    */
    

    filmUrl = url.replace(/\/comments.*$/, '');
    title = $.trim($('div.navbar-title').text());
    if (title) {
        title = $.trim(title.replace('短评 -', ''));
    }
    // alert(title + '\t' + filmUrl);
    pushResult('url', filmUrl);
    pushResult('title', title);

    now = new Date().toLocaleString();
    pushResult('fetch date', now);

    pushResult('commentUrl', url);
    $('li.list-view-item').each(function(){
        var cDetailUrl = $.trim($(this).find('a').attr('href'));
        if (cDetailUrl) {
            cDetailUrl = 'http://m.maoyan.com' + cDetailUrl;
        }
        var user = $.trim($(this).find('footer > em').text());
        var userImg = $.trim($(this).find('footer > img').attr('src'));
        
        var starOne = $(this).find('i.icon.icon-star').length;
        var starHalf = $(this).find('i.icon.icon-star-half-o').length;
        var star = starOne + starHalf * 0.5;
        
        var comment = normalStr($.trim($(this).find('p.content').text()));
        var date = $.trim($(this).find('time.timeago').text());
        
        var zanCnt = $.trim($(this).find('i.icon.icon-approve').next().text());
        var commentVal = user+"@@@"+userImg+"@@@"+star+"@@@"+zanCnt+'@@@'+date+"@@@"+cDetailUrl+'@@@'+comment;
        pushResult('comment', commentVal);
        
    });

    sendResult(result);
    /*
    网友头像
    网友昵称
    评价星级
    评价内容
    发布日期
    链接
    短评列表链接
    */
     //sendMessage(result);
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
    return url.replace(/\?.*$/, "");
}
