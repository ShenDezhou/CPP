var result=[];
BASEURL = 'http://www.mafengwo.cn';
NUM_OF_PAGE = 15;

function main()
{
    parseBaseInfos();
    sendMessage(result);
}

// http://www.mafengwo.cn/travel-scenic-spot/mafengwo/10083.html --> 10083
function getIDofUrl(url) {
    var urlSeg = url.split('/');
    return urlSeg[urlSeg.length - 1].replace('.html', '');
}


function getRank(url, rank) {
    url = url.replace(/.*\-/, '');
    pageNum = url.replace('.html', '');
    if (isNaN(parseInt(pageNum))) {
        return rank;
    }
    return (rank + (parseInt(pageNum) - 1) * NUM_OF_PAGE);
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    var url = window.location.href;
    
    cityName = $.trim($('div.m-recList > div.hd > h1').text().replace(/全部.*/, ''));
    
    rank = 0;
    $('ul.poi-list > li').each(function() {
        score = $.trim(normalStr($(this).find('div.grade').text()).replace(/分.*/g, ''));
        shopUrl = BASEURL + $(this).find('div.title > h3 > a').attr('href');
        shopName = $.trim($(this).find('div.title > h3 > a').text());
        revNum = $.trim($(this).find('p.rev-num').text().replace(/条.*/, ''));
        rank += 1;
        key = url + '\t' + cityName + '\t' + shopUrl + '\t' + getRank(url, rank);
        value = shopName + '\t' + score + '\t' + revNum;
        pushResult(key, value);
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

