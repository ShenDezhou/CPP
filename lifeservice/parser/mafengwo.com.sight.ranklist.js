var result=[];
BASEURL = 'http://www.mafengwo.cn';


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


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    var url = window.location.href;
    
    cityName = $.trim($('div.row-allPlace h2').text().replace(/全部景点.*/, ''));
    
    sightRank = 0;
    $('div.row-allPlace div.list li>a').each(function() {
        sightUrl = BASEURL + $(this).attr('href');
        sightName = $.trim($(this).find('strong').text());
        sightRank += 1;
        key = url + '\t' + cityName + '\t' + sightUrl + '\t' + sightRank;
        value = sightName;
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

