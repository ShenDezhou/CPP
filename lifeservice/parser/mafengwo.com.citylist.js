var current_nextpage_num = 0;
var MAX_NEXTPAGE_NUM = 10;
var result=[];


function main()
{
    // 解析热门城市
    //parseHotCitylist();
    //sendMessage(result);
    
    // 解析全部城市
    getAllCitys();
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
// 热门城市列表
function parseHotCitylist(){
    var url = window.location.href;
    var countryName = $('div.r-main h1').text();
    var countryID = getIDofUrl(url);
    $('div.row-allPlace li>a').each(function() {
        cityID = getIDofUrl($(this).attr('href'));
        cityName = $.trim($(this).text());
        key = countryID + '\t' + countryName + '\t' + cityID;
        value = cityName;
        pushResult(key, value);
    });
}


// 解析全部城市列表
function parseAllCityList() {
    var countryName = $('div.hm-bar > div.hd > dl > dt > a').text();
    var countryUrl = $('div.hm-bar > div.hd > dl > dt > a').attr('href');
    countryUrl = countryUrl.replace('.html', '');
    countryID = countryUrl.replace(/.*\//, '');
    
    $('div.poi-mList li').each(function(){
        var cityID = $(this).attr('data-id');
        var cityName = $(this).find('h3>a').text();
        var key = countryID + '\t' + countryName + '\t' + cityID;
        pushResult(key, cityName);
    })
}


function getAllCitys(){
    parseAllCityList();

    var nextPage = $('a.btn-next');
    var disableNextPage = $('a.btn-next.disable');
    if (++current_nextpage_num<=MAX_NEXTPAGE_NUM && nextPage && disableNextPage.length <= 0) {
        nextPage[0].click();
		window.setTimeout(getAllCitys, 1000);
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

