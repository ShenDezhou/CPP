var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

function main()
{
    window.setTimeout(parseBaseInfos,2000);
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
    */
    var url = window.location.href;
    url = url.replace(/photo.*/, '');
    var photos = '';
    $('ul.pla_photolist > li').each(function() {
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

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

