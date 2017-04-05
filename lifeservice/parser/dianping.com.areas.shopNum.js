var result=[];

function main()
{
    parseBaseInfos();
    sendMessage(result);
}

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    var url = window.location.href;
    var city = $.trim($('a.J-city').text());
    var shopNum = $.trim($('div.J_bread span.num').text().replace(/[\(\)]/g, ""));
    pushResult(url + '\t' + city, shopNum);
}