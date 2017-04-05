var result=[];

function main()
{
    var url = window.location.href;
    //var fetchTime = formatDate();
    //pushResult('fetch time', fetchTime);
    
    baseUrl = url.replace("/cinema/", "");
    $('div#j-district-item-wrap > a').each(function(){
        areaName = $.trim($(this).text());
        areaUrl = $(this).attr('href');
        
        areaUrl = baseUrl + areaUrl.replace("#cinema-nav", "");
        areaName = areaName.replace(/[\d\s]+$/g, "");
        if (areaName !== "全部" && areaName !== "地铁附近") {
            pushResult(url, areaName + '\t' + areaUrl);
        }
    });
    //sendMessage(result);
    sendResult(result);
}



// 获取当前时间
function formatDate() {
    var today = new Date();
    var dateStr = today.toISOString().substring(0, 10);
    var timeStr = today.getHours() + ":" + today.getMinutes() + ":" + today.getSeconds();
    return dateStr + " " + timeStr;
}


function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}


