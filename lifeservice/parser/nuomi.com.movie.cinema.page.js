var result=[];



function main()
{
    url = window.location.href;
    page = "0";
    cinemaList = $('ul#j-cinema-info-list > li');
    if (cinemaList && cinemaList.length > 0) {
        totalPage = $('div.ui-pager >a.ui-pager-normal:last').text();
        if (totalPage.length === 0) {
            page = "1";
        } else {
            page = totalPage;
        }
    }
   
    pushResult(url, page);
    sendMessage(result);
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


