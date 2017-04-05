var result=[];


function main() {
    parseBaseInfos();
}



/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    
    var url = window.location.href;
    var category = normalStr($.trim($('p.m-crm').text()));
    if (category) {
        categoryArray = category.split(' > ');
        if (categoryArray.length === 4) {
            category = $.trim(categoryArray[2]);
        }
    }
    var like = $.trim($('div.m-goods span.num').text());
    var poi = '';
    var poiStr = $.trim($('a#showVenueMap').attr('map-src'))
    if (poiStr) {
        poiRegex = 'marker={y:([0-9\.]+),x:([0-9\.]+)';
        poiMatch = poiStr.match(poiRegex);
        if (poiMatch) {
            poi = poiMatch[1] + ',' + poiMatch[2];
        }
    }
    payUrl = url;
    var address = $.trim($('div.m-sdbox.m-venue p.txt').text());
    var tel = $('strong.hotline').text();
    var content = normalStr($.trim($('div.itm-tab.z-show').html()));
    var tips = normalStr($('dl.infoitm > dt.tt > span:contains("温馨提示")').parent().next().html());
    info = category + '\t' + like + '\t' + poi + '\t' + payUrl + '\t' + address + '\t' + tel + '\t' + tips + '\t' + content;
    pushResult(url, info);
    
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

