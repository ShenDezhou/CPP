var result=[];

function main() {
    parseFun();
}


function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}



function parseFun(){
    // basic info
    var url = window.location.href;
    var title = $.trim($('div.dish-name > h1').text());
    var price = normalStr($('div.dish-name > span.dish-price').text());
    // 推荐标签
    var dishTagArray = [];
    $('div.picture-tag > a').each(function(){
         dishTagArray.push($.trim($(this).text()));
    });
    
    var photoArray = [];
    $('div.menu-pic-list img').each(function(){
        photoArray.push($(this).attr('src')); 
    });
    
    // 基本信息保存
	pushResult('url', url);
    pushResult('title', title);
    pushResult('dishPrice', price);
    pushResult('dishTags', dishTagArray.toString());
    pushResult('dishPphotos', photoArray.toString());
    
	sendMessage(result);
}
