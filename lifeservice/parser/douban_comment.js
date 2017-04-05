var result=[];
var doubanBaseUrl = "http://movie.douban.com";

function main()
{
    parseBaseInfos();
    sendMessage(result);   
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    
	var url = window.location.href;
	$('div.comment-item').each(function(){
		// var date = $.trim($(this).find('span.rating').next().text());
		
		var star = $.trim($(this).find('span.rating').attr('class'));
		if (star && star.length > 0) {
		    star = star.replace(' rating', '').replace('allstar', '');
		} else {
		    star = '30';
		}
		var vote = $(this).find('span.votes').text();
		var comment = $(this).find('p').text();
		
        pushResult('date', star + '\t' + vote + '\t' + normalStr(comment)); 
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

