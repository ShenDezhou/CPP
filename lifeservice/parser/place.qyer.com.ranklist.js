var result=[];
var NUM_OF_PAGE = 15;

function main()
{
    parseFunc();
    sendMessage(result);
}


    

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}


function getRank(url, rank) {
    pageNum = url.replace(/.*page=/, '');
    if (isNaN(parseInt(pageNum))) {
        return rank;
    }
    return (rank + (parseInt(pageNum) - 1) * NUM_OF_PAGE);
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function parseFunc(){
    var url = window.location.href;
    var cityName = $.trim($('div.plcTopBarL p.plcTopBarNameCn').text());

    rank = 0;
	$('ul.plcPoiList > li').each(function(){
	   var itemUrl = $(this).find('h3.title > a').attr('href');
	   var itemName = $.trim( normalStr($(this).find('h3.title > a').text()) );
	   var itemScore = $.trim( $(this).find('div.info > span.grade').text() );
	   var itemRevNum = $.trim( $(this).find('div.info > span.dping').text().replace(/人.*/, '') );
	  
	   rank += 1;
	  
	   var itemRank = getRank(url, rank);
	   key = url + '\t' + cityName + '\t' + itemUrl + '\t' + getRank(url, rank);
       value = itemName + '\t' + itemScore + '\t' + itemRevNum;
	   pushResult(key, value);
	});

}

