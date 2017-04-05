var result=[];
var MAX_CLICK_NUM = 30;
var cur_click_num = 0;


function main() {
        /*
        var head = document.getElementsByTagName('head')[0],
         script = document.createElement('script');
        script.src = 'https://code.jquery.com/jquery.js';
        head.appendChild(script);
        */
	url = window.location.href;
//	window.setTimeout(clickMoreFilm, 2 * 1000);
	clickMoreFilm();
    
}


function clickMoreFilm() {
    var moreFilmBtn = $('a.moreCinemaListBtn');
    //document.getElementsByClassName("moreCinemaListBtn touching")[0].click();
    
    if (cur_click_num < MAX_CLICK_NUM) {
        cur_click_num += 1;
        moreFilmBtn.click();
        window.setTimeout(clickMoreFilm, 5 * 100);
    } else {
        parseCinemaList();
    }
}


function parseCinemaList() {
	$('div#cinema-list-info > a').each(function(){
		cinemaID = $(this).find('div.cinema-info').attr('data-uid');
		
		cinemaUrl = "https://mdianying.baidu.com/info/cinema/detail?cinemaId=" + cinemaID + "#showing";
		cinemaName = $(this).find('div.name').text();
		cinemaAddr = $(this).find('p.cinema-address').text();
		cinemaInfo = cinemaID + "\t" + cinemaUrl + "\t" + cinemaName + "\t" + cinemaAddr;
		result.push(cinemaInfo);
	});
	sendMessage(result);
}






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




