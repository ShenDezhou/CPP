var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

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

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}


function parseFunc(){
    // basic info
    var url = window.location.href;
    //var breadcrumb = $.trim($('.l-column2__main li.c-breadcrumb__item.is-current').text());   // need handle
    var title = $.trim($('.rd-header__headline > h2.rd-header__rst-name > small.rd-header__rst-name-ja').text()).replace(/[\(\)]/g, '');
    var enName = $.trim($('.rd-header__headline > h2.rd-header__rst-name > a.rd-header__rst-name-main').text());

    pushResult('url', url);
    pushResult('title', title);
    pushResult('enName', enName);

    // food photos
	var foodPhoto = [];
    $('.c-grids.rd-grids  p.rd-grids__photo-img').each( function(){
        var photo = $(this).find('a').attr('href');
		if (photo && photo.length>0) {
			foodPhoto.push(photo);
		}
    });
	// 每页的图片都是不同的URL
	pushResult('foodPhoto', foodPhoto.toString());
}

                                           
