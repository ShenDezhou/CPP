var result=[];
//var MAX_SCROLL_NUM = 20;
//var current_scroll_num = 0;
var MAX_PHOTO_NUM = 200;
var PHOTO_NUM_PER_SET = 30;
var cur_photo_num = 0;
var last_photo = '';
var photos = '';
var url = '';
var city = '';


function main()
{
    url = window.location.href;
    url = url.replace(/_[0-9]+.html/, '.html');
    city = $.trim($('div.mdd-title > h1').text()).replace('图片', '');
    window.setTimeout(clickNext, 1 * 1000);
}

// 点击查看下一张图片
function clickNext() {
    var nextPhotoBtn = $('div.photo-next');
    if (nextPhotoBtn && cur_photo_num < MAX_PHOTO_NUM) {
        cur_photo_num += 1;
        nextPhotoBtn.click();
        parsrBaseinfo();
    } else {
        sendMessage(result);
    }
}

function parsrBaseinfo() {
    
    // 图片@@@时间
    var photo = $.trim($('div.col-main>div.stage>div.box img').attr('src'));
    var author = $.trim($('div.col-main>div.photo-info span#_j_staguser > a').text());
    var photoInfo = $.trim($('div.col-main>div.photo-info span#_j_staguser').text());
    photoInfo = photoInfo.replace(author, '').replace(/[年月]/g, '-').replace('日', '');
    if (last_photo != photo) {
        last_photo = photo;
        pushResult(url + '\t' + city, photo + '\t' + photoInfo);
        window.setTimeout(clickNext, 5 * 100);
    } else {
        sendMessage(result);
    }
    /*
    if (photos === '') {
        photos = photo + '@@@' + photoInfo;
    } else {
        photos = photos + ',' + photo + '@@@' + photoInfo;
    }
    
    
    if (last_photo != photo) {
        last_photo = photo;
        window.setTimeout(clickNext, 5*100);
    } else {
        sendMessage(result);
    }
    */
    
}


function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

