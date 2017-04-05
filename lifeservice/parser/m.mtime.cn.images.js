var result=[];
MAX_PHOTO_NUM = 10;
cur_photo_num = 0;

function main()
{
    window.setTimeout(clickMorePhoto, 3 * 1000);
    // clickMorePhoto();
    // sendMessage(result);
}


// 点击查看下一张图片
function clickMorePhoto() {
    var morePhotoBtn = $('div.btn_viewmore');
    if (morePhotoBtn && cur_photo_num < MAX_PHOTO_NUM) {
        cur_photo_num += 1;
        morePhotoBtn.click();
        window.setTimeout(clickMorePhoto, 5 * 100);
    } else {
        parseBaseInfo();
        sendMessage(result);
    }
}



/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfo(){
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    var url = window.location.href;
    url = url.replace("posters_and_images/", "");
    var title = $.trim($('div.m_tit > h2').text());
    imageArray = [];
    $('ul#imageList img').each(function(){
         var image = $(this).attr('src');
         if (image) {
             image = normalImage(image);
         }
         if (image !== '') {
             imageArray.push(image);
         }
    });

    pushResult('url', url);    
    pushResult('title', title);
    if (imageArray.length > 0) {
        pushResult('photoCnt', (imageArray.length).toString());
        pushResult('photo', imageArray.toString());
    }
}


function normalImage(imageUrl) {
    //imageRegex = ".*uri=(.*\.jpg)&width=.*";
    
    imageUrl = imageUrl.replace(/.*uri=/, '');    
    imageUrl = imageUrl.replace(/&width=.*$/, ''); 
    imageUrl = imageUrl.replace(/%2[fF]/g, '/');
    imageUrl = imageUrl.replace(/%3[aA]/g, ':');
    return imageUrl;
}


function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function normalUrl(url) {
    return url.replace(/\?.*$/, "");
}
