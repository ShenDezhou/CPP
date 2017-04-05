var result=[];
cur_click_num = 0;
MAX_CLICK_NUM = 10;

function main()
{
    window.setTimeout(clickMoreFilm, 1 * 1000);
    /*
    parseBaseInfos();
    sendMessage(result);
    */
}


function clickMoreFilm() {
    debugger;
    var moreFilmBtn = $('div.mp-more-ajax');
    if (moreFilmBtn && cur_click_num < MAX_CLICK_NUM) {
        cur_click_num += 1;
        moreFilmBtn.click();
        window.setTimeout(clickMoreFilm, 5 * 100);
    } else {
        parseBaseInfos();
        sendMessage(result);
    }
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    url = window.location.href;
    
    
    var filmIDArray = [];
    $('ul.movie-list > li').each(function() {
       var filmUrl = 'http:' + $(this).find('a.item.ma-no').attr('href');
       var title = $.trim($(this).find('div.cont > h4 > span:first').text());
       var shortdesc = $.trim($(this).find('div.cont p.desc').text());
       var wantcnt = $.trim($(this).find('span.info > em').text());
       if (!(wantcnt && parseInt(wantcnt) > 10)) {
           wantcnt = '';
       }
       
       // 猫眼电影id
       var filmID = normalFilmID(filmUrl);
       filmIDArray.push(filmID);
       pushResult(title, shortdesc + '\t' + wantcnt + '\t' + filmUrl + '\t' + filmID);
    });
    
    // 猫眼电影评论的URL
    pushKey("\n\n评论URL");
    for (var idx in filmIDArray) {
        pushKey("http://m.maoyan.com/movie/" + filmIDArray[idx] + "/comments?_v_=yes");
    }
    
    //sendMessage(result);
}

function pushKey(key) {
    if (key && key.length>0) {
        result.push(key);
    }
}

function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function normalFilmID(url) {
    id = url.replace('?_v_=yes', '');   
    id = id.replace(/.*movie\//, '');
    return id;
}