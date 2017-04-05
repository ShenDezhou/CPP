var result=[];
var url="", title="";


function main()
{
    //window.setTimeout(parseBaseInfos, 1 * 1000);
    
    parseBaseInfos();
    //sendMessage(result);
    
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos() {
    
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    url = window.location.href;
    
    filmIDArray = [];
    $('div.movielist').each(function(){
        var filmUrl = $(this).find('div.upmovie_txt a').attr('href');
        var shortdesc = $.trim($(this).find('p.movie_tip').text());
        
        filmID = filmUrl.replace('#!/movie/', '');
        filmID = filmID.replace('/', '');
        filmIDArray.push(filmID);
        
                
        filmUrl = "http://movie.mtime.com/" + filmID + "/";
        pushResult('shortdesc', filmUrl + '\t' + shortdesc);
    });
    
/*  
    wapUrlPrefix = "http://m.mtime.cn/#!/movie/";
    // 视频片花的URL
    pushKey("\n\n片花URL task-123");
    for (var idx in filmIDArray) {
        //pushResult('videoUrl', wapUrlPrefix + id + "/videos/");
        pushKey(wapUrlPrefix + filmIDArray[idx] + "/videos/");
    }
    // 演员表的URL
    pushKey("\n\n演员表URL task-121");
    for (idx in filmIDArray) { 
        //pushResult('actorUrl', wapUrlPrefix + id + "/fullcredits/");
        pushKey(wapUrlPrefix + filmIDArray[idx] + "/fullcredits/");
    }
    
    // 剧照的URL
    pushKey("\n\n剧照URL task-122");
    for (idx in filmIDArray) {
        //pushResult('photoUrl', wapUrlPrefix + id + "/posters_and_images/");
        pushKey(wapUrlPrefix + filmIDArray[idx] + "/posters_and_images/");
    }
*/
    //sendMessage(result);
    sendResult(result);
}


function normalImage(imageUrl) {
    //imageRegex = ".*uri=(.*\.jpg)&width=.*";
    if (!imageUrl) {
        return '';
    }
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

function pushKey(key) {
    if (key && key.length>0) {
        result.push(key);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function normalUrl(url) {
    return url.replace(/\?.*$/, "");
}
