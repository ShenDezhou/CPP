var result=[];


function main()
{
    window.setTimeout(parseBaseInfos, 5 * 1000);
    //parseBaseInfos();
    //sendMessage(result);
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
function parseBaseInfos(){
    
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    var url = window.location.href;
    /*
    var title = $('header h2').text();
    pushResult('url', url);
    pushResult('title', title); 
    */
    
    url = url.replace('videos/', '');
    var title = $.trim($('div.m_tit > h2').text());    
    if (title) {
        title = title.replace(' - 预告片&拍摄花絮', '');
    }


    pushResult('url', url);
    pushResult('title', title);
    
    $('ul#vedioList > li').each(function(){
         var image = $.trim($(this).find('img').attr('src') );
         if (image) {
             image = normalImage(image);
         }
         var time = $.trim($(this).find('div.newstxt p').text() );
         if (time) {
             time = time.replace('片长：', '');
         }
         var videoInfoStr = $.trim($(this).attr('data-video') );
         var videoUrl = $.trim(parseJson(videoInfoStr, 'url')); 
         var title = $.trim(parseJson(videoInfoStr, 'title')); 
        
        videoInfo = title + '@@@' + image + '@@@' + videoUrl + '@@@' + time;
        pushResult('video', videoInfo);
        
    });
 
 
    sendMessage(result);   
}



function parseJson(jsonStr, key) {
    regex = '"' + key + '":"([^"]+)"';
    matchGroup = jsonStr.match(regex);
    if (matchGroup && matchGroup.length > 0) {
        return matchGroup[1];
    }
    return '';
}


function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function normalImage(imageUrl) {
    //imageRegex = ".*uri=(.*\.jpg)&width=.*";
    
    imageUrl = imageUrl.replace(/.*uri=/, '');    
    imageUrl = imageUrl.replace(/&width=.*$/, ''); 
    imageUrl = imageUrl.replace(/%2[fF]/g, '/');
    imageUrl = imageUrl.replace(/%3[aA]/g, ':');
    return imageUrl;
}


function normalUrl(url) {
    return url.replace(/\?.*$/, "");
}
