var result=[];
var url="", title="";


function main()
{
    window.setTimeout(parseBaseInfos, 1 * 1000);
    /*
    parseBaseInfos();
    sendMessage(result);
    */
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
    url = url.replace("fullcredits/", "");
    
    pushResult('url', url);
    // http://m.mtime.cn/#!/movie/207337/
    // http://m.mtime.cn/#!/person/913953/
    urlPrefix = "http://m.mtime.cn/#";
    
    // 导演
    $('h2.search_title:contains("导演")').each(function(){
       if ($.trim($(this).text()) === '导演') {
            $(this).next().find('li').each(function(){
                var directorUrl = $(this).attr('data-url');
                if (directorUrl) {
                    directorUrl = urlPrefix + directorUrl;
                }
                
                var directorCHName = $.trim($(this).find('div.dir_txt h2').text());
                var directorENName = $.trim($(this).find('div.dir_txt p.enname').text());
                var directorImg = normalImage($(this).find('div.dir_pic > img').attr('src'));
                
                if (directorCHName.length === 0 && directorENName.length > 0) {
                    directorCHName = directorENName;
                }
        
                var directorInfo = '导演@@@' + directorCHName+'@@@'+directorENName+'@@@'+directorUrl+'@@@'+directorImg + '@@@@@@';
                // pushResult('directorInfo', directorInfo);
                pushResult('actorInfo', directorInfo);
            });
       }
    });
        
    // 演员表
    $('ul.director.actor > li').each(function() {
        var actorUrl = $(this).attr('data-url');
        if (actorUrl) {
            actorUrl = urlPrefix + actorUrl;
        }

        var actorCHName = $.trim($(this).find('div.dir_txt h2').text());
        var actorENName = $.trim($(this).find('div.dir_txt p.enname').text());
        var actorImg = normalImage($(this).find('div.dir_pic > img').attr('src'));
        var characterName = $.trim($(this).find('div.dir_txt span:contains("饰")').text());
        var characterImg = normalImage($(this).find('div.dir_spic > img').attr('src'));
        
        if (actorCHName.length === 0 && actorENName.length > 0) {
            actorCHName = actorENName;
        }
        if (characterName) {
            characterName = characterName.replace('饰：', '');
            characterName = $.trim(characterName.replace('(uncredited)', ''));
        }
        var actorInfo = "演员@@@"+actorCHName+'@@@'+actorENName+'@@@'+actorUrl+'@@@'+actorImg+'@@@'+characterName+'@@@'+characterImg;
        pushResult('actorInfo', actorInfo);
    });

    sendMessage(result);
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

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

function normalUrl(url) {
    return url.replace(/\?.*$/, "");
}
