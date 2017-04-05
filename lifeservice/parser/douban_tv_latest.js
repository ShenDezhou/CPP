var result=[];
var url, title, prefix

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
    
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    url = window.location.href;
    title = $.trim($('div#content>h1>span:first').text());
	var name = $.trim($('head>title').text().replace(/\(豆瓣\)/, ""));
    var alias = $.trim(title.replace(name, ""));

    if(name.length === 0) {
        return;
    }
    prefix = "-1\t" + url + "\t" + name + '\t';

	pushResult('url', url);
	pushResult('name', name);
	pushResult('别名', alias);
	
	// 电影 or 电视剧
	var videoType = '电视剧';
	var str = $.trim($('div#recommendations>h2').text());
	if (str && str.indexOf('电影') !== -1) {
	    videoType = '电影';
	}
	pushResult('videoType', videoType);
	
	var photo = $('div#mainpic img').attr('src');
	pushResult('图片', photo);
	
	// 年份
	var year = $.trim($('div#content>h1>span:last').text().replace(/[\(\)]/g, ""));
	pushResult('上映年份', year);
    // 评分
	var score = $.trim($('strong.rating_num').text());
	pushResult('评分', score);
	
    // 导演
    $('div.subjectwrap div#info span:contains("导演")').next().find('a').each(function() {
        var tmpUrl = $(this).attr("href");
        var tmpName = $.trim($(this).text());
        if (tmpUrl.length > 0 && tmpUrl.indexOf('celebrity') !== -1) {
            pushResult('导演', tmpName + "@@@" + doubanBaseUrl + tmpUrl);
        } else if (tmpName.indexOf('更多') === -1){
            pushResult('导演', tmpName);
        }
    });
    // 编剧
    $('div.subjectwrap div#info span:contains("编剧")').next().find('a').each(function() {
        var tmpUrl = $(this).attr("href");
        var tmpName = $.trim($(this).text());
        if (tmpUrl.length > 0 && tmpUrl.indexOf('celebrity') !== -1) {
            pushResult('编剧', tmpName + "@@@" + doubanBaseUrl + tmpUrl);
        } else if (tmpName.indexOf('更多') === -1){
            pushResult('编剧', tmpName);
        }
    });

    // 主演
    $('div.subjectwrap div#info span.actor > span:contains("主演")').next().find('a').each(function() {
        var tmpUrl = $(this).attr("href");
        var tmpName = $.trim($(this).text());
        if (tmpUrl.length > 0 && tmpUrl.indexOf('celebrity') !== -1) {
            pushResult('主演', tmpName + "@@@" + doubanBaseUrl + tmpUrl);
        } else if (tmpName.indexOf('更多') === -1){
            pushResult('主演', tmpName);
        }
    });

    // 类型
    $('div.subjectwrap div#info span[property="v:genre"]').each(function() {
        var tmpName = $.trim($(this).text());
        if (tmpName.length > 0) {
            pushResult('类型', tmpName);
        }
    });


    // 上映日期(电影)   首播时间(电视剧)
    $('div.subjectwrap div#info span[property="v:initialReleaseDate"]').each(function() {
        var tmpName = $.trim($(this).text());
        if (tmpName.length > 0) {
            pushResult('首播时间', tmpName);
        }
    });
	
	// 电影片长
	$('div.subjectwrap div#info span[property="v:runtime"]').each(function() {
        var tmpName = $.trim($(this).text());
        if (tmpName.length > 0) {
            pushResult('单集片长', tmpName);
        }
    });
    
    // 无法用CSS选择器获取的数据
    var infoStr = $.trim($('div.subjectwrap div#info').html());
    
    // 制片地区
    var makePlaceRegex = ">制片国家\/地区:</span>([^<]+)<br>";
    matchGroup = infoStr.match(makePlaceRegex);
    if (matchGroup && matchGroup.length > 1) {
        pushResult('制片地区', matchGroup[1]);
    }
    
    // 语言
    var languageRegex = ">语言:</span>([^<]+)<br>";
    matchGroup = infoStr.match(languageRegex);
    if (matchGroup && matchGroup.length > 1) {
        pushResult('语言', matchGroup[1]);
    }
    
    // 集数
    var numberRegex = ">集数:</span>([^<]+)<br>";
    matchGroup = infoStr.match(numberRegex);
    if (matchGroup && matchGroup.length > 1) {
        pushResult('集数', matchGroup[1]);
    }
    
    // 单集片长
    var singleTimeRegex = ">单集片长:</span>([^<]+)<br>";
    matchGroup = infoStr.match(singleTimeRegex);
    if (matchGroup && matchGroup.length > 1) {
        pushResult('单集片长', matchGroup[1]);
    }   
    
    // 别名
    var aliasRegex = ">又名:</span>([^<]+)<br>";
    matchGroup = infoStr.match(aliasRegex);
    if (matchGroup && matchGroup.length > 1) {
        pushResult('别名', matchGroup[1]);
    }
    // 标签
    $('div.tags-body>a').each(function(){
        var tag = $.trim($(this).text());
        pushResult('标签', tag);
    });
    
    // 相关推荐
    $('div.recommendations-bd > dl > dd > a').each(function(){
         var tmpName = $.trim($(this).text());
         var tmpUrl = normalUrl($.trim($(this).attr('href')));
         if (tmpUrl.length > 0) {
            pushResult('相关推荐', tmpName + '@@@'+ tmpUrl);
         }
    });
    
    // 剧情简介
    var summary = normalStr($.trim($('span[property="v:summary"]').text()));
    pushResult('剧情简介', summary);
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
