var result=[];
var url="", title="";

function main()
{
	window.setTimeout(parseBaseInfos, 1 * 1000);
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
    
    var title = $.trim($('div.db_ihead h1').text());
    var enName = $.trim($('div.db_ihead p.db_enname').text());
    var photo = $.trim($('div.db_coverout img').attr('src'));


    var year = $.trim($('div.db_ihead p.db_year').text());
    if (year) {
        year = year.replace(/[\(\)]/g, '');
    }
    
    var runtime = $.trim($('span[property="v:runtime"]').text());

    var typeArray = [];
    $('a[property="v:genre"]').each(function(){
        typeArray.push($.trim($(this).text()));        
    });
    
    var date = $.trim($('a[property="v:initialReleaseDate"]').text());
    var score = $.trim($('div[pan="M14_Movie_Overview_Rating_Final"]').find('b').text());
    
    // 2015年11月6日中国上映 - 2D
    var infoStr = $.trim($('div.otherbox').text());
    if (infoStr && date && date.length > 0) {
        regex = ".*" + date + "(.*?)上映";
        matchGroup = infoStr.match(regex);
        if (matchGroup && matchGroup.length > 0) {
            onlineCountry = matchGroup[1];
            date = date + "(" + onlineCountry + "上映)";
        }
    }
    // displaytype
    var displaytype = '';
    if (infoStr && infoStr.length > 0) {
        regex = ".*上映(.*)$";
        matchGroup = infoStr.match(regex);
        if (matchGroup && matchGroup.length > 0) {
            displaytype = matchGroup[1];
            displaytype = $.trim(displaytype.replace('-', ''));
        }
    }
    
    
    var scoreCount = $.trim($('span#ratingCountRegion').text());
    var wantCount = $.trim($('span#attitudeCountRegion').text());
    
    var releasecountry = $('dd[pan="M14_Movie_Overview_BaseInfo"] > strong:contains("国家地区")').nextAll().text();

    var summary = $.trim($('dt[pan="M14_Movie_Overview_PlotsSummary"] > p').text());
    if (summary) {
        summary = summary.replace(/更多剧情$/, '');
    }
    
    var rank = $.trim($('div.grad_endtxt span.first').text());
    var boxoffice = $.trim($('div.grad_endtxt span:contains("票房")').text());
    if (rank && rank.indexOf("No.") != -1) {
        rank = rank.replace("时光热榜：", "");
        rank = rank.replace("No.", "");
    } else {
        rank = "";
    }
    if (boxoffice && boxoffice.indexOf("票房") != -1) {
        boxoffice = boxoffice.replace("票房：", "");
    } else {
        boxoffice = "";
    }
    
    var online = $('span.sharp_showtime').attr('class');
    var willOnline = $('span.sharp_will').attr('class');
    var onlinestatus = "";
    if (online && online.length > 0) {
        onlinestatus = "正在热映";
    } else if (willOnline && willOnline.length > 0) {
        onlinestatus = "即将上映";
    }
    
    pushResult('url', url);
    pushResult('title', title);

	now = new Date().toLocaleString();
	pushResult('fetch-date', now);

    pushResult('enName', enName);
    pushResult('photo', photo);
    pushResult('displaytype', displaytype);
    pushResult('year', year);
    pushResult('runtime', runtime);
    pushResult('type', typeArray.toString());
    pushResult('date', date);
    pushResult('score', score);
    pushResult('scorecnt', scoreCount);
    //pushResult('wantcnt', wantCount);
    pushResult('releasecountry', releasecountry);
    pushResult('summary', summary);
    pushResult('rank', rank);
    pushResult('boxoffice', boxoffice);
    pushResult('onlinestatus', onlinestatus);
    
    sendResult(result);
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
