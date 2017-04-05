var result=[];
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
    var url = window.location.href;
    var title = $.trim($('div#content>h1').text());
    var name = $.trim($('head>title').text().replace(/\(豆瓣\)/, ""));
    var alias = $.trim(title.replace(name, ""));

	pushResult('url', url);
	pushResult('name', normalStr(name));
	pushResult('videoType', 'ACTOR');
	
	pushResult('别名', normalStr(alias));
	var photo = $('div#headline > div.pic img').attr('src');
	pushResult('图片', photo);
	
	// 性别
	var sex = $.trim($('div#headline > div.info li:contains("性别")').text()).replace(/性别:/g, "");
	// 星座
	var constellation = $.trim($('div#headline > div.info li:contains("星座")').text()).replace(/星座:/g, "");
    // 出生日期
    var birthday = $.trim($('div#headline > div.info li:contains("出生日期")').text()).replace(/出生日期:/g, "");
    // 出生地
    var birthplace = $.trim($('div#headline > div.info li:contains("出生地")').text()).replace(/出生地:/g, "");
    // 职业
    var jobs = $.trim($('div#headline > div.info li:contains("职业")').text()).replace(/职业:/g, "");
    // 家庭成员
    var family = $.trim($('div#headline > div.info li:contains("家庭成员")').text()).replace(/家庭成员:/g, "");
    
    pushResult('性别', normalStr(sex));
    pushResult('星座', normalStr(constellation));
    pushResult('出生日期', normalStr(birthday));
    pushResult('出生地', normalStr(birthplace));
    pushResult('职业', normalStr(jobs));
    pushResult('家庭成员', normalStr(family));
    
}




function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

