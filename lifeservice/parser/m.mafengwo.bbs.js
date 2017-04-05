var result=[];
var MAX_NEXTPAGE_NUM = 5;
var current_nextpage_num = 0;

function main()
{
    //parseBaseInfos();
    window.setTimeout(parseBaseInfos,2*1000);
}


/*
 *  本函数内容需要您填充
 *  即在页面加载完成后可以直接解析的字段内容
 *  常见情况是抽取URL 基本信息等内容
*/
// 有好几种样式   http://www.mafengwo.cn/i/3417098.html
//                http://www.mafengwo.cn/i/3290447.html
//
function parseBaseInfos(){
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取(url  title  photo)为例
    // 抽取以jQuery的方式进行，可以根据自己的需求写特定的归一化函数对抽取的值进行归一化，参考 normalStr
    var url = window.location.href;
    var breadcrumb = normalStr($.trim($('.crumb').text()));
    var title = normalStr($.trim($('h1').text()));
    

    var authorName = normalStr($.trim($('.post_info a.name:first').text()));
    var authorName2 = normalStr($.trim( $.trim($('div.person a.per_name').text())));
    
    var authorLevel = normalUserLevel($.trim( $('div.author_info div.out_lv:first').text() ));
    var authorLevel2 = normalUserLevel($.trim($('div.person a.per_grade').text()));

    var date = normalDate($.trim($('.basic-info li:contains("时间") span').text()));
    var date2 = normalStr($.trim($('div.person span.time').text()));
    var date3 = normalStr($.trim($('.travel_detail li span:contains("时间")').next().text()));

    // 查看评论数  30603/295  查看数/评论数
    var viewComment = normalStr($.trim($('div.person span:last').text()));
    
    var playDay = normalDate($.trim($('.basic-info li:contains("天数") span').text()));
    var playDay2 = $.trim($('.travel_detail li span:contains("出行天数")').next().text());
    
    var like = $.trim( $('.num._j_up_num').text() );
    var like2 = $.trim( $('.ding strong').text() );
    
    var content = normalStr($('div.vc_article').html());
    var content2 = normalStr($('.a_con_text:first').html());
    var content3 = normalStr($('.vc_article').html());
    
    // 出行方式
    var playType = normalStr($.trim($('.basic-info li:contains("形式") span').text()));
    var playType2 = normalStr($.trim($('.travel_detail li span:contains("出行方式")').next().text()));

    var avgConsume = normalStr($.trim($('.travel_detail li span:contains("人均费用")').next().text()));
    
    // 是否精华
    var isEssence = '';
    var essenceFlag = $('.digest_icon');
    if (essenceFlag && essenceFlag.length > 0) {
        isEssence = '1';
    }
    
    //var collect = $.trim( $('.bbs_info_content div.names span.collect').text() );
    //var authorPhoto = $.trim( $('.bbs_info_content div.names a > img').attr('src') );
    
    pushResult("url",  url);
    pushResult("title",  title);
    pushResult('breadcrumb', breadcrumb);

    // 作何名称
    pushResult("authorName",  authorName);
    pushResult("authorName",  authorName2);
    // 作者级别
    pushResult("authorLevel",  authorLevel);
    pushResult("authorLevel",  authorLevel2);
    // 游玩时间    
    pushResult("date",  date);
    pushResult("date",  date2);
    pushResult("date",  date3);
    // 游玩天数
    pushResult("playDay",  playDay);
    pushResult("playDay",  playDay2);
    // 喜欢人数
    pushResult("like",  like);
    pushResult("like",  like2);
    //出行方式
    pushResult('playType', playType);
    pushResult('playType', playType2);
    pushResult("isEssence",  isEssence);
    pushResult('avgConsume', avgConsume);
    // 内容
    pushResult("content",  content);
    pushResult("content",  content2);
    pushResult("content",  content3);
    
    pushResult('viewComment', viewComment);

    sendMessage(result);
    // pushResult("authorPhoto",  authorPhoto);
    //pushResult("collect",  collect);
    //pushResult("tags",  tags);
    //pushResult("isEssence",  isEssence);

    
}

// 归一化用户等级
function normalUserLevel(userLevel) {
    if (userLevel && userLevel.length > 0) {
        userLevel = userLevel.replace('LV.', '');
    }
    return userLevel;
}

// 归一化时间
function normalDate(date) {
    if (date && date.length>0) {
        date = date.replace(/\//g, "-");
        date = date.replace('天', '');
    }
    return date;
}




/*
 *  本函数内容需要您填充
 *  即抽取在每次翻页时，需要解析的字段内容
 *  常见情况是抽取翻页的评论信息
*/
function parseTurnPageInfos() {
    // 以 http://place.qyer.com/poi/V2EJalFlBzZTZg/ 网页中抽取评论列表（urserName  userPhoto）为例
    // 与parseBaseInfos抽取基本信息不同， 抽取的是列表型信息，所以使用了.each 
    /*
    // user's comments
    $('#commentlist > li').each(
        function(){
            var userPhoto = $(this).find('.largeavatar > img').attr('src');
            var userName = $(this).find('.largeavatar > span').text();
            pushResult('userName', userName);
            pushResult('userPhoto', userPhoto);
        }
    );
    */
}



function pushResult(key, val) {
    if (key && key.length>0 && val && val.length>0) {
        result.push(key + '\t' + val);
    }
}

function normalStr(str) {
    if (!str) {
        return '';
    }
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}

