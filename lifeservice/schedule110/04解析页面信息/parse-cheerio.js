var fs = require('fs');
var liner = require('./lib/liner');
//var dianpingParser = require('./Parser/dianping-cinema-cheerio-parser.js');
var dianpingParser = require('./Parser/dianping_shop_parser.js');
var booheeParser = require('./Parser/boohee-parser.js');
var jtwsmParser = require('./Parser/jtwsm-parser.js');
var xcArticleParser = require('./Parser/xiancheng-article-parser.js')
var xcThemeDetailParser = require('./Parser/xiancheng-theme-detail-parser.js')
var xcSubjectDetailParser = require('./Parser/xiancheng-subject-detail-parser.js')
var DianpingCommentParser = require('./Parser/dianping-comment-parser.js')

// 参数解析
var argvs = process.argv;
if (argvs.length < 3) {
    console.err('Usage: node parse.js data_path');
    process.exit(-1);
}
var dataFile = argvs[2];
var source = fs.createReadStream(dataFile);
source.pipe(liner);


// 本脚本使用了jsdom，jsdom存在内存泄露问题
// 详情请参考 http://stackoverflow.com/questions/13893163/jsdom-and-node-js-leaking-memory
// 在jsdom项目解决内存泄露之前的解决办法：手动gc
// 后续解决办法：转到 cheerio 阵地
function manualGCMemory() {
	//only call if memory use is bove 500MB
	var heapUsed = process.memoryUsage().heapUsed;
	if (heapUsed > 500 * 1000 * 1000) {
		console.error('[GC]: \t' + new Date().toLocaleString() + '\tHeapUesd: ' + heapUsed.toString());
    		global.gc();
	}
}


// url分发给对应的解析器去解析页面
function urlDispatch(url, content) {
    // 手动GC
    //manualGCMemory()

    if (url.indexOf('http://')!==0 || content.length < 100) {
        console.error("[Error]: " + url);
	return -1;
    }
    if (url.indexOf('.qyer.com') !== -1) {
        qyerParser.parse(url, content);
    } else if (url.indexOf('dianping.com') !== -1 && url.indexOf('/review') !== -1) {
	DianpingCommentParser.parse(url, content)
    } else if (url.indexOf('www.dianping.com') !== -1) {
        dianpingParser.parse(url, content);
    } else if (url.indexOf('www.boohee.com') !== -1) {
        booheeParser.parse(url, content);
    } else if (url.indexOf('m.jtwsm.cn') !== -1) {
        jtwsmParser.parse(url, content);
    } else if (url.indexOf('51xiancheng.com/article') !== -1) {
	xcArticleParser.parse(url, content)
    } else if (url.indexOf('51xiancheng.com/index/hot/detail') !== -1) {
	xcThemeDetailParser.parse(url, content)
    } else if (url.indexOf('51xiancheng.com/subject') !== -1) {
	xcSubjectDetailParser.parse(url, content)
    } else {
        console.error("[Error]: " + url);
	return -1;
    }
    //console.error("[Info]: " + url);

}


// 逐行读取输入文件，并处理
var url, content;
liner.on('readable', function () {
    var line;
    while (line = liner.read()) {
        // do something with line.length
        if (line.length === 0) {
            continue;
        } else if (line.indexOf('http://') === 0) {
            url = line;
        } else if (line.length > 100) {
            content = line;
            urlDispatch(url, content);
        }
    }
});



