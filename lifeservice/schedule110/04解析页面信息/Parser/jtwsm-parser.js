var cheerio = require("cheerio");
var fs = require("fs");
var common = require("../lib/common.js");

var pushResult = common.pushResult;
var normalStr = common.normalStr;
var normalMultiLineStr = common.normalMultiLineStr;
var takeFirst = common.takeFirst;
var trim = common.trim;


function parseBaseInfo(url, content) {
	var $ = cheerio.load(content);
	
	var payUrl = $('section#sg-pay dd>a').attr('href');
	if (payUrl) {
		payUrl = "http://m.jtwsm.cn" + payUrl;
	}
	pushResult('url', url);
	pushResult('payUrl', payUrl);
}

// 根据url分发给不同的解析函数
parse = function(url, content) {
    //console.log(url);
    parseBaseInfo(url, content);
}

// 对外开放接口
exports.parse = parse;
