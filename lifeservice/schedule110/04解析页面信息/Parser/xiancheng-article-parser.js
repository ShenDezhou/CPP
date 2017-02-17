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
	
	var crumb = trim($('p.bnav-xc').text());
	if (crumb) {
		crumb = crumb.replace(/.*>/, '');
	}
	var reason = trim($('div.reason-text').text());
	var title = trim($('div.artcile-detail > h2').text());
	var shopname = trim($('div.shop-msg h3').text());
	var addr = trim($('div.shop-msg strong:contains("地址：")').parent().text());
	var time = trim($('div.shop-msg strong:contains("时间：")').parent().text());
	var price = trim($('div.shop-msg strong:contains("价格：")').parent().text());
	var tel = trim($('div.shop-msg strong:contains("联系：")').parent().text());
	if (reason) { reason = trim(reason.replace('推荐理由', '')); }
	if (addr) { addr = trim(addr.replace('地址：', '')); }
	if (time) { time = trim(time.replace('时间：', '')); }
	if (price) { price = trim(price.replace('价格：', '')); }
	if (tel) { tel = trim(tel.replace('联系：', '')); }
	
	
	//var detail = normalStr($('div.artcile-detail').last().html());
	//if (detail) {
	//	detail = detail.replace(/&#x/g, "\\u");
	//	Detail = detail.replace(/;/g, "");
	//}
	var shopInfo = crumb + '\t' + title + '\t' + reason + '\t' + shopname + '\t' + addr + '\t' + time + '\t' + price + '\t' + tel;
	
	if (title.length > 0) {
		pushResult(url, shopInfo);
	}
	
}


// 根据url分发给不同的解析函数
parse = function(url, content) {
    parseBaseInfo(url, content);
}

// 对外开放接口
exports.parse = parse;
