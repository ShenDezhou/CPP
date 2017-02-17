// 解析时候常用的公共函数
var fs = require("fs");

// log 
exports.LOG = function(message) {
    //console.log(message);
    test(message);
};

// 归一化一行数据的换行
exports.normalStr = function(str) {
    if (str) {
        return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
    }
    return '';
};

// 归一化多行，给多行的数据
exports.normalMultiLineStr = function(str) {
    str = str.replace(/[\r\n]/g, '###').replace(/\s+/g,' ');
    str = str.replace(/######/g, '###');
    return str;
};


// 以追加的方式写文件
function write (file, writeStr) {
    fs.appendFile(file, writeStr, function (error) {
        if (error) {
            console.error("[Error]: " + content);
        }
    });
};

// 将输出写入文件中
// 存在异步写入顺序乱序的问题
exports.storeResult = function (file, key, val) {
    if (key && key.length>0 && val && val.length>0) {
        var writeLine = key + '\t' + val + '\n';
        write(file, writeLine);
    }
};


// 将解析结果输出到标准输出
exports.pushResult = function (key, val) {
    if (key && key.length>0 && val && val.length>0) {
        console.log(key + '\t' + val);
        //return -1;
    }
};


exports.takeFirst = function (valueArray) {
    for (idx in valueArray) {
        if (valueArray[idx].length !== 0) {
            return valueArray[idx];
        }
    }
    return '';
};

exports.trim = function (val) {
	if (!val || val.length === 0) {
		return '';
	}
    val = val.replace(/^[ \s]+/g, '').replace(/[ \s]+$/g, '');
	return val;
}

