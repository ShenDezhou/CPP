var server="http://10.134.14.117/";

function get_urls() {
    var urls = [];
    var a = document.getElementsByTagName('A');
    for (var idx= 0; idx < a.length; ++idx){
        urls.push(a[idx].href);
    }   
    if (window.frames.length) {
        var frames = document.getElementsByTagName('FRAME');
        for (var frame, x = 0; frame = frames[x]; x++) {
            urls.push(frame.src);
        }
        var iframes = document.getElementsByTagName('IFRAME');
        for (var frame, x = 0; frame = iframes[x]; x++) {
            urls.push(frame.src);
        }
    }
    return urls;
}


var ParserResult={
    taskinfo:{},
    links: [],
    url:null,
    result:[]
};


function sendResult(result)
{
    var res={
            tableid:tableId,
            result:result
     };
     ParserResult.result.push(res)
}


function sendMessage(result)
{
     chrome.runtime.sendMessage({
            taskinfo:taskinfo,
            result:result,
            links: get_urls(),
            url:document.location.href
     });
}

function sendAllResult()
{
     ParserResult.taskinfo=taskinfo;
     ParserResult.links=get_urls();
     ParserResult.url=document.location.href;
     chrome.runtime.sendMessage(
        ParserResult
     );
}

function sendListResult(result)
{
     chrome.runtime.sendMessage({
            taskid:taskid,
            listresult:true,
            links: get_urls(),
            url:document.location.href
     });
}

function sendError(result)
{
     chrome.runtime.sendMessage({
            error:result,
            taskid:taskid,
            url:document.location.href
     });
}


function $x(xPath, $scope) {
    var selector = convertXPath(xPath);
    return $(selector, $scope);
}
 
function convertXPath(x) {
 
    //parse //*
    x = replace(x, '//\\*', '');
 
    //parse id
    x = replace(x, '\\[@id="([^"]*)"\\]', '#$1');
 
    //parse [1]
    x = replace(x, '\\[1\\]', ':first');
 
    //parse [n]
    x = replace(x, '\\[([0-9]+)\\]', ':eq($1)');
 
    //parse :eq's and lower 1
    var z = x.split(':eq(');
    x = z[0];
    if (z.length > 1) {
        for (var i = 1; i < z.length; i++) {
            var end = z[i].indexOf(')');
            var number = parseInt(z[i].substr(0, end)) - 1;
            x = x + ':eq(' + number + z[i].substr(end);
        }
    }
 
    //parse /
    x = replace(x, '/', ' > ');
 
    return x;
}
 
function replace(txt, r, w) {
	var re = new RegExp(r, "g");
	return txt.replace(re, w);
}


function onBackgroundMessage(message,sender,sendResponse)
{
    if(message.method=="AddConsoleMessage")//&&message.value.source=="javascript")
    {
     
      $("#console-message").append("<p>Source:"+message.value.source+"</p>");
      $("#console-message").append("<p>Text:"+message.value.text+"</p>");
    }
    if(message.method=="AddResult")
    {
      for(i=0;i<message.value.length;i++)
      {
        $("#console-result").append("<p>"+message.value[i]+"</p>");
      }
    }
}

//system run

chrome.runtime.onMessage.addListener(onBackgroundMessage);

var insertedNode={};
document.addEventListener("DOMNodeInserted", function(e) {
    insertedNode[e.target]=true;
}, false);

$(document).ready(function(){
});


