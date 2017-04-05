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

window.attemptedLoads = 0;

var result=[];
//var appendScript = document.createElement('script');
//appendScript.setAttribute("id","jessieknowledgescript");
//appendScript.setAttribute("type","text/javascript");
//var code='document.dispatchEvent(new CustomEvent(\'RW759_connectExtension\', {detail: window}));';
//appendScript.textContent = code;

//document.addEventListener('RW759_connectExtension', function(e) {
//	debugger;
//	if(sourceWindow==null)
//	{
//		sourceWindow=e.detail;
//	}
//});

//function sourceEventFunc(){
//	(document.head||document.documentElement).appendChild(appendScript);
//	s.parentNode.removeChild(s);
//}

//function sourceWindowFuncLoop(){
//	if(sourceWindow==null)
//	{
//		window.clearTimeout(window.pageLoaderPid);
//		sourceEventFunc();
		
//		window.pageLoaderPid=window.setTimeout(sourceWindowFuncLoop,5);
//	}
//	else
//	{
//		chrome.runtime.sendMessage({
//			taskinfo:taskinfo,
//			result:result,
//			links: get_urls(),
//			url:document.location.href
//		});
//	}
//}

function parse()
{
	debugger;
	result.push(sourceWindow);
	chrome.runtime.sendMessage({
		taskinfo:taskinfo,
			result:result,
			links: get_urls(),
			url:document.location.href
	});
	//sourceEventFunc();
	window.pageLoaderPid=window.setTimeout(sourceWindowFuncLoop,1);
}

function finish(){
	if((!document || document.readyState !='complete') && window.attemptedLoads<6) {
		window.clearTimeout(window.pageLoaderPid);
		window.pageLoaderPid = window.setTimeout( finish, 500 );
		window.attemptedLoads++;
		return;
	}
	parse();
}

window.pageLoaderPid = window.setTimeout( finish, 500 );

