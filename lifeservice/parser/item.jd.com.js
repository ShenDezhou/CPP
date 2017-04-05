function get_urls() {
    var urls = [];
    // Copy the link nodelist into an array.
    var a = document.getElementsByTagName('A');
    for (var idx= 0; idx < a.length; ++idx){
        urls.push(a[idx].href);
    }
    // Finding frame URLs using window.frames doesn't work since
    // the framed windows haven't been loaded yet.
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

function parse_url()
{
    /*var result=[];
     var	nodesSnapshot=document.evaluate("/html/body/div[3]/div[2]/div[2]/div[3]/ul/li", document, null,XPathResult.ORDERED_NODE_SNAPSHOT_TYPE,null);
     for (var i=0 ; i < nodesSnapshot.snapshotLength; i++ ){
     result.push( nodesSnapshot.snapshotItem(i).textContent );
     }
     return result;*/

    var result=[];
    var commentDiv = $('#comments-list'),
        items = commentDiv.find('.comments-item');
    items.each(function(){
        var text = $(this).text();
        result.push(text);
    });
    return result;
}

function finish(){
    if((!document || document.readyState !='complete') && window.attemptedLoads<6) {
        window.clearTimeout(window.pageLoaderPid);
        window.pageLoaderPid = window.setTimeout( finish, 500 );
        window.attemptedLoads++;
        return;
    }

    $("#detail-tab-comm").find("a")[0].click();
    setTimeout(function(){
        sendMessage();
    },1000);
    function sendMessage(){
        chrome.runtime.sendMessage({
            links: get_urls(),
            url:document.location.href,
            parsers: parse_url()
        });
        alert(111);
        var commentDiv = $('#comments-list'),
            nextPage = commentDiv.find('.ui-pager-next');
        if(nextPage.length > 0){
            nextPage.click();
            alert(222);
            setTimeout(function(){
                sendMessage();
            }, 1000);
        }
    }


}

window.pageLoaderPid = window.setTimeout( finish, 5000 );
