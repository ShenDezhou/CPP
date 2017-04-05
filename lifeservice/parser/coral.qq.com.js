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
    var commentIframe = document.getElementById('commentIframe'),
        iframeDoc = commentIframe.contentWindow.document,
        lis = $('.post-list li.np-post',iframeDoc);
    lis.each(function(){
        result.push($(this).text());
    });
    return result;
}

function finish(){
    debugger;
    if((!document || document.readyState !='complete') && window.attemptedLoads<6) {
        window.clearTimeout(window.pageLoaderPid);
        window.pageLoaderPid = window.setTimeout( finish, 500 );
        window.attemptedLoads++;
        return;
    }
    var commentIframe = document.getElementById('commentIframe'),
        iframeDoc = commentIframe.contentWindow.document,
        link = iframeDoc.getElementById('loadMore'),
        linkSpan = link.getElementsByTagName('span')[0],
        em = link.getElementsByTagName('em')[0];

    sendMessage();

    function sendMessage(){
        if((linkSpan && linkSpan.style.display != 'none') || (em.className == 'np-load-more-loading' && em.style.display != 'none')){
            linkSpan.click();
            setTimeout(function(){
                sendMessage();
            }, 200);
            return;
        }

        chrome.runtime.sendMessage({
            links: get_urls(),
            url:document.location.href,
            parsers: parse_url()
        });
    }


}

window.pageLoaderPid = window.setTimeout( finish, 500 );
