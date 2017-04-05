function messageDispatch(request, sender, sendResponse) {
    var element = null;
    //what are we using
    switch(request.method){
        case "getElementById":
            element =document.getElementById(request.id);
            break;
        case "getElementsByTag":
            element =document.getElementById(request.id);
            break;
    }
    //what are we doing
    switch(request.action){
        case "getInnerHTML":
            sendResponse(element.innerHTML);
            break;
        case "getValue":
            sendResponse(element.value);
            break;
        case "setInnerHTML":
            element.innerHTML=request.value;
            break;
        case "setValue":
            element.value=request.value;
            break;
        case "insertBodyTR":
            insertBodyTR(request.id,request.value);
            break;
        case "show":
            document.getElementById(request.id).style.display = "inline";
            break;
        case "hide":
            document.getElementById(request.id).style.display = "none";
            break;
    }
}
function clickStop() {

    document.getElementById("stopSpider").value="任务已停止";
    chrome.runtime.sendMessage({
        stop: "Stopping",
        taskid:document.getElementById("taskid").textContent
    });
}

function clickPause() {
    if( document.getElementById("pauseSpider").value == "暂停任务" ){
        document.getElementById("pauseSpider").value="恢复任务";
        chrome.runtime.sendMessage({
               pause: "Resume",
               taskid:document.getElementById("taskid").textContent
    });
    }
    else{
        document.getElementById("pauseSpider").value="暂停任务";
         chrome.runtime.sendMessage({
               pause: "Pause",
               taskid:document.getElementById("taskid").textContent
        });
    }
    
}


function pageLoaded() {
    document.getElementById("stopSpider").addEventListener("click",clickStop);
    document.getElementById("pauseSpider").addEventListener("click",clickPause);
    chrome.runtime.onMessage.addListener(messageDispatch);
}

function insertBodyTR(id,innerHTML){
    var tbody = document.getElementById(id);
    var tr = document.createElement('tr');
    tr.innerHTML += innerHTML
    tbody.appendChild(tr);
}

window.addEventListener("load",pageLoaded);

