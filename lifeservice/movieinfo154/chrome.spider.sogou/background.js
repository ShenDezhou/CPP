// Add listener for message events from the injected spider code.
chrome.extension.onMessage.addListener(
    function(request, sender, sendResponse) {
        if ('result' in request) {
            if(request.taskinfo.taskid=="debugTask" && request.taskinfo.user=="debugUser")
            {
                chrome.tabs.sendMessage(taskTabId, {
                        method:"AddResult",
                        value:request.result[0].result
                    }, function(response) {
                    if (response =="" || response== null){
                         alert('You have close the task tab');
                    }
                });
                closeDebugSpiderWindow();
                window.clearTimeout(debugTask.debugTabWatchDogPid);
            }
            else if(request.taskinfo.taskType=="private")
            {
                spiderInjectCallback(request.taskinfo.taskid,request.links,request.url);
                sendParserResult(request.taskinfo,request.result);
            }
            else //public task
            {
                window.setTimeout(function(){
                    closeSpiderTab(request.taskinfo.taskid);
                },10);
                window.clearTimeout(allTask[request.taskinfo.taskid].newTabWatchDogPid);
                var func=function(taskid,url){
                        
                         window.setTimeout(function(){
                             spiderPublicPage(taskid,url);
                         },allTask[taskid].spiderFreq);
                };
                getTaskUrl(request.taskinfo.taskid,func);
               
               
                sendPublicListResult(request.taskinfo.taskid,request.links,request.url);
                sendParserResult(request.taskinfo,request.result);
            }
        }
        if('listresult' in request)
        {
            if(allTask[request.taskid].taskType=="private")
            {
                spiderInjectCallback(request.taskid,request.links,request.url);
            }
            else
            {
                window.setTimeout(function(){
                    closeSpiderTab(request.taskid);
                },10);
                window.clearTimeout(allTask[request.taskid].newTabWatchDogPid);
                var func=function(taskid,url){
                        
                         window.setTimeout(function(){
                             spiderPublicPage(taskid,url);
                         },allTask[taskid].spiderFreq);
                };
                getTaskUrl(request.taskid,func);
                sendPublicListResult(request.taskid,request.links,request.url);
            }
        }
        if ('stop' in request) {
            if(allTask[request.taskid].started){
                if (request.stop =="Stopping"){
                    setStatus(request.taskid,"Stopped");
                    stopTask(request.taskid);
                }
            }
        }
        if ('pause' in request) {
            if (request.pause =="Resume" && allTask[request.taskid].started && !allTask[request.taskid].paused){
                allTask[request.taskid].paused=true;
            }
            if (request.pause =="Pause" && allTask[request.taskid].started && allTask[request.taskid].paused){
                allTask[request.taskid].paused=false;
                spiderPage(request.taskid);

            }
        }
        if ('start' in request) {
                    taskTabId=sender.tab.id;
                    taskWindowId=sender.tab.windowId;
                    initTask(request);
                    createSpiderWindow();
                    window.setTimeout(function(){startTask(request.taskid)},500); 
        }
        if('error' in request){
                window.setTimeout(function(){
                    closeSpiderTab(request.taskinfo.taskid);
                },10);
                window.clearTimeout(allTask[request.taskinfo.taskid].newTabWatchDogPid);
                var func=function(taskid,url){
                        
                         window.setTimeout(function(){
                             spiderPublicPage(taskid,url);
                         },allTask[taskid].spiderFreq);
                };
                getTaskUrl(request.taskinfo.taskid,func);
        }
        if('debugrun' in request)
        {
            taskTabId=sender.tab.id;
            taskWindowId=sender.tab.windowId;
            initDebugTask(request);
            window.setTimeout(function(){debugRun()},500);
         }
        if('debug' in request)
        {
            taskTabId=sender.tab.id;
            taskWindowId=sender.tab.windowId;
            initDebugTask(request);
            window.setTimeout(function(){debug()},500);
        }
        if('addtask' in request)
        {

                    taskTabId=sender.tab.id;
                    taskWindowId=sender.tab.windowId;
                    addTask(request);
        }
        if("statusChanged" in request)
        {
            publicTaskConfig.runStyle=request.statusChanged;
            publicTaskConfig.taskStyle="particularTask"
            stopPublicTask();
            initPublicTask();
            window.setTimeout(function(){publicTaskDaemon()},500);
        }
    }
 );


chrome.webRequest.onBeforeRequest.addListener(function(details) {
    if(details.tabId<0)
    {
        return {cancel:false};
    }
    var requestTaskid = spiderTabTaskid[details.tabId];
    if(requestTaskid!=null && !(allTask[requestTaskid].loadImg))
     	return {cancel:true};
    return {cancel:false};
}, {urls: ["http://*/*", "https://*/*"],types: ["image", "object"]}, ["blocking"]);


chrome.tabs.onRemoved.addListener(
    function(tabId,removeInfo)
    {
        if(removeInfo.windowId==taskWindowId)
        {
            for(taskid in allTask)
            {
                if(tabId==allTask[taskid].resultsTab.id)
                {
                    stopTask(taskid);
                }               
            }
         }
    }
);

chrome.windows.onRemoved.addListener(
    function(windowId)
    {
        if(windowId==taskWindowId)
        {
            if(spiderWindowId!=null)
                chrome.windows.remove(spiderWindowId);
            resetAllTask();
        }
        if(windowId==spiderWindowId)
        {
            spiderWindowId=null;
            createSpiderWindow();
        }
        if(windowId==spiderDebugWindowId)
        {
            spiderDebugWindowId=null;
        }
    }
);

chrome.windows.onFocusChanged.addListener(
    function(windowId)
    {
//         if(windowId==spiderWindowId)
//         {
//             chrome.windows.update(windowId,{state:"minimized"});
//             alert("please don't change to spider window");
//         }
    }
)



chrome.tabs.onRemoved.addListener(
    function(tabId,removeInfo)
    {
        if(removeInfo.windowId==taskWindowId)
        {
            for(taskid in allTask)
            {
                if(tabId==allTask[taskid].resultsTab.id)
                {
                    stopTask(taskid);
                }               
            }
         }
    }
);


