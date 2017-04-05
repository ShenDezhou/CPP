var ROOT_PAGE = "[root page]";
var taskTabId=null;
var taskWindowId=null;
var spiderWindowId = null;
var spiderTabTaskid = {};
var allTask={};

var spiderDebugWindowId=null;
var debugTask=null;

var publicTaskConfig={};

//var receiveApi="http://10.134.14.117:8080/recordtable/recordservice/data/put";
var receiveApi="http://10.134.14.117/multiReceiveResult.php";
var allTaskServiceUrlApi="http://10.134.34.33:8080/taskservice/task?limit=100";
var getUrlApi="http://10.134.34.33:8080/taskservice/task/{taskid}/url?count=2";
var postAnchorApi="http://10.134.34.33:8080/taskservice/task/{taskid}/url";
var taskScriptServer="http://10.134.34.33/";

function resetAllTask()
{
    allTask={};
    taskTabId=null;
    spiderWindowId=null;
    taskWindowId=null;
}
/**
init vars
 */
function createDebugTaskLoadPage(tryCount,tryInterval)
{
    var loadStr='window.attemptedLoads = 0;\n'+'window.pageLoaderPid = window.setTimeout( finish, '+tryInterval+');\n'+
                'function finish(){\n'+'if((!document || document.readyState !=\'complete\') && window.attemptedLoads<'+tryCount+') {\n'+
                'window.clearTimeout(window.pageLoaderPid);'+'window.pageLoaderPid = window.setTimeout( finish, '+tryInterval+');\n' +
                'window.attemptedLoads++;'+'return;' +'}\n'+'debugger;\nmain();\nsendAllResult();\n}\n';
    return loadStr;
}

function createTaskLoadPage(tryCount,tryInterval)
{
        var loadStr= 'window.attemptedLoads = 0;\n'+'window.pageLoaderPid = window.setTimeout( finish, '+tryInterval+');\n'+
                    'function finish(){\n'+'if((!document || document.readyState !=\'complete\') && window.attemptedLoads<'+tryCount+') {\n'+
                    'window.clearTimeout(window.pageLoaderPid);'+'window.pageLoaderPid = window.setTimeout( finish, '+tryInterval+');\n' +
                    'window.attemptedLoads++;'+'return;' +'}\n';
       
       return loadStr;
}

function createTaskSendResult(tableId)
{
       var loadStr='function sendResult(result){var res={tableid:"'+tableId+'",result:result};ParserResult.result.push(res);dtd.resolve();}\n'
       return loadStr;
}

function createTaskListPageParser(taskid,tryCount,tryInterval)
{
  
    var loadStr='var taskid=\"'+taskid+'\";\nwindow.attemptedLoads = 0;\n'+'window.pageLoaderPid = window.setTimeout( finish, '+tryInterval+');\n'+
                'function finish(){\n'+'if((!document || document.readyState !=\'complete\') && window.attemptedLoads<'+tryCount+') {\n'+
                'window.clearTimeout(window.pageLoaderPid);'+'window.pageLoaderPid = window.setTimeout( finish, '+tryInterval+');\n' +
                'window.attemptedLoads++;'+'return;' +'}\n'+'try\n{'+'sendListResult();\n'+'}catch(e){\n'+'sendError(e);}}\n';
    return loadStr;
}

function createTaskInfo(taskid,user,timestamp,taskType)
{
    var info='var taskinfo={taskid:"'+taskid+'",\nuser:"'+user+'",\ntimestamp:'+timestamp+',\ntaskType:"'+taskType+'"};';
    return info;
}

function createDebugSpiderWindow(request)
{
    if(spiderDebugWindowId!=null)
        return;
    else{
        if('debugrun' in request)
        {
            chrome.windows.create({
                focused: false
            }, function(window){
                spiderDebugWindowId=window.id
                chrome.windows.update(window.id,{state:"minimized"});
            });
        }
        if('debug' in request)
        {
            chrome.windows.create({
                focused: true
            }, function(window){
                spiderDebugWindowId=window.id
            });
        }
    }
        
}

function closeDebugSpiderWindow()
{
    if(spiderDebugWindowId!=null)
    {
         chrome.windows.remove(spiderDebugWindowId);
         spiderDebugWindowId=null;
    }
}




function createSpiderWindow()
{
    if(spiderWindowId!=null)
    {
            chrome.windows.get(spiderWindowId,function(w){
                if(chrome.runtime.lastError)
                {
                    spiderWindowId=null;
                    console.log("window is not exist,wait for create new one");
                    window.setTimeout(createSpiderWindow,100);
                }
            });
    }
    if(spiderWindowId==null||spiderWindowId==taskWindowId)
    {
        chrome.windows.create({
            focused: false
        }, function(window){
            spiderWindowId=window.id
            chrome.windows.update(window.id,{state:"minimized"});
        });
    }
}

function closeSpiderWindow()
{
    if(spiderWindowId!=null)
    {
        chrome.tabs.getAllInWindow(spiderWindowId,function(tabs){
            if(tabs.length==1)
                chrome.windows.remove(spiderWindowId);
                spiderWindowId=null;
        })
    }
}

function initDebugTask(request)
{
    createDebugSpiderWindow(request);
    debugTask={
        server:request.server,
        url:request.url,
        parserUrl:request.parserUrl,
        tryCount:request.tryCount,
        tryInterval:request.tryInterval,
        pageLoadMaxWait:request.pageLoadMaxWait,
        debugTabId:null,
        debugTabWatchDogPid:0,
        actualCode:null
    };

    setDebugStatus();
    var xhr = new XMLHttpRequest();
    xhr.open("GET", debugTask.server+debugTask.parserUrl,false);
    xhr.onreadystatechange = function(){
    if ( xhr.readyState == 4 ) {
        if ( xhr.status == 200 ) {
            debugTask.actualCode=createTaskInfo("debugTask","debugUser",0,"private")+'\n'+createTaskSendResult("0")+createDebugTaskLoadPage(debugTask.tryCount,debugTask.tryInterval)+xhr.responseText;
        } else {
            debugTask.actualCode="";
        }
       }
    };
    xhr.send(null);
    
}



function initTask(request) { 
    var d=new Date();
    var task={
        taskid:request.taskId,
        //user:request.user,
        user:"system",
        regex:request.regex,
        listRegex:request.listregex,
        parser:request.parser,
        seedUrl:request.seeds,
        depth:request.depth,
        timestamp:d.getTime(),
        server:taskScriptServer,
        description:request.description,
        taskType:request.taskType,
        bufferCount:request.bufferCount,

        loadImg:true,
        spiderFreq:request.spiderFreq*1000,
        pageLoadMaxWait:request.pageLoadMaxWait*1000,
        pageFailedRetryCount:3,
        tryCount:request.tryCount,
        tryInterval:request.tryInteval*1000,

        httpRequest:null,
        httpRequestWatchDogPid:0,
        newTabWatchDogPid:0,

        injectScript:"",
        pagesTodo:{},
        pagesDone:{},
        pagesDepth:{},
        pagesFailed:{},
        pagesResultBuffer:[],
        
        listPageParser:null,
        started:true,
        paused:false,
        currentRequest:{
            requestedURL:null,
            returnedURL:null,
            referrer:null
        },
        spiderTab:null,
        resultsTab:null,
        taskType:"particular"
    }; 
    if(request.listregex==null||request.listregex==""||request.listregex.match("^ *$")||request.listregex=="*"||request.listregex==".*")
            task.listRegex="^jessieblankurl$";
    if(request.depth<0){
        task.depth=100000;
    }
    
    if(task.taskType=="private")
    {
        for(i=0;i<task.seedUrl.length;i++)
        {
            task.pagesTodo[task.seedUrl[i]]=ROOT_PAGE;
            task.pagesDepth[task.seedUrl[i]]=0;
        }
    }
    if(publicTaskConfig.runStyle=='notRun')
    {
        task.started=false;
        task.paused=true;
    }
    task.listPageParser=createTaskListPageParser(task.taskid,task.tryCount,task.tryInterval);
    
    task.injectScript=createTaskInfo(task.taskid,task.user,task.timestamp,task.taskType)+createTaskLoadPage(task.tryCount,task.tryInterval);
    var DeferredString="$.when(";
    for(i=0;i<task.parser.length;i++)
    {
        var xhr = new XMLHttpRequest();
        xhr.open("GET", task.parser[i].parserUrl, false);
        xhr.onreadystatechange = function(){
            if ( xhr.readyState == 4 ) {
                if ( xhr.status == 200 ) {
                    task.parser[i].pagesParser=xhr.responseText;
                    task.injectScript+='\n\n'+
                                        'var dtd'+task.parser[i].tableId.replace(/-/g,"")+'=$.Deferred();'+                                 
                                       'function run'+task.parser[i].tableId.replace(/-/g,"")+'(dtd){\n'
                                        +createTaskSendResult(task.parser[i].tableId)
                                        +'\n'+xhr.responseText+'\nmain();\n'+'return dtd}'
                    if(i==task.parser.length-1)
                        DeferredString+='run'+task.parser[i].tableId.replace(/-/g,"")+'('+'dtd'+task.parser[i].tableId.replace(/-/g,"")+')).done';
                    else
                        DeferredString+='run'+task.parser[i].tableId.replace(/-/g,"")+'('+'dtd'+task.parser[i].tableId.replace(/-/g,"")+'),';
                } else {
                    task.parser[i].pagesParser="";
                }
            }
        };
        xhr.send(null);
    }

    task.injectScript+="\n"+DeferredString+"(function(){sendAllResult()});}";
    
    allTask[request.taskId]=task;
}



function trimAfter(string, sep) {
    var i=0;
    while(1)
    {
        var div=string.indexOf(sep,i);
        if(div===-1)
            return string;
        else if(string[div+1]=='!')
            i=div+1;
        else
            return string.substring(0, div);
    }
}

/**
 * Start a spidering session.
 * Called by the popup's Go button.
 */
function startTask(taskid) {
    function resultsLoadCallback_(tab) {
        allTask[taskid].resultsTab = tab;
        window.setTimeout(resultsLoadCallbackDelay_, 100);
    }
    function resultsLoadCallbackDelay_(){
        chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
            method:"getElementById",
            id:"taskid",
            action:"setInnerHTML",
            value:setInnerSafely(taskid)
        });
        chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
            method:"getElementById",
            id:"description",
            action:"setInnerHTML",
            value:setInnerSafely(allTask[taskid].description)
        });
        chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
            method:"getElementById",
            id:"startingOn",
            action:"setInnerHTML",
            value:setInnerSafely(allTask[taskid].seedUrl)
        });
        chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
            method:"getElementById",
            id:"restrictTo",
            action:"setInnerHTML",
            value:setInnerSafely(allTask[taskid].regex)
        });
        if(allTask[taskid].listRegex=="^jessieblankurl$")
        {
            chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
                method:"getElementById",
                id:"listRestrict",
                action:"setInnerHTML",
                value:setInnerSafely("Skip...")
            });
        }
        else
        {
            chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
                method:"getElementById",
                id:"listRestrict",
                action:"setInnerHTML",
                value:setInnerSafely(allTask[taskid].listRegex)
            });
        }
        var storeUrl =allTask[taskid].server+"result"+"/"+allTask[taskid].user+"/"+allTask[taskid].taskid+"/" + allTask[taskid].timestamp;
        var storeInfo = '<a target="_blank" href="' + storeUrl +'">'+storeUrl+'</a>';
        chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
            method:"getElementById",
            id:"store",
            action:"setInnerHTML",
            value:storeInfo
        });
 
       
        // Start spidering.
        allTask[taskid].started = true;
        spiderPage(taskid);
    }
    // Open a tab for the results.
    chrome.tabs.create({
        windowId:taskWindowId,
        url: chrome.extension.getURL('crawler/results.html')
    }, resultsLoadCallback_);
}

/**
 * Set the innerHTML of a named element with a message.  Escape the message.
 * @param {Document} doc Document containing the element.
 * @param {string} id ID of element to change.
 * @param {*} msg Message to set.
 */
function setInnerSafely(msg) {
    msg = msg.toString();
    msg = msg.replace(/&/g, '&amp;');
    msg = msg.replace(/</g, '&lt;');
    msg = msg.replace(/>/g, '&gt;');
    return msg;
}

function stopTask(taskid) {
    writeBackBuffer(taskid);
    allTask[taskid].started= false;
    allTask[taskid].pagesTodo = {};
    closeSpiderTab(taskid);
    allTask[taskid].spiderTab = null;
    allTask[taskid].resultsTab = null;
    window.clearTimeout(allTask[taskid].httpRequestWatchDogPid);
    window.clearTimeout(allTask[taskid].newTabWatchDogPid);
    delete allTask[taskid];
    var havetask=null;
    for(havetask in allTask){
         break;
    }
    if(!havetask)
         closeSpiderWindow();
  
}


function writeBackBuffer(taskid)
{
    while(pair=allTask[taskid].pagesResultBuffer.pop())
    {
        taskinfo=pair.taskinfo;
        result=pair.result;
        for(i=0;i<result.length;i++)
        {
            if(result[i].result===null||result[i].result===undefined||result[i].result==="")
                continue;
            var xhr = new XMLHttpRequest();
            xhr.open("POST", receiveApi, true);
            //xhr.setRequestHeader('Authorization','Bearer '+accessToken);
            xhr.setRequestHeader('Content-type','application/json; charset=utf-8');
            var msg={
                taskid:taskinfo.taskid,
                tableid:result[i].tableid,
                user:taskinfo.user,
                timestamp:taskinfo.timestamp,
                result:result[i].result
            }
            var rstr=JSON.stringify(msg);
            xhr.send(rstr);
        }
       
      
    }    
    allTask[taskid].usedCount=0;
}

function sendPublicListResult(taskid,links,url)
{
    var listLinks=[];
    var linkSet={};
    for (var x = 0; x < links.length; x++) {
        var link = links[x];
        link = trimAfter(link, '#');  // Trim off any anchor.
        if(link&&!(link in linkSet) && (link.match(allTask[taskid].listRegex)||link.match(allTask[taskid].regex)))
        {
            linkSet[link]=true;
            listLinks.push(link);
        }
    }
    var xhr = new XMLHttpRequest();
    xhr.open("POST", postAnchorApi.replace("{taskid}",taskid), true);
    xhr.setRequestHeader('Content-type','application/json; charset=utf-8');
    var listResult=[{url:url,linkList:listLinks}];
    xhr.send(JSON.stringify(listResult));
    
}

function sendParserResult(taskinfo,result)
{
    var pair={};
    pair.taskinfo=taskinfo;
    pair.result=result;
    allTask[taskinfo.taskid].pagesResultBuffer.push(pair);
    if(allTask[taskinfo.taskid].pagesResultBuffer.length<allTask[taskinfo.taskid].bufferCount)
    {
        return;
    }
    else
    {
        writeBackBuffer(taskinfo.taskid);
    }
}

function spiderPublicPage(taskid,url){
    allTask[taskid].currentRequest ={
        requestedURL:null,
        returnedURL:null,
        referrer:null
    };

    if(allTask[taskid].paused){
        return;
    }  
  
    // Record page details.
    allTask[taskid].currentRequest.referrer=allTask[taskid].pagesTodo[url];
    allTask[taskid].currentRequest.requestedURL =url;
    delete allTask[taskid].pagesTodo[url];

    //Fetching
    allTask[taskid].currentRequest.returnedURL = null;
    allTask[taskid].newTabWatchDogPid = window.setTimeout(function(){newPublicTabWatchDog(taskid)}, allTask[taskid].pageLoadMaxWait);

    if(!(allTask[taskid].currentRequest.requestedURL.match(allTask[taskid].listRegex)))
    {
        chrome.tabs.create({
            windowId:spiderWindowId,
            url: allTask[taskid].currentRequest.requestedURL,
            active:true
        }, spiderLoadCallback_);
        function spiderLoadCallback_(tab) {
            allTask[taskid].spiderTab = tab;
            //setStatus(taskid,'Spidering details:' + allTask[taskid].spiderTab.url);
            spiderTabTaskid[tab.id]=taskid;

            chrome.tabs.executeScript(allTask[taskid].spiderTab.id, {
                    code: allTask[taskid].injectScript
            },function(){
                        if (chrome.extension.lastError){
                           chrome.runtime.sendMessage({
                                taskinfo:{taskid:taskid},
                                error:"true"
                            });
            }});

        }
    }
    else
    {
        chrome.tabs.create({
            windowId:spiderWindowId,
            url: allTask[taskid].currentRequest.requestedURL,
            active:true
        }, spiderLoadListCallback_);
        function spiderLoadListCallback_(tab) {
            allTask[taskid].spiderTab = tab;
            //setStatus(taskid,'Spidering lists:' + allTask[taskid].spiderTab.url);
            spiderTabTaskid[tab.id]=taskid;
            chrome.tabs.executeScript(allTask[taskid].spiderTab.id, {
                code: allTask[taskid].listPageParser
            },function(){
                        if (chrome.extension.lastError){
                           chrome.runtime.sendMessage({
                               taskinfo:{taskid:taskid},
                                error:"true"
                            });
            }});
        }
    }
}

/**
 * Start spidering one page.
 */
function spiderPage(taskid) {
    allTask[taskid].currentRequest ={
        requestedURL:null,
        returnedURL:null,
        referrer:null
    };

    if(allTask[taskid].paused){
        return;
    }
    setStatus(taskid,'Next page...');
    if (!allTask[taskid].resultsTab) {
        // Results tab was closed.
        return;
    }

    // Pull one page URL out of the todo list.
    var url = null;
    for (url in allTask[taskid].pagesTodo) {
        break;
    }
    if (!url) {
        // Done.
        setStatus(taskid,'Complete');
        stopTask(taskid);
        return;
    }
    // Record page details.
    allTask[taskid].currentRequest.referrer=allTask[taskid].pagesTodo[url];
    allTask[taskid].currentRequest.requestedURL =url;
    delete allTask[taskid].pagesTodo[url];
    allTask[taskid].pagesDone[url] = true;

    //Fetching
    allTask[taskid].currentRequest.returnedURL = null;
    setStatus(taskid,'Fetching ' + allTask[taskid].currentRequest.requestedURL);
    allTask[taskid].newTabWatchDogPid = window.setTimeout(function(){newTabWatchDog(taskid)}, allTask[taskid].pageLoadMaxWait);

    if(!(allTask[taskid].currentRequest.requestedURL.match(allTask[taskid].listRegex)))
    {
        chrome.tabs.create({
            windowId:spiderWindowId,
            url: allTask[taskid].currentRequest.requestedURL,
            active:true
        }, spiderLoadCallback_);
        function spiderLoadCallback_(tab) {
            allTask[taskid].spiderTab = tab;
            setStatus(taskid,'Spidering details:' + allTask[taskid].spiderTab.url);
            spiderTabTaskid[tab.id]=taskid;
            chrome.tabs.executeScript(allTask[taskid].spiderTab.id, {
                    code: allTask[taskid].injectScript
            });
            
        }
    }
    else
    {
        chrome.tabs.create({
            windowId:spiderWindowId,
            url: allTask[taskid].currentRequest.requestedURL,
            active:true
        }, spiderLoadListCallback_);
        function spiderLoadListCallback_(tab) {
            allTask[taskid].spiderTab = tab;
            setStatus(taskid,'Spidering lists:' + allTask[taskid].spiderTab.url);
            spiderTabTaskid[tab.id]=taskid;
            chrome.tabs.executeScript(allTask[taskid].spiderTab.id, {
                code: allTask[taskid].listPageParser
            });
        }
    }
    
    
}

/**
 * Terminate an http request that hangs.
 */
function httpRequestWatchDog(taskid) {
    setStatus(taskid,'Aborting HTTP Request');
    if (allTask[taskid].httpRequest) {
        if(!(allTask[taskid].currentRequest.requestedURL in allTask[taskid].pagesFailed))
            allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL]=1;
        else
            allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL]++;
        var tryTimes=allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL];
        if(tryTimes<allTask[taskid].pageFailedRetryCount)
        {
            allTask[taskid].pagesTodo[allTask[taskid].currentRequest.requestedURL]="Retry:"+tryTimes;
            delete allTask[taskid].pagesDone[allTask[taskid].currentRequest.requestedURL];
        }
        
        allTask[taskid].httpRequest.abort();
        // Log your miserable failure.
        allTask[taskid].currentRequest.returnedURL=null;
        recordPage(taskid,allTask[taskid].currentRequest);
        allTask[taskid].httpRequest = null;

    }
    window.setTimeout(function(){spiderPage(taskid)}, 1);
}

function newPublicTabWatchDog(taskid) {
    closeSpiderTab(taskid);
    if(!(allTask[taskid].currentRequest.requestedURL in allTask[taskid].pagesFailed))
            allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL]=1;
    else
            allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL]++;
    var tryTimes=allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL];
    if(tryTimes<allTask[taskid].pageFailedRetryCount)
    {
            allTask[taskid].pagesTodo[allTask[taskid].currentRequest.requestedURL]="Retry:"+tryTimes;
            delete allTask[taskid].pagesDone[allTask[taskid].currentRequest.requestedURL];
    }
    // Log your miserable failure.
    allTask[taskid].currentRequest.returnedURL=null;

}

/**
 * Terminate a new tab that hangs (happens when a binary file downloads).
 */
function newTabWatchDog(taskid) {
    if(allTask[taskid].taskType=="private")
    {
        setStatus(taskid,'Aborting New Tab');
    }
    closeSpiderTab(taskid);
    if(!(allTask[taskid].currentRequest.requestedURL in allTask[taskid].pagesFailed))
            allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL]=1;
    else
            allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL]++;
    var tryTimes=allTask[taskid].pagesFailed[allTask[taskid].currentRequest.requestedURL];
    if(tryTimes<allTask[taskid].pageFailedRetryCount)
    {
            allTask[taskid].pagesTodo[allTask[taskid].currentRequest.requestedURL]="Retry:"+tryTimes;
            delete allTask[taskid].pagesDone[allTask[taskid].currentRequest.requestedURL];
    }
    // Log your miserable failure.
    allTask[taskid].currentRequest.returnedURL=null;
    
    recordPage(taskid,allTask[taskid].currentRequest);

    window.setTimeout(function(){spiderPage(taskid)}, 1);
}

function debugTabWatchDog() {
    closeDebugSpiderWindow();
}

function setDebugStatus()
{

}

function debug()
{
    if(debugTask.actualCode=="")
    {
        setDebugStatus();
        return;
    }
    
    debugTask.debugTabWatchDogPid = window.setTimeout(function(){debugTabWatchDog()}, debugTask.pageLoadMaxWait);
    chrome.tabs.create({
        windowId:spiderDebugWindowId,
        url: debugTask.url,
        active:true
    }, debugSpiderLoadCallback_);
    function debugSpiderLoadCallback_(tab) {
        var version = "1.0";
        chrome.debugger.attach({tabId:tab.id}, version,onAttach.bind(null, tab.id));
        debugTask.debugTabId = tab.id;
        chrome.tabs.executeScript(debugTask.debugTabId, {
            code: debugTask.actualCode
        });
        function onAttach(tabId) {
            if (chrome.runtime.lastError) {
                alert(chrome.runtime.lastError.message);
                return;
            }
            chrome.debugger.sendCommand({tabId:tabId}, "Debugger.enable", {},onDebuggerEnabled.bind(null, tabId));
            function onDebuggerEnabled(debuggeeId) {
                //chrome.debugger.sendCommand({tabId:debuggeeId}, "Page.enable");
                chrome.debugger.sendCommand({tabId:debuggeeId}, "Debugger.pause");
//                 chrome.debugger.sendCommand({tabId:debuggeeId}, "Debugger.stepOver");
//                 chrome.debugger.sendCommand({tabId:debuggeeId}, "Debugger.stepInto");
//                 chrome.debugger.onDetach.addListener(onDetach);
//                 function onDetach(debuggeeId,reason) {
//                     if(reason=="replaced_with_devtools")
//                     {
//                         chrome.debugger.sendCommand(debuggeeId, "Debugger.stepOver");
//                         chrome.debugger.sendCommand(debuggeeId, "Debugger.stepInto");
//                     }
//                     alert(reason);
//                 }
            }
       }
    }
}


function debugRun()
{
    if(debugTask.actualCode=="")
    {
        setDebugStatus();
        return;
    }
    
    debugTask.debugTabWatchDogPid = window.setTimeout(function(){debugTabWatchDog()}, debugTask.pageLoadMaxWait);
    chrome.tabs.create({
        windowId:spiderDebugWindowId,
        url: debugTask.url,
        active:true
    }, debugSpiderLoadCallback_);
    function debugSpiderLoadCallback_(tab) {
        var version = "1.0";
        chrome.debugger.attach({tabId:tab.id}, version,onAttach.bind(null, tab.id));
    
        debugTask.debugTabId = tab.id;
        chrome.tabs.executeScript(debugTask.debugTabId, {
            code: debugTask.actualCode
        });
        
        function onAttach(tabId) {
            if (chrome.runtime.lastError) {
                alert(chrome.runtime.lastError.message);
                return;
            }
            chrome.debugger.sendCommand({tabId:tabId}, "Console.enable");
            chrome.debugger.onEvent.addListener(onEvent);
            function onEvent(debuggeeId, message, params) {
              if (tabId != debuggeeId.tabId)
                    return;
              if (message == "Console.messageAdded") {
                   chrome.tabs.sendMessage(taskTabId, {
                        method:"AddConsoleMessage",
                        value:params.message
                    }, function(response) {
                       if (response =="" || response== null){
                           alert('You have close the task tab');
                        }
                   });
               }  
            }
         }
     }    

}


function stopPublicTask()
{
    for(taskid in allTask)
    {
        if(allTask[taskid].taskType=="particular"||allTask[taskid].taskType=="random")
        {
           stopTask(taskid);
        }
    }
   
}

function initPublicTask()
{
         
    allTask={};
    chrome.storage.sync.get({user:"system"},
        function(items)
        {
            publicTaskConfig.user=items.user;
        }
    );
    var xhr = new XMLHttpRequest();
    xhr.open("GET", allTaskServiceUrlApi,false);
    xhr.onreadystatechange = function(){
    if ( xhr.readyState == 4 ) {
        if ( xhr.status == 200 ) {
             var obj = JSON.parse(xhr.responseText);
             var i=0;
            for(i=0;i<obj.rows.length;i++){
                initTask(obj.rows[i]);
            }
                
        } else {
                
        }
       }
    };
    xhr.send(null);
   
   
}

function publicTaskDaemon()
{
    if(publicTaskConfig.runStyle=='startRun')
    {
        createSpiderWindow();
        window.setTimeout(startPublicTask,100);
    }
    if(publicTaskConfig.runStyle=="notRun")
    {
        for(taskid in allTask)
        {
            stopTask(taskid);
        }
    }

}

function isEmpty(hashs)
{
    var havetask=null;
    for(havetask in hashs){
         break;
    }
    if(!havetask)
        return true;
    else
        return false;
}

function startPublicTask()
{
    if(publicTaskConfig.taskStyle=="particularTask")
    {
        var taskid;
        for(taskid in allTask)
        {
            if(allTask[taskid].taskType=="particular")
            {
                getTaskUrl(taskid,function(t,url){
                         window.setTimeout(function(){
                             spiderPublicPage(t,url);
                         },allTask[t].spiderFreq);
                })
          
            }
        }
    }
    if(publicTaskConfig.taskStyle=="randomTask")
    {

    }
}


function getTaskUrl(taskid,callback)
{
       if(allTask[taskid].paused){
            return;
       } 
       var url = null;
       for (url in allTask[taskid].pagesTodo) {
            break;
       }
       if(url!==null){
            var t=taskid;
            console.log("ignore net get");
            callback(t,url);
            return;
       }
       else
       {
           var xhr = new XMLHttpRequest();
           console.log(getUrlApi.replace("{taskid}",taskid));
           xhr.open("GET", getUrlApi.replace("{taskid}",taskid), true);
           xhr.onreadystatechange = function(){
                if ( xhr.readyState == 4 ) {
                    if ( xhr.status == 200 ) {
                        var obj = JSON.parse(xhr.responseText);
                        var i=1;
                        for(;i<obj.length;i++)
                        {
                            allTask[taskid].pagesTodo[obj[i]]="ROOT_PAGE";
                        }
                        if(obj.length>0)
                        {
                            var t=taskid;
                            console.log("get url from net:"+obj);
                            callback(t,obj[0]);
                            return;
                        }    
                        else
                        {
                             console.log(taskid+":notask");
                             window.setTimeout(function(){getTaskUrl(taskid,callback)},1000);
                        }
                        return;

                    }
                }
            };
            xhr.send(null);
       }
}


function updatePublicTaskStatus()
{

}






function addTask(request)
{
    chrome.storage.sync.get({tasks:{}}, function(items) {
        var tasks=items.tasks;
        tasks[request.taskid]=request;
        tasks[request.taskid].paused=false;
        chrome.storage.sync.set({tasks:items.tasks})
    });
}
/**
 * Process the data returned by the injected spider code.
 * @param {Array} links List of links away from this page.
 * @param {Array} inline List of inline resources in this page.
 */
function spiderInjectCallback(taskid,links, url) {
    window.clearTimeout(allTask[taskid].newTabWatchDogPid);

    setStatus(taskid,'Scanning ' + url);
    allTask[taskid].currentRequest.returnedURL =url;

    // In the case of a redirect this URL might be different than the one we
    // marked spidered above.  Mark this one as spidered too.
    allTask[taskid].pagesDone[url] = true;


    // Add any new links to the Todo list.
    for (var x = 0; x < links.length; x++) {
        var link = links[x];
        link = trimAfter(link, '#');  // Trim off any anchor.
        if(link && !(link in allTask[taskid].pagesDone)&& !(link in allTask[taskid].pagesTodo)&& link.match(allTask[taskid].listRegex))
        {
            allTask[taskid].pagesDepth[link]=0;
            allTask[taskid].pagesTodo[link] =url;

        }
        if (link && !(link in allTask[taskid].pagesDone) && !(link in allTask[taskid].pagesTodo)&&allTask[taskid].pagesDepth[allTask[taskid].currentRequest.requestedURL]<allTask[taskid].depth&&link.match(allTask[taskid].regex)) {
            allTask[taskid].pagesTodo[link] =url;
            if(!(link in allTask[taskid].pagesDepth) || allTask[taskid].pagesDepth[link]>allTask[taskid].pagesDepth[allTask[taskid].currentRequest.requestedURL]+1)
            {
                        allTask[taskid].pagesDepth[link]=allTask[taskid].pagesDepth[allTask[taskid].currentRequest.requestedURL]+1;
            }
        }
    }
    // Close this page and mark done.
    recordPage(taskid,allTask[taskid].currentRequest);

    //We want a slight delay before closing as a tab may have scripts loading
    window.setTimeout(function(){
        closeSpiderTab(taskid);
    },10);
    window.setTimeout(function(){
        spiderPage(taskid);
    },allTask[taskid].spiderFreq);
}


function closeSpiderTab(taskid){
    console.log("close tab:",allTask[taskid].currentRequest.requestedURL);
    if(allTask[taskid].spiderTab!=null && allTask[taskid].spiderTab.id in spiderTabTaskid)
    {
        delete spiderTabTaskid[allTask[taskid].spiderTab.id];
    }
    if (allTask[taskid].spiderTab)
    {
        chrome.tabs.remove(allTask[taskid].spiderTab.id,function(){
             if (chrome.runtime.lastError) {
                console.log(chrome.runtime.lastError.message);     
                return;
            }
        });
        allTask[taskid].spiderTab = null;
    }

}
/**
 * Record the details of one url to the results tab.
 */
function recordPage(taskid,req) {
    if (req.requestedURL!=null && (req.returnedURL ==null)) {
        var codeclass = 'x0';
        req.returnedURL = "Error"
    }
    if(req.referrer.match("^Retry:[0-9]*$"))
    {
        var codeclass = 'x0';
    }
    var requestedURL = '<a href="' + req.requestedURL + '" target="spiderpage" title="' + req.requestedURL + '">' + req.requestedURL + '</a>';
    var value ='<td>' + requestedURL + '</td>' +
    '<td class="' + codeclass + '"><span title="' + req.returnedURL + '">' + req.returnedURL + '</span></td>' +
    '<td class="' + codeclass + '"><span title="' + req.referrer + '">' + req.referrer + '</span></td>';

    chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
        id:"resultbody",
        action:"insertBodyTR",
        value:value
    });
}

function setStatus(taskid,msg) {
    if(allTask[taskid].started){
        try{
            chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
                method:"getElementById",
                id:"stopSpider",
                action:"getValue"
            }, function(response) {
                if (taskid in allTask && allTask[taskid].started && (response =="" || response== null)){
                    stopTask(taskid);
                    //alert('Lost access to results pane. Halting.');
                }
            });
            chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
                method:"getElementById",
                id:"queue",
                action:"setInnerHTML",
                value:Object.keys(allTask[taskid].pagesTodo).length
            });
            chrome.tabs.sendMessage(allTask[taskid].resultsTab.id, {
                method:"getElementById",
                id:"status",
                action:"setInnerHTML",
                value:setInnerSafely(msg)
            });
        }catch(err){
            stopTask(taskid);
        }
    }
}



