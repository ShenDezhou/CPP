var timeoutPid=null;
var result=[];

function main()
{
        //alert(typeof jQuery == 'undefined');
    var title = document.getElementsByTagName('h3')[0].innerHTML;
    result.push("htllo!!");
    sendMessage(result);
    /*
	clickFunc();
	timeoutPid=window.setTimeout(parseFuncLoop,1);
	*/
}


function clickFunc(){
	$("#detail-tab-comm").find("a").each(function(){this.click()});
}

function parseFunc(){
	$(".p-comment").each(
		function(){
			result.push($(this).text());
		}
	);
}

function parseFuncLoop(){
	if(result.length==0)
	{
		window.clearTimeout(timeoutPid);
		timeoutPid=window.setTimeout(parseFuncLoop,500);
	}
	else
	{
		sendMessage(result);
	}
	parseFunc();
}

