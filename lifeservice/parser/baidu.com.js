var result=[];

function main()
{
	var url = window.location.href;
    	var head = $('head').html();
    	var body = $('body').html();
	result.push(url + '\n' + normalStr(head) + normalStr(body));
	
	//sendMessage(result);
	sendResult(result);
}

function normalStr(str) {
    return str.replace(/[\r\n]/g, '').replace(/\s+/g,' ');
}
