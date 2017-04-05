var result=[];
function main()
{
	$($x(".con")).each(
		function()
		{
			result.push(this.textContent);
		}
	);
	sendResult(result);
}
