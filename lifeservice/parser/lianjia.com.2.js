var result=[];
function main()
{
	$($x(".div-cun")).each(
		function()
		{
			result.push(this.textContent);
		}
	);
	sendResult(result);
}