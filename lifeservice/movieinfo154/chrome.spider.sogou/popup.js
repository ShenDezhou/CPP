function notRunClick()
{
     chrome.runtime.sendMessage({
           statusChanged:"notRun"
     });
}

function startRunClick()
{
     chrome.runtime.sendMessage({
           statusChanged:"startRun"
     });
}


function pageLoaded() {
}

  
window.addEventListener("load",pageLoaded);

$(document).ready(function(){
        $("#notRun").click(notRunClick);
        $("#startRun").click(startRunClick);
});



