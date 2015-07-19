<?php
$htid = $_GET["htid"];

$vis = $_GET['visualize'];

$url = $_GET['url'];
$sha1 = $_GET['sha1'];
$fast=0;
if ($url && $sha1){
//	echo 'fast';
	$fast=1;
	$unique_idx = $htid;
}


if ($vis<1){
	$vis = 0;
}
else{
	$vis = 1;
}



$mysqli = new mysqli("localhost", "memex", "darpamemex", "imageinfo");

/* check connection */
if (mysqli_connect_errno()) {
    printf("Connect failed: %s\n", mysqli_connect_error());
    exit();
}
if (!$fast){
$query = "SELECT f.uid,u.location,u.sha1 FROM fullIds f left join uniqueIds u on f.uid=u.htid where f.htid=". $htid;
//echo $query;
if ($stmt = $mysqli->prepare($query)) {

    /* execute statement */
    $stmt->execute();

    /* bind result variables */
    $stmt->bind_result($unique_idx,$url,$sha1);

    /* fetch values */
    $stmt->fetch();
    

    /* close statement */
    $stmt->close();
}
}
if ($vis){
    echo '<script src="http://code.jquery.com/jquery-2.0.3.min.js"></script>';
    echo '<script type="text/javascript">(function(e,t){var a=function(){var e={value:.05,reveal:true,revealonclick:false};var t=arguments[1]||{};var a=this,n=a.parentNode;if(typeof t!=="object"){t={value:parseInt(arguments[1])}}t=function(){var n={};for(var i in e){if(a.hasAttribute("data-"+i)){n[i]=a.getAttribute("data-"+i);continue}if(i in t){n[i]=t[i];continue}n[i]=e[i]}return n}();var i=a.style.display,r=a.width,l=a.height,o=false;var d=document.createElement("canvas");d.width=r;d.height=l;var f=d.getContext("2d");f.mozImageSmoothingEnabled=false;f.webkitImageSmoothingEnabled=false;f.imageSmoothingEnabled=false;var u=r*t.value,s=l*t.value;f.drawImage(a,0,0,u,s);f.drawImage(d,0,0,u,s,0,0,d.width,d.height);a.style.display="none";n.insertBefore(d,a);if(t.revealonclick!==false&&t.revealonclick!=="false"){d.addEventListener("click",function(e){o=!o;if(o){f.drawImage(a,0,0,r,l)}else{f.drawImage(a,0,0,u,s);f.drawImage(d,0,0,u,s,0,0,d.width,d.height)}})}if(t.reveal!==false&&t.reveal!=="false"){d.addEventListener("mouseenter",function(e){if(o)return;f.drawImage(a,0,0,r,l)});d.addEventListener("mouseleave",function(e){if(o)return;f.drawImage(a,0,0,u,s);f.drawImage(d,0,0,u,s,0,0,d.width,d.height)})}};e.HTMLImageElement.prototype.pixelate=a;if(typeof t==="function"){t.fn.extend({pixelate:function(){return this.each(function(){a.apply(this,arguments)})}});t(e).on("load",function(){t("img[data-pixelate]").pixelate()})}else{document.addEventListener("DOMContentLoaded",function(e){var t=document.querySelectorAll("img[data-pixelate]");for(var a=0;a<t.length;a++){t[a].addEventListener("load",function(){this.pixelate()})}})}})(window,typeof jQuery==="undefined"?null:jQuery);</script>';

	echo '<a href='.$url.'><img src="'.$url.'" style="margin:3;border:0;height:120px;" title="'.$sha1.'" data-pixelate data-value="0.4"></a>';
	echo '<br/>URL: ' .$url. '<br/>SHA1: '.$sha1.'<br/>HT_INDEX: ';
}
else {
	echo '{ "SHA1": '.$sha1.', "cached_url": '.$url.', "ht_index": [';
}
$query = "SELECT htid FROM fullIds where uid=". $unique_idx;
if ($stmt = $mysqli->prepare($query)) {

    /* execute statement */
    $stmt->execute();

    /* bind result variables */
    $stmt->bind_result($HT_idx);
	$fir = 1;
    /* fetch values */
    while ($stmt->fetch()) {
		if ($fir) {
			$fir = 0;
		} 
		else {
			echo ', ';
		}
		echo($HT_idx);
		
    }

    /* close statement */
    $stmt->close();
}
if (!$vis) {
	echo ']}';
}
/* close connection */
$mysqli->close();

?>
