<?php
error_reporting(E_ALL | E_STRICT);

function downloadFile ($url, $path) {

  $newfname = $path;

  $file = fopen ($url, "rb");
  if ($file) {
    $newf = fopen ($newfname, "wb");

    if ($newf)

    while(!feof($file)) {
      fwrite($newf, fread($file, 1024 * 8 ), 1024 * 8 );
    }
  }

  if ($file) {
    fclose($file);
  }

  if ($newf) {
    fclose($newf);
  }
 }

function time_elapsed($secs){
    $bit = array(
        'y' => $secs / 31556926 % 12,
        'w' => $secs / 604800 % 52,
        'd' => $secs / 86400 % 7,
        'h' => $secs / 3600 % 24,
        'm' => $secs / 60 % 60,
        's' => $secs % 60
        );
        
    foreach($bit as $k => $v)
        if($v > 0) $ret[] = $v . $k;
        
    if(count($ret) > 0) {
    	$out = join(' ', $ret);
    }
    else {
    	$out = 'Less than 1 second.';
    }

    return $out;
    }



$mainpath = '/home/ubuntu/memex/';
$savepath = $mainpath . 'img/';
if (PHP_SAPI === 'cli') {
$image_url = $argv[1];
$query_num = $argv[2];
$vis = $argv[3];
$fast = $argv[4];
$nodup = $argv[5];
$neardup = $argv[6];
$neardup_type = $argv[7];
$neardup_th = $argv[8];
$nocache = $argv[9];
}
else {
$image_url = $_GET["url"];
$query_num = $_GET['num'];
$vis = $_GET['visualize'];
$fast = $_GET['fast'];
$nodup = $_GET['nodup'];
$neardup = $_GET['neardup'];
$neardup_th = $_GET['neardup_th'];
$neardup_type = $_GET['neardup_type'];
$nocache = $_GET['nocache'];
}

if (empty($image_url)) {
  echo "<h1>Please provide an image url!</h1>";
}
if (empty($query_num)) {
  $query_num=30;
}
if (empty($vis)) {
  $vis = 0;
}
if (empty($fast)) {
  $fast = 0;
}
if (empty($nodup)) {
  $nodup = 0;
}
if (empty($neardup)) {
  $neardup = 0;
}
if (empty($neardup_type)) {
  $neardup_type = 'standard';
}

if (empty($nocache)) {
  $nocache = 0;
}

$dup = 1;
$dupstr = '_dup';
if ($nodup>0){
	$dup =0;
	$dupstr = '';
}

if ($query_num<1){
	$query_num = 30;
}


if ($vis<1){
	$vis = 0;
}
else{
	$vis = 1;
}
if ($fast<1){
	$fast = 0;
}
else{
	$fast = 1;
}
$fgval = fopen ("global_var_new.json", "rb");
$gread=fread($fgval,filesize("global_var_new.json"));
$global_var = json_decode($gread);
if ($fast){
  $ratio = $global_var->{'fast_ratio'};
}
else {
  $ratio = $global_var->{'ratio'};
}

$neardupstr = '';
if ($neardup>0){
  if (empty($neardup_th)) { // If specified manually, do not override
  switch ($neardup_type) {
    case "strict":
        $neardup_th = $global_var->{'neardup_th_strict'};
        break;
    case "loose":
        $neardup_th = $global_var->{'neardup_th_loose'};
        break;
    default:
        $neardup_th = $global_var->{'neardup_th_standard'};
        break;
  }
  }
  $neardupstr = '_neardup'.$neardup_th;
}
//echo $query_num . ' ' . $vis; 

$name = basename($image_url);
$fullname = $savepath . $name;
$pos = strrpos($fullname, ".");
if ($pos === false) { // note: three equal signs
    // not found...
        $fullname=$fullname.'.jpg';
        $pos = strrpos($fullname, ".");

}
$fullnamet = substr_replace($fullname, "_" . Rand(), $pos, 0);
downloadFile($image_url,$fullnamet);

//$output = shell_exec("md5sum " . $fullnamet );
$output = shell_exec("sha1sum " . $fullnamet );

//list($md5, $tmp) = split(" ", $output);
list($sha1, $tmp) = split(" ", $output);
//$fullname = substr_replace($fullname, "_" . $md5, $pos, 0);
$fullname = substr_replace($fullname, "_" . $sha1, $pos, 0);

if (file_exists($fullname)) {
    //echo "The file $filename exists";
	unlink($fullnamet);
} else {
    //echo "The file $filename does not exist";
	rename($fullnamet, $fullname);

}


$start_time = time();
//$outname = substr_replace($fullname, "-sim_".$query_num."_".$ratio.$dupstr."_".date('Y-m-d_H').".json", -4, 4); // Date is for one hour caching.
$outname = substr_replace($fullname, "-sim_".$query_num."_".$ratio.$dupstr.$neardupstr.".json", -4, 4); // Date is for one hour caching.
if ($nocache==1) {
  unlink($outname);
}
//echo "cd " . $mainpath . " && export LD_LIBRARY_PATH=/usr/local/cuda/lib64 && python getSimilarNew.py " . $fullname . " " . $query_num . " " . $ratio . " " . $dup . " " . $neardup . " " . $neardup_th;
shell_exec("cd " . $mainpath . " && export LD_LIBRARY_PATH=/usr/local/cuda/lib64 && python getSimilarNew.py " . $fullname . " " . $query_num . " " . $ratio . " " . $dup . " " . $neardup . " " . $neardup_th);

$fout = fopen ($outname, "rb");
 if ($fout) {
	$json = fread($fout,filesize($outname));
	if ($vis==0){
		echo $json;
	}
	else {
		//echo '<div id="debug" value="'.time_elapsed(time()-$start_time).'" outfile="'.$outname.'" params="image_url:'.$image_url.';query_num:'.$query_num.';vis:'.$vis.';fast:'.$fast.';nodup:'.$nodup.';neardup:'.$neardup.';neardup_th:'.$neardup_th.';nocache:'.$nocache.';ratio:'.$ratio.';"></div>';
		$obj = json_decode($json);
    //echo '<div id="debug" value="'.time_elapsed(time()-$start_time).'" params="image_url:'.$image_url.';query_num:'.$query_num.';vis:'.$vis.';fast:'.$fast.';nodup:'.$nodup.';neardup:'.$neardup.';neardup_th:'.$neardup_th.';nocache:'.$nocache.';"></div>';
		//echo '<script src="pixelate.min.js"></script><script src="jquery-2.0.3.min.js"></script>';
    echo '<script src="http://code.jquery.com/jquery-2.0.3.min.js"></script>';
    echo '<script type="text/javascript">(function(e,t){var a=function(){var e={value:.05,reveal:true,revealonclick:false};var t=arguments[1]||{};var a=this,n=a.parentNode;if(typeof t!=="object"){t={value:parseInt(arguments[1])}}t=function(){var n={};for(var i in e){if(a.hasAttribute("data-"+i)){n[i]=a.getAttribute("data-"+i);continue}if(i in t){n[i]=t[i];continue}n[i]=e[i]}return n}();var i=a.style.display,r=a.width,l=a.height,o=false;var d=document.createElement("canvas");d.width=r;d.height=l;var f=d.getContext("2d");f.mozImageSmoothingEnabled=false;f.webkitImageSmoothingEnabled=false;f.imageSmoothingEnabled=false;var u=r*t.value,s=l*t.value;f.drawImage(a,0,0,u,s);f.drawImage(d,0,0,u,s,0,0,d.width,d.height);a.style.display="none";n.insertBefore(d,a);if(t.revealonclick!==false&&t.revealonclick!=="false"){d.addEventListener("click",function(e){o=!o;if(o){f.drawImage(a,0,0,r,l)}else{f.drawImage(a,0,0,u,s);f.drawImage(d,0,0,u,s,0,0,d.width,d.height)}})}if(t.reveal!==false&&t.reveal!=="false"){d.addEventListener("mouseenter",function(e){if(o)return;f.drawImage(a,0,0,r,l)});d.addEventListener("mouseleave",function(e){if(o)return;f.drawImage(a,0,0,u,s);f.drawImage(d,0,0,u,s,0,0,d.width,d.height)})}};e.HTMLImageElement.prototype.pixelate=a;if(typeof t==="function"){t.fn.extend({pixelate:function(){return this.each(function(){a.apply(this,arguments)})}});t(e).on("load",function(){t("img[data-pixelate]").pixelate()})}else{document.addEventListener("DOMContentLoaded",function(e){var t=document.querySelectorAll("img[data-pixelate]");for(var a=0;a<t.length;a++){t[a].addEventListener("load",function(){this.pixelate()})}})}})(window,typeof jQuery==="undefined"?null:jQuery);</script>';

    echo '<font size="6"><b>Query Image</b></font><br><a href="'.$image_url.'"><img src="'.$image_url.'" style="margin:3;border:0;height:120px;" title="Query Image" data-pixelate></a><br><br><font size="6"><b>Query Results:</b><br>';
		$imglist = $obj->{'images'}[0]->{'similar_images'}->{'cached_image_urls'};
		$orilist = $obj->{'images'}[0]->{'similar_images'}->{'page_urls'};
		$uidlist = $obj->{'images'}[0]->{'similar_images'}->{'ht_images_id'};
		$sha1list = $obj->{'images'}[0]->{'similar_images'}->{'sha1'};
		$distlist = $obj->{'images'}[0]->{'similar_images'}->{'distance'};

		for ($i=0; $i<sizeof($imglist); $i++) {
			$dupurl = 'getDuplicate.php?htid='.$uidlist[$i].'&visualize=1';
			echo '<a href="'.$dupurl.'"><img src="'.$imglist[$i].'" style="margin:3;border:0;height:120px;" origin="'.$orilist[$i].'" title="'.$distlist[$i].'" data-pixelate></a>';
		}
	}
	
}

?>
