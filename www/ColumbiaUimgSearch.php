<?php
error_reporting(E_ALL | E_STRICT);

$conf_file = "/home/ubuntu/memex/conf/global_var_all.json";
$mainpath = "/home/ubuntu/memex/DeepSentiBank_memex/www/";

$verbose=0;

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
$noblur = $argv[10];
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
$noblur = $_GET['noblur'];
$nocache = $_GET['nocache'];
}

if (empty($image_url)) {
  echo "<h1>Please provide an image url!</h1>How to use this API:<br/><ul><li>Minimal requirement: an image URL: https://isi.memexproxy.com/ColumbiaUimgSearch.php?url=https://hostingservice.com/image.jpg</li></ul><ul>Other parameters:<li>visualize: 1, 0 [JSON or visualization: default: 0]</li><li>nodup: 1, 0 [remove or display exact duplicate, default: 0]</li><li>num: maximum number of returned images [default: 30, 1000 if neardup activated]</li><li>neardup: 1, 0 [activate near duplicate search, default: 0]</li><li>neardup_type: strict, loose, balanced [default: balanced]</li></ul>Issues, questions or suggestions? Contact us:<ul><li>Svebor Karaman: svebor.karaman@columbia.edu</li><li>Tao Chen: taochen@ee.columbia.edu</li></ul>";
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
  $neardup_type = 'balanced';
} else {
  $neardup = 1;
}

if (empty($nocache)) {
  $nocache = 1;
}
if (empty($noblur)) {
  $noblur = 0;
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
$fgval = fopen($conf_file, "rb");
$gread = fread($fgval,filesize($conf_file));
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
        $neardup_th = $global_var->{'neardup_th_balanced'};
        break;
  }
  }
  $neardupstr = '_neardup'.$neardup_th;
  $query_num=1000;
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
//echo $image_url,$fullnamet;
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
if ($verbose) {
   echo "cd " . $mainpath . " && export LD_LIBRARY_PATH=/usr/local/cuda/lib64 && python ../getSimilarNew.py " . $fullname . " " . $query_num . " " . $ratio . " " . $dup . " " . $neardup . " " . $neardup_th;
}
shell_exec("cd " . $mainpath . " && export LD_LIBRARY_PATH=/usr/local/cuda/lib64 && python ../getSimilarNew.py " . $fullname . " " . $query_num . " " . $ratio . " " . $dup . " " . $neardup . " " . $neardup_th);

$fout = fopen ($outname, "rb");
 if ($fout) {
	$json = fread($fout,filesize($outname));
	if ($vis==0){
		echo $json;
	}
	else {
		//echo '<div id="debug" value="'.time_elapsed(time()-$start_time).'" outfile="'.$outname.'" params="image_url:'.$image_url.';query_num:'.$query_num.';vis:'.$vis.';fast:'.$fast.';nodup:'.$nodup.';neardup:'.$neardup.';neardup_th:'.$neardup_th.';nocache:'.$nocache.';ratio:'.$ratio.';"></div>';
		$obj = json_decode($json);
		echo '<link rel="stylesheet" type="text/css" href="style.css" />';
   		if ($noblur) {
      			$img_style="img_vis";
      			$dup_style="dup_vis";
    		}
    		else {
      			$img_style="img_blur";
      			$dup_style="dup_blur";
   		 }

    		echo '<font size="6"><b>Query Image</b></font><br><a href="'.$image_url.'"><img src="'.$image_url.'" class="'.$img_style.'" title="Query Image"></a><br><br><font size="6"><b>Query Results:</b><br>';
		$imglist = $obj->{'images'}[0]->{'similar_images'}->{'cached_image_urls'};
		$orilist = $obj->{'images'}[0]->{'similar_images'}->{'page_urls'};
		$uidlist = $obj->{'images'}[0]->{'similar_images'}->{'ht_images_id'};
		$sha1list = $obj->{'images'}[0]->{'similar_images'}->{'sha1'};
		$distlist = $obj->{'images'}[0]->{'similar_images'}->{'distance'};

		for ($i=0; $i<sizeof($imglist); $i++) {
      			$dupurl = 'getDuplicate.php?htid='.$uidlist[$i].'&visualize=1&style='.$dup_style.'';
      			echo '<a href="'.$dupurl.'"><img src="'.$imglist[$i].'" class="'.$img_style.'" origin="'.$orilist[$i].'" title="'.$distlist[$i].'"></a>';
   		 }
	}
	
}

?>
