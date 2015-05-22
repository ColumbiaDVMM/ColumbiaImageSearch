<?php

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
$mainpath = '/home/ubuntu/memex/';
$savepath = $mainpath . 'img/';
$image_url = $_GET['url'];
$name = basename($image_url);
$fullname = $savepath . $name;
$pos = strrpos($fullname, ".");
if ($pos === false) { // note: three equal signs
    // not found...
	return;
}
$fullnamet = substr_replace($fullname, "_" . Rand(), $pos, 0);
downloadFile($image_url,$fullnamet);
$output = shell_exec("md5sum " . $fullnamet );
list($md5, $tmp) = split(" ", $output);
$fullname = substr_replace($fullname, "_" . $md5, $pos, 0);
if (file_exists($fullname)) {
    //echo "The file $filename exists";
	unlink($fullnamet);
} else {
    //echo "The file $filename does not exist";
	rename($fullnamet, $fullname);

}
shell_exec("cd " . $mainpath . " && export LD_LIBRARY_PATH=/usr/local/cuda/lib64 && python sentiBank.py " . $fullname );
$outname = substr_replace($fullname, ".json", -4, 4);


$fout = fopen ($outname, "rb");
 if ($fout) {
echo fread($fout,filesize($outname));
}

?>
