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
	echo '<a href='.$url.'><img src="'.$url.'" style="margin:3;border:0;height:120px;" title="'.$sha1.'"></a>';
	echo '<br>SHA1: '.$sha1.'<br>HT_INDEX: ';
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
