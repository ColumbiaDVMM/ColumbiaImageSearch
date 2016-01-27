<?php
error_reporting(E_ALL | E_STRICT);

$conf_file = "/home/ubuntu/memex/conf/global_var_all.json";
$mainpath = "/home/ubuntu/memex/DeepSentiBank_memex/www/";

$fgval = fopen($conf_file, "rb");
$gread=fread($fgval,filesize($conf_file));
$global_var = json_decode($gread);
$images_by_batch = $global_var->{'images_update_batch'};

// Connect to local DB
$local_db_host = $global_var->{'local_db_host'};
$local_db_user = $global_var->{'local_db_user'};
$local_db_pwd = $global_var->{'local_db_pwd'};
$local_db_dbname = $global_var->{'local_db_dbname'};
$local_db = new mysqli($local_db_host, $local_db_user, $local_db_pwd, $local_db_dbname);
if($local_db->connect_errno > 0){
    die('Unable to connect to database [' . $local_db->connect_error . ']');
}
// Get biggest HT_ID in local DB
$result = $local_db->query('select id,htid from uniqueIds order by htid desc limit 1');
if (!$result) {
    die("Invalid request : " . $local_db->error());
}
while($row = $result->fetch_assoc()){
    $biggest_local_htid = $row['htid'];
    //printf ("%s (%s)\n",$row['id'],$row['htid']);
}
$local_db->close();

// Connect to IST DB
$ist_db_host = $global_var->{'ist_db_host'};
$ist_db_user = $global_var->{'ist_db_user'};
$ist_db_pwd = $global_var->{'ist_db_pwd'};
$ist_db_dbname = $global_var->{'ist_db_dbname'};
$ist_db = new mysqli($ist_db_host, $ist_db_user, $ist_db_pwd, $ist_db_dbname);
if($ist_db->connect_errno > 0){
    die('Unable to connect to database [' . $ist_db->connect_error . ']');
}
// Get how many images have been added to IST DB since local biggest HT_ID
$sql='select id,location from images where id > '.$biggest_local_htid.' and location is not null order by id LIMIT '.$images_by_batch;
$result = $ist_db->query($sql);
if (!$result) {
    die("Invalid request : " . $ist_db->error());
}
// Count
$nb_images = $result->num_rows;
//printf("We have %s new images\n",$nb_images);
$ist_db->close();

// Check if bigger than number of images per batch
if ($nb_images<$images_by_batch) {
     $status=array('status' => 'OK', 'images_not_indexed' => $nb_images);
}
elseif ($nb_images<2*$images_by_batch) {
     $status=array('status' => 'LAG', 'images_not_indexed' => $nb_images);
}
else {
     $status=array('status' => 'OUTOFDATE', 'images_not_indexed' => $nb_images);
}
// Report status 
echo json_encode($status);
?>
