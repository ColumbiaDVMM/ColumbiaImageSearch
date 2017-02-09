import happybase
import json
import sha1_tools

hbase_conn_timeout = None
pool = happybase.ConnectionPool(size=12,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool
global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var

sha1_mysql = sha1_tools.get_SHA1_from_MySQL(1)
print sha1_mysql
sha1_mysql = sha1_tools.get_SHA1_from_MySQL(151)
print sha1_mysql
sha1_mysql = sha1_tools.get_SHA1_from_MySQL(10)
print sha1_mysql
sha1_aaron = sha1_tools.compute_SHA1_for_image_id_from_tab_aaron(1,"aaron_memex_ht-images")
print sha1_aaron
sha1_aaron = sha1_tools.compute_SHA1_for_image_id_from_tab_aaron(10,"aaron_memex_ht-images")
print sha1_aaron
sha1s_mysql = sha1_tools.get_batch_SHA1_from_mysql(["1","10","151"])
print sha1s_mysql
sha1s_mysql = sha1_tools.get_batch_SHA1_from_mysql([1,10,151])
print sha1s_mysql
