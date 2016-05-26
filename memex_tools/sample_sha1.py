import happybase
import json
import sha1_tools

pool = happybase.ConnectionPool(size=12,host='10.1.94.57',timeout=hbase_conn_timeout)
sha1_tools.pool = pool
global_var = json.load(open('../../conf/global_var_all.json'))
sha1_tools.global_var = global_var

sha1_mysql = sha1_tools.get_SHA1_from_MySQL(1)
print sha1_mysql
sha1_aaron = compute_SHA1_for_image_id_from_tab_aaron.get_SHA1_from_MySQL(1)
print sha1_aaron