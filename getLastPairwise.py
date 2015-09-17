import struct,time
import MySQLdb
import json
global_var = json.load(open('global_var_all.json'))
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']
db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
c=db.cursor()
sql='select * from pairwise_infos order by update_id desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
print "query database: %s seconds ---" % (time.time() - start_time)
re = c.fetchall()
print "Latest batch:",re
db.close()
