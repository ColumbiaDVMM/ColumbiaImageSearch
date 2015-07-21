import struct,time
import MySQLdb
import json
global_var = json.load(open('global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
c=db.cursor()
sql='select id,location from images where location is not null order by id desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
print "query database: %s seconds ---" % (time.time() - start_time)
re = c.fetchall()
print re
db.close()









