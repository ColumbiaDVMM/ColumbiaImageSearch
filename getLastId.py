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
sql='select * from images where location is not null order by id desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
print "query database: %s seconds ---" % (time.time() - start_time)
re = c.fetchall()
print "Biggest HTID in IST DB:",re
db.close()
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']
db=MySQLdb.connect(host=localhost,user=localuser,passwd=localpwd,db=localdb)
c=db.cursor()
sql='select id,htid from uniqueIds order by htid desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
print "query database: %s seconds ---" % (time.time() - start_time)
re = c.fetchall()
print "Biggest HTID in LOCAL DB:",re
sql='select id,htid from uniqueIds order by id desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
print "query database: %s seconds ---" % (time.time() - start_time)
re = c.fetchall()
print "Biggest unique ID in LOCAL DB:",re
sql='SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "imageinfo" AND  TABLE_NAME   = "uniqueIds";'
c.execute(sql)
re = c.fetchall()
print "AutoIncrement uniqueIds:",re
sql='SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "imageinfo" AND  TABLE_NAME   = "fullIds";'
c.execute(sql)
re = c.fetchall()
print "AutoIncrement fullIds':",re
sql='SELECT * FROM fullIds order by id DESC LIMIT 1;'
c.execute(sql)
re = c.fetchall()
print "Biggest ID fullIds':",re
db.close()
