import struct,time
import MySQLdb
import json
global_var = json.load(open('../conf/global_var_all.json'))
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
re = c.fetchall()
print "Biggest HTID in LOCAL DB:",re
BiggestHTID_Local = re[0][1]
sql='select cdr_id,htid from uniqueIdsCDR order by htid desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
re = c.fetchall()
print "Biggest HTID in LOCAL DB CDR:",re

sql='select id,htid from uniqueIds order by id desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
re = c.fetchall()
print "Biggest unique ID in LOCAL DB:",re
sql='select cdr_id,htid,feat_id from uniqueIdsCDR order by feat_id desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
re = c.fetchall()
print "Biggest unique CDR ID in LOCAL DB:",re

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
print re
sql='SELECT * FROM fullIdsCDR order by id DESC LIMIT 1;'
c.execute(sql)
re = c.fetchall()
print "Biggest ID fullIdsCDR':",re
print re
db.close()



isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
c=db.cursor()
sql='select * from images where location is not null order by id desc limit 1'
query_id = []
start_time = time.time()
#c.execute(sql, query_id)
#print "query database: %s seconds ---" % (time.time() - start_time)
#re = c.fetchall()
#print "Biggest HTID in IST DB:",re
#BiggestHTID_IST = re[0][0]
sql='select id,location from images where id > ' + str(BiggestHTID_Local) +' and location is not null order by id LIMIT 200000'
query_id = []
start_time = time.time()
#c.execute(sql, query_id)
#re = c.fetchall()
#new_images=len(re)
#db.close()
#print BiggestHTID_IST - BiggestHTID_Local
#print new_images
