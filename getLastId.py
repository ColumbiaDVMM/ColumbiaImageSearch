import struct,time
import MySQLdb
#db=MySQLdb.connect(host='54.191.207.159',user='dig',passwd="VKZhUGMDN6wGtGQd",db="memex_ht")
db=MySQLdb.connect(host='memex-db.istresearch.com',user='dig',passwd="VKZhUGMDN6wGtGQd",db="memex_ht")
c=db.cursor()
sql='select id,location from images where location is not null order by id desc limit 1'
query_id = []
start_time = time.time()
c.execute(sql, query_id)
print "query database: %s seconds ---" % (time.time() - start_time)
re = c.fetchall()
print re
db.close()









