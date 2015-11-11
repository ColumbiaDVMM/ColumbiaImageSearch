import csv
import MySQLdb
import json
import pickle

global_var = json.load(open('global_var_all.json'))
isthost=global_var['ist_db_host']
istuser=global_var['ist_db_user']
istpwd=global_var['ist_db_pwd']
istdb=global_var['ist_db_dbname']
localhost=global_var['local_db_host']
localuser=global_var['local_db_user']
localpwd=global_var['local_db_pwd']
localdb=global_var['local_db_dbname']

# Get data from CSV
csv_filename='nov2015_qpr_truth__ids_images.csv'
csv_file=open(csv_filename,'rb')
csv_file.readline() # discard header
csvreader = csv.reader(csv_file)
ads_id=[]
images_urls=[]
for row in csvreader:
    print len(row),":",', '.join(row)
    ads_id.append(row[5])
    images_urls.append(row[6].split(";"))
print len(ads_id),ads_id
print len(images_urls),images_urls

# Get all data from IST database
db=MySQLdb.connect(host=isthost,user=istuser,passwd=istpwd,db=istdb)
c=db.cursor()
sql_withurl='select i.id,i.url,i.location,ads.url,ads.id from images i left join ads on i.ads_id=ads.id where i.url in (%s) and ads.id=(%s) order by i.id;'
sql_nourl='select i.id,i.url,i.location,ads.url,ads.id from images i left join ads on i.ads_id=ads.id where ads.id=(%s) order by i.id;'
all_imagesid=[]
all_imagesurl=[]
all_imageslocation=[]
all_adsurl=[]
all_adsid=[]
for pos,ad_id in enumerate(ads_id):
    if len(images_urls[pos])==1 and images_urls[pos][0]=='': # empty
        sqlq = sql_nourl % (ad_id,)
    else:
        sqlq = sql_withurl % (ad_id,', '.join(map(lambda x: '%s', images_urls[pos])))
    print sqlq
    c.execute(sqlq)
    tmpresult = c.fetchall()
    for posres,oneres in enumerate(tmpresult):
        all_imagesid.append(oneres[0])
        all_imagesurl.append(oneres[1])
        all_imageslocation.append(oneres[2])
        all_adsurl.append(oneres[3])
        all_adsid.append(oneres[4])
db.close()

alldata_istgt={}
alldata_istgt['all_imagesid']=all_imagesid
alldata_istgt['all_imagesurl']=all_imagesurl
alldata_istgt['all_imageslocation']=all_imageslocation
alldata_istgt['all_adsurl']=all_adsurl
alldata_istgt['all_adsid']=all_adsid
pickle.dump(alldata_istgt,open("alldata_istgt.pkl","wb"))

# Get similar images from HBase

# Compute connected components