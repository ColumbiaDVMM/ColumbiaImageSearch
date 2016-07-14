import json
from pyspark import SparkContext, SparkConf

def get_SHA1_from_URL(URL):
    import image_dl
    sha1hash = image_dl.get_SHA1_from_URL_StringIO(URL,1) # 1 is verbose level
    return sha1hash

def get_row_sha1(URL_S3,verbose=False):
    row_sha1 = None
    #print type(URL_S3),URL_S3
    if type(URL_S3) == unicode and URL_S3 != u'None' and URL_S3.startswith('https://s3'):
        row_sha1 = get_SHA1_from_URL(URL_S3)
    if row_sha1 and verbose:
        print "Got new SHA1 {} from_url {}.".format(row_sha1,URL_S3)
    return row_sha1

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]

def check_get_sha1(data):
    json_x = [json.loads(x) for x in data[1].split("\n")]
    # First check if sha1 is not already there...
    try:
        row_sha1 = get_list_value(json_x,("info","sha1"))[0].strip()
        # Check for None here just to be safe
        if row_sha1 is None or row_sha1 == u'None':
            raise ValueError('sha1 is None.')
    except Exception as inst2: 
        # sha1 column does not exist
        URL_S3 = None
        try:
            URL_S3 = get_list_value(json_x,("info","obj_stored_url"))[0].strip()
            #print key,URL_S3,type(URL_S3)
        except Exception as inst2:
            print "[Error] for row {}. {}".format(key,inst2)
            print "No way to get SHA1 for row {}: no obj_stored_url.".format(key)
        row_sha1 = get_row_sha1(URL_S3)
        if row_sha1:
            key = data[0]
            return [(key, [key, "info", "sha1", row_sha1.upper()])]
    return []

def get_sha1(data):
    key = data[0]
    URL_S3 = None
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        URL_S3 = get_list_value(json_x,("info","obj_stored_url"))[0].strip()
        #print key,URL_S3,type(URL_S3)
    except Exception as inst2:
        print "[Error] for row {}. {}".format(key,inst2)
        print "No way to get SHA1 for row {}: no obj_stored_url.".format(key)
    row_sha1 = get_row_sha1(URL_S3)
    if row_sha1:
        return [(key, [key, "info", "sha1", row_sha1.upper()])]
    return []

def fill_sha1(sc, hbase_man):
    in_rdd = hbase_man.read_hbase_table()
    out_rdd = in_rdd.flatMap(lambda x: check_get_sha1(x))
    hbase_man.rdd2hbase(out_rdd)

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name = job_conf["tab_name"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='sha1_'+tab_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man = HbaseManager(sc, conf, hbase_host, tab_name)
    fill_sha1(sc, hbase_man)
