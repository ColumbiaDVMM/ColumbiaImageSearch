import json
from pyspark import SparkContext, SparkConf


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def s3url_missing_sha1(data):
    key = data[0]
    try:
        json_x = [json.loads(x) for x in data[1].split("\n")]
        # try to get sha1 and s3url
        sha1_list = get_list_value(json_x, ("info", "sha1"))
        s3url_list = get_list_value(json_x, ("info", "obj_stored_url"))
        s3url = s3url_list[0].strip()    
        # discard urls that are not stored in s3
        if not s3url.startswith('https://s3'):
            raise ValueError('obj_stored_url is not stored in S3.')
        # if sha1 not computed, build tuple (s3url, cdrid)
        if not sha1_list:
            print("[s3url_missing_sha1] Missing sha1 for cdrid {} with url: {}.".format(key, s3url))
            return [(str(s3url), str(key))]
        else: # sha1 seems already computed
            # test it is valid
            sha1 = sha1_list[0].strip()
            if sha1 is None or sha1 == 'None' or sha1 == u'None' or not sha1:
                return [(str(s3url), str(key))]
            return []
    except Exception as inst:
        print "[Error] could not build output for row {}. {}".format(key, inst)
        return []


def read_sha1(data):
    s3url = data[0]
    try:
        json_x = [json.loads(x) for x in data[1].split("\n")]
        # try to get sha1
        sha1_list = get_list_value(json_x, ("info", "sha1"))
        if sha1_list:
            sha1 = sha1_list[0].strip()
            if not sha1 or sha1 == 'None' or sha1 == u'None':
                raise ValueError('sha1 is incorrect.')
            return [(str(s3url), str(sha1))]
        else: # sha1 is not present
            return []  
    except Exception as inst:
        print "[Error] could not build output for row {}. {}".format(s3url, inst)
        return []


def cdrid_sha1(data):
    s3url = str(data[0])
    cdr_id = str(data[1][0])
    sha1 = data[1][1]
    if sha1:
        return [(cdr_id, [cdr_id, "info", "sha1", str(sha1)])]
    else:
        return []


def get_missing_sha1_from_s3url(hbase_man_in, hbase_man_s3, hbase_man_out, nb_partitions):
    s3url_missing_sha1_rdd = hbase_man_in.read_hbase_table().partitionBy(nb_partitions).flatMap(lambda x: s3url_missing_sha1(x))
    # read s3url_sha1 table
    s3url_sha1_rdd = hbase_man_s3.read_hbase_table().partitionBy(nb_partitions).flatMap(lambda x: read_sha1(x))
    # join and revert key
    out_rdd = s3url_missing_sha1_rdd.join(s3url_sha1_rdd).flatMap(lambda x: cdrid_sha1(x))
    # fast hash join with lookup?
    # Nope: Exception: It appears that you are attempting to broadcast an RDD or reference an RDD from an action or transformation. RDD transformations and actions can only be invoked by the driver, not inside of other transformations; for example, rdd1.map(lambda x: rdd2.values.count() * x) is invalid because the values transformation and count action cannot be performed inside of the rdd1.map transformation. For more information, see SPARK-5063.
    # out_rdd = s3url_missing_sha1_rdd.map(lambda x: (x[0], (x[1], s3url_sha1_rdd.lookup(x[0])))).flatMap(lambda x: cdrid_sha1(x))
    # save sha1 obtained
    hbase_man_out.rdd2hbase(out_rdd)


if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_cdrid_infos_name = job_conf["tab_cdrid_infos_name"]
    tab_s3url_name = job_conf["tab_s3url_name"]
    hbase_host = job_conf["hbase_host"]
    nb_partitions = job_conf["nb_partitions"]
    sc = SparkContext(appName='get_sha1_from_'+tab_s3url_name+'_to_'+tab_cdrid_infos_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_cdrid_infos_name, columns_list=["info:sha1", "info:obj_stored_url"])
    hbase_man_s3 = HbaseManager(sc, conf, hbase_host, tab_s3url_name)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_cdrid_infos_name)
    get_missing_sha1_from_s3url(hbase_man_in, hbase_man_s3, hbase_man_out, nb_partitions)
