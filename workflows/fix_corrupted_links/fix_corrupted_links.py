import json
from hbase_manager import HbaseManager
from pyspark import SparkContext, SparkConf


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def to_sha1_key(data):
    # Get values from fields "meta:sha1", "meta:columbia_near_dups_sha1", "meta:columbia_near_dups_sha1_dist"
    sha1_fn = ("meta","sha1")
    nd_sha1_fn = ("meta","columbia_near_dups_sha1")
    ndd_sha1_fn = ("meta","columbia_near_dups_sha1_dist")
    htid = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    tup_list=[]
    im_sha1_list = get_list_value(json_x, sha1_fn)
    if not im_sha1_list:
        return tup_list
    im_sha1 = im_sha1_list[0].strip()
    sim_im_sha1_list_list = get_list_value(json_x, nd_sha1_fn)
    dist_list_list = get_list_value(json_x, ndd_sha1_fn)
    if not sim_im_sha1_list_list or not dist_list_list:
        return tup_list
    sim_im_sha1_list = sim_im_sha1_list_list[0].split(',')
    dist_list = dist_list_list[0].split(',')
    tup_list.append( (im_sha1, (htid, sim_im_sha1_list, dist_list) ))
    return tup_list


def to_sim_sha1_key(data):
    #print("[to_sim_sha1_key] {}".format(data))
    im_sha1 = data[0]
    htid = data[1][0][0]
    dist_list = data[1][0][2]
    tup_list = []
    for i,sim_sha1 in enumerate(data[1][0][1]):
        tup_list.append( (str(sim_sha1), (str(im_sha1), htid, float(dist_list[i]))) )
    return tup_list


def fix_link(data):
    #print("[fix_link] {}".format(data))
    sim_sha1 = data[0]
    im_sha1, htid, dist = data[1][0]
    filter_me = data[1][1]
    tup_list = []
    if not filter_me:
        tup_list.append( (htid, ([sim_sha1], [dist])) )
    else:
        print "Discarding potentially corrupted link between image {} and image {}.".format(im_sha1, sim_sha1)
    return tup_list


def reduce_fixed(a, b):
    #print("[reduce_fixed] {}, {}".format(a, b))
    c = (a[0]+b[0], a[1]+b[1])
    return c


def back_to_list(data):
    #print("[back_to_list] {}".format(data))
    tup_list = []
    htid = data[0]
    sim_im_sha1_list_fix = data[1][0]
    dist_list_fix = data[1][1]
    tup_list.append( (htid, [htid, "meta", "columbia_near_dups_sha1", str(','.join(sim_im_sha1_list_fix))]) )
    tup_list.append( (htid, [htid, "meta", "columbia_near_dups_sha1_dist", str(','.join([str(x) for x in dist_list_fix]))]) )
    return tup_list


def fix_corrupted_links(sc, hbase_man_in, hbase_man_out, cs_rdd, ct_rdd, nb_partitions):
    in_rdd = hbase_man_in.read_hbase_table().partitionBy(nb_partitions)
    #to_be_fixed_rdd = in_rdd.filter(lambda x: long(x[0])<=max_htid).flatMap(lambda x: to_sha1_key(x)).join(cs_rdd)
    to_be_fixed_rdd = in_rdd.flatMap(lambda x: to_sha1_key(x)).join(cs_rdd)
    fixed_rdd = to_be_fixed_rdd.flatMap(lambda x: to_sim_sha1_key(x)).leftOuterJoin(ct_rdd)
    fixed_corrupted_links_rdd = fixed_rdd.flatMap(lambda x: fix_link(x)).reduceByKey(reduce_fixed)
    hbase_man_out.rdd2hbase(fixed_corrupted_links_rdd.flatMap(lambda x: back_to_list(x)))
    print "[fix_corrupted_links] Done."

if __name__ == '__main__':
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_images_name = job_conf["tab_name_in"]
    hbase_host = job_conf["hbase_host"]
    #min_htid = 143409237
    #max_htid = 153934312
    nb_partitions = job_conf["nb_partitions"]
    #row_start = '52010000'
    #row_stop = '52020000'
    sc = SparkContext(appName='fix_corrupted_links_'+tab_images_name)
    sc.setLogLevel("ERROR")
    c_rdd = sc.textFile('hdfs://memex/user/skaraman/fix_corrupted_links/corrupted_slice_33514258_33515000.csv').map(lambda x: (x.strip()[1:-1],x.strip()[1:-1])).partitionBy(nb_partitions)
    #ct_rdd = sc.textFile('hdfs://memex/user/skaraman/fix_corrupted_links/corrupted_targets.csv').map(lambda x: (x.strip()[1:-1],x.strip()[1:-1])).partitionBy(nb_partitions)
    #cs_rdd = sc.textFile('hdfs://memex/user/skaraman/fix_corrupted_links/corrupted_sources.csv').map(lambda x: (x.strip()[1:-1],x.strip()[1:-1])).partitionBy(nb_partitions)
    conf = SparkConf()
    in_columns_list = ["meta:sha1", "meta:columbia_near_dups_sha1", "meta:columbia_near_dups_sha1_dist"]
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_images_name, columns_list=in_columns_list, row_start=min_htid, row_stop=max_htid)
    #hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_images_name, columns_list=in_columns_list)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_images_name)
    fix_corrupted_links(sc, hbase_man_in, hbase_man_out, c_rdd, c_rdd, nb_partitions)
