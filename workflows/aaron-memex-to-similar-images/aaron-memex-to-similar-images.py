import json
from hbase_manager import HbaseManager
from pyspark import SparkContext, SparkConf

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]

def create_sim_images_tuple(data):
    # Get values from fields "meta:sha1", "meta:columbia_near_dups_sha1", "meta:columbia_near_dups_sha1_dist"
    sha1_fn = ("meta","sha1")
    nd_sha1_fn = ("meta","columbia_near_dups_sha1")
    ndd_sha1_fn = ("meta","columbia_near_dups_sha1_dist")
    doc_id = data[0]
    #print data[1]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    tup_list=[]
    #im_sha1_list = [x["value"] for x in json_x if x["columnFamily"]==sha1_fn[0] and x["qualifier"]==sha1_fn[1]]
    im_sha1_list = get_list_value(json_x, sha1_fn)
    if not im_sha1_list:
        print "Missing sha1 from row {}".format(doc_id)
        return tup_list
    im_sha1 = im_sha1_list[0]
    #sim_im_sha1_list_list = [x["value"] for x in json_x if x["columnFamily"]==nd_sha1_fn[0] and x["qualifier"]==nd_sha1_fn[1]]
    #dist_list_list = [x["value"] for x in json_x if x["columnFamily"]==ndd_sha1_fn[0] and x["qualifier"]==ndd_sha1_fn[1]]
    sim_im_sha1_list_list = get_list_value(json_x, nd_sha1_fn)
    dist_list_list = get_list_value(json_x, ndd_sha1_fn)
    if not sim_im_sha1_list_list or not dist_list_list:
        print "Missing similar sha1 images from row {}".format(doc_id)
        return tup_list
    # Build similar images tuples
    sim_im_sha1_list = sim_im_sha1_list_list[0].split(',')
    dist_list = dist_list_list[0].split(',')
    for i,sha1 in enumerate(sim_im_sha1_list):
        key = "{}-{}".format(min(sha1,im_sha1).upper(),max(sha1,im_sha1).upper())
        tup_list.append( (key, [key, "info", "dist", str(dist_list[i])]) )
    return tup_list

def fill_sim(sc, hbase_man_in, hbase_man_out):
    in_rdd = hbase_man_in.read_hbase_table()
    sim_images_hb_rdd = in_rdd.flatMap(lambda x: create_sim_images_tuple(x))
    hbase_man_out.rdd2hbase(sim_images_hb_rdd)
    print "[fill_sim] Done."

if __name__ == '__main__':
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name_in = job_conf["tab_name_in"]
    tab_name_out = job_conf["tab_name_out"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName=tab_name_in+'_to_'+tab_name_out)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    in_columns_list = ["meta:sha1", "meta:columbia_near_dups_sha1", "meta:columbia_near_dups_sha1_dist"]
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_name_in, columns_list=in_columns_list)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_name_out)
    fill_sim(sc, hbase_man_in, hbase_man_out)
