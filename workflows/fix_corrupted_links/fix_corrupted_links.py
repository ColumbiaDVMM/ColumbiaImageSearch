import json
from hbase_manager import HbaseManager
from pyspark import SparkContext, SparkConf

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]

def fix_link(data, cs_list, ct_list):
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
        return tup_list
    im_sha1 = im_sha1_list[0]
    if im_sha1 not in cs_list:
        return tup_list
    #sim_im_sha1_list_list = [x["value"] for x in json_x if x["columnFamily"]==nd_sha1_fn[0] and x["qualifier"]==nd_sha1_fn[1]]
    #dist_list_list = [x["value"] for x in json_x if x["columnFamily"]==ndd_sha1_fn[0] and x["qualifier"]==ndd_sha1_fn[1]]
    sim_im_sha1_list_list = get_list_value(json_x, nd_sha1_fn)
    dist_list_list = get_list_value(json_x, ndd_sha1_fn)
    if not sim_im_sha1_list_list or not dist_list_list:
        #print "Missing similar sha1 images from row {}".format(doc_id)
        return tup_list
    sim_im_sha1_list = sim_im_sha1_list_list[0].split(',')
    dist_list = dist_list_list[0].split(',')
    sim_im_sha1_list_fix = []
    dist_list_fix = [] 
    for i,sha1 in enumerate(sim_im_sha1_list):
        if sha1 not in ct_list:
            sim_im_sha1_list_fix.append(sim_im_sha1_list[i])
            dist_list_fix.append(dist_list[i])
        else:
            print "Discarding corrupted link between image {} and image {}.".format(im_sha1, sha1)
    tup_list.append( (doc_id, [doc_id, "meta", "columbia_near_dups_sha1", str(','.join(sim_im_sha1_list_fix))]) )
    tup_list.append( (doc_id, [doc_id, "meta", "columbia_near_dups_sha1_dist", str(','.join(dist_list_fix))]) )
    return tup_list

def fix_corrupted_links(sc, hbase_man_in, hbase_man_out, cs_list, ct_list, nb_partitions):
    in_rdd = hbase_man_in.read_hbase_table().partitionBy(nb_partitions)
    fixed_corrupted_links_rdd = in_rdd.flatMap(lambda x: fix_link(x, cs_list, ct_list))
    hbase_man_out.rdd2hbase(fixed_corrupted_links_rdd)
    print "[fix_corrupted_links] Done."

if __name__ == '__main__':
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    with open('corrupted_targets.csv','rt') as ct:
        ct_list = [x.strip()[1:-1] for x in ct]
    #with open('corrupted_sources_dev.csv','rt') as cs:
    with open('corrupted_sources.csv','rt') as cs:
        cs_list = [x.strip()[1:-1] for x in cs]
    print("[fix_corrupted_links] Initial 'cs_list' has {} entries.".format(len(cs_list)))
    tab_images_name = job_conf["tab_name_in"]
    nb_partitions = job_conf["nb_partitions"]
    hbase_host = job_conf["hbase_host"]
    source_start = job_conf["source_start"]
    source_stop = job_conf["source_stop"]
    cs_list = cs_list[source_start:source_stop]
    print("[fix_corrupted_links] Truncated 'cs_list' from {} to {} has {} entries.".format(source_start, source_stop, len(cs_list)))
    row_start = '52004860'
    row_stop = '52104880'
    sc = SparkContext(appName='fix_corrupted_links_'+tab_images_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    in_columns_list = ["meta:sha1", "meta:columbia_near_dups_sha1", "meta:columbia_near_dups_sha1_dist"]
    #hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_images_name, columns_list=in_columns_list, row_start=row_start, row_stop=row_stop)
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_images_name, columns_list=in_columns_list)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_images_name)
    fix_corrupted_links(sc, hbase_man_in, hbase_man_out, cs_list, ct_list, nb_partitions)
