import json
from pyspark import SparkContext, SparkConf


def prepare_mark_precomp(data):
    key = str(data).rstrip()
    #print("[prepare_mark_precomp] data: {}".format(data))
    return [(key, [key, "info", "precomp_sim", "True"])]


def mark_precomp_sim(hbase_man_in, hbase_man_out):
    in_rdd = hbase_man_in.read_hbase_table()
    existing_sims = in_rdd.keys()
    existing_sims_count = existing_sims.count()
    print("existing_sims count: {}".format(existing_sims_count))
    sample_existing_sims = existing_sims.first()
    print("existing_sims first: {}".format(sample_existing_sims))
    out_rdd = existing_sims.flatMap(prepare_mark_precomp)
    sample_out_rdd = out_rdd.take(5)
    print("out_rdd sample: {}".format(sample_out_rdd))
    hbase_man_out.rdd2hbase(out_rdd)
            

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_sim_name = job_conf["tab_sim"]
    tab_sha1_infos_name = job_conf["tab_sha1_infos"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='mark-precomp-sim_from_'+tab_sim_name+'_to'+tab_sha1_infos_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_sim_name)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_sha1_infos_name)
    mark_precomp_sim(hbase_man_in, hbase_man_out)
