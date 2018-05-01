import json
from datetime import datetime
import happybase
from pyspark import SparkContext, SparkConf
TTransportException = happybase._thriftpy.transport.TTransportException


def get_create_table(table_name, conn, families={'info': dict()}):
    try:
        # what exception would be raised if table does not exist, actually none.
        # need to try to access families to get error
        table = conn.table(table_name)
        # this would fail if table does not exist
        _ = table.families()
        return table
    except Exception as inst:
        # TODO: act differently based on error type (connection issue or actually table missing)
        if type(inst) == TTransportException:
            raise inst
        else:
            print "[get_create_table: info] table {} does not exist (yet): {}{}".format(table_name, type(inst),
                                                                                        inst)
            conn.create_table(table_name, families)
            table = conn.table(table_name)
            print "[get_create_table: info] created table {}".format(table_name)
            return table

def get_list_value(json_x, field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def get_list_qualifier_value(json_x, column_family):
    return [(x["qualifier"], x["value"]) for x in json_x if x["columnFamily"] == column_family]


def get_ads_ids(json_x, key):
    ads_ids = []
    try:
        ads_ids_str_list = get_list_value(json_x, ("info", "all_parent_ids"))
        if ads_ids_str_list:
            ads_ids = ads_ids_str_list[0].strip().split(',')
    except Exception as inst:
        print "[Error] could not get ads ids for row {}. {}".format(key, inst)
    return ads_ids


def get_s3url(json_x, key):
    s3url = None
    try:
        s3url_list = get_list_value(json_x, ("info", "s3_url"))
        if s3url_list:
            s3url = s3url_list[0].strip()
    except Exception as inst:
        print "[Error] could not get s3url for row {}. {}".format(key, inst)
    return s3url

def get_sbimage_feat(json_x, key):
    sb_feat = None
    try:
        sb_feat_list = get_list_value(json_x, ("info", "featnorm_cu"))
        if sb_feat_list:
            sb_feat = sb_feat_list[0]
    except Exception as inst:
        print "[Error] could not get sb_feat for row {}. {}".format(key, inst)
    return sb_feat


def get_dlibface_feat(json_x, key):
    dlibface_feat_list = []
    try:
        dlibface_feat_list = get_list_qualifier_value(json_x, "face")
        if dlibface_feat_list:
            # parse?
            pass
    except Exception as inst:
        print "[Error] could not get sha1 for row {}. {}".format(key, inst)
    return dlibface_feat_list

def transform(data):
    key = data[0]
    fields = []
    try:
        json_x = [json.loads(x) for x in data[1].split("\n")]

        # Get ads ids
        ads_ids = get_ads_ids(json_x, key)
        #print "ads_ids",ads_ids
        if ads_ids:
            for ad_id in ads_ids:
                # build timestamp as we would get it from CDR or Kafka topic...
                ts = datetime.utcnow().isoformat()+'Z'
                fields.append((key, [key, "ad", str(ad_id), str(ts)]))
        else: # old image that cannot be linked to any ad...
            print "Image with key {} cannot be linked to any ad. Discarding.".format(key)
            return []
        # Get s3url
        s3_url = get_s3url(json_x, key)
        #print "s3_url",s3_url
        if s3_url:
            fields.append((key, [key, "info", "s3_url", s3_url]))
        else: # consider an image without s3_url is invalid
            print "Image with key {} has no 's3_url'. Discarding.".format(key)
            return []
        # Get image feature
        sb_feat = get_sbimage_feat(json_x, key)
        if sb_feat:
            fields.append((key, [key, "ext", "sbcmdline_feat_full_image", sb_feat]))
        # Get face feature
        dlibface_feat_list = get_dlibface_feat(json_x, key)
        # format here would be a list of tuple (dlib_feat_dlib_face_64_31_108_74, featB64)
        if dlibface_feat_list:
            for dlibface_feat_id, dlibface_feat_value in dlibface_feat_list:
                # Should we add a fake score to dlibface_feat_id?
                fields.append((key, [key, "ext", dlibface_feat_id, dlibface_feat_value]))
    except Exception as inst:
        print "Transformation failed ({}) for key {}: {}".format(inst, key, fields)
    return fields

def transform_table(hbase_man_in, hbase_man_out):
    in_rdd = hbase_man_in.read_hbase_table()
    out_rdd = in_rdd.flatMap(lambda x: transform(x))
    hbase_man_out.rdd2hbase(out_rdd)

if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_name_in = job_conf["tab_name_in"]
    tab_name_out = job_conf["tab_name_out"]
    # Try to create "tab_name_out"
    tab_out_families = job_conf["tab_out_families"]
    hbase_conn_timeout = None
    nb_threads = 1
    pool = happybase.ConnectionPool(size=nb_threads, host='10.1.94.57', timeout=hbase_conn_timeout)
    with pool.connection() as conn:
        get_create_table(tab_name_out, conn, tab_out_families)
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='transform_'+tab_name_in+'_to_'+tab_name_out)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_name_in)
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_name_out)
    transform_table(hbase_man_in, hbase_man_out)
