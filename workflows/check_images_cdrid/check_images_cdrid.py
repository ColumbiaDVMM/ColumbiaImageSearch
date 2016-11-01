import json
from pyspark import SparkContext, SparkConf

max_images_reduce = 50000


def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def check_wsha1valid_cdrid(data, verbose=False):
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        s3_url = get_list_value(json_x,("info","obj_stored_url"))[0]
        # Check for None here just to be safe
        if s3_url is None or not s3_url.startswith('https://s3'):
            raise ValueError("[check_wsha1valid_cdrid] image {} has an invalid s3_url value.".format(data[0]))
        sha1 = get_list_value(json_x,("info","sha1"))[0]
        # Check for None here just to be safe
        if sha1 is None:
            raise ValueError("[check_wsha1valid_cdrid] image {} has an invalid sha1 value.".format(data[0]))
        return True
    except Exception as inst: # Invalid image
        if verbose:
            print(inst)
        return False


def cdrid_key_to_sha1_key(data):
    cdr_id = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    sha1 = None
    s3_url = None
    try:
        sha1_val = get_list_value(json_x,("info","sha1"))[0]
        if type(sha1_val)==list and len(sha1_val)==1:
            sha1 = sha1_val[0].strip()
        else:
            sha1 = sha1_val.strip()
        s3_url = get_list_value(json_x,("info","obj_stored_url"))[0]
    except Exception as inst:
        print("[cdrid_key_to_sha1_key] could not read sha1 or s3_url for cdr_id {}. Error was: {}".format(cdr_id, inst))
        pass
    if cdr_id and sha1 and s3_url:
        return [(sha1, {"info:all_cdr_ids": [cdr_id], "info:s3_url": [s3_url]})]
    return []


def safe_reduce_infos(a, b, c, field, verbose=False):
    try:
        c[field] = list(set(a[field]+b[field]))
    except Exception as inst:
        try:
            c[field] = a[field]
            if verbose:
                print("[safe_reduce_infos: error] key error for '{}' for b".format(field))
        except Exception as inst2:
            try:
                c[field] = b[field]
                if verbose:
                    print("[safe_reduce_infos: error] key error for '{}' for a".format(field))
            except Exception as inst3:
                c[field] = []
                if verbose:
                    print("[safe_reduce_infos: error] key error for '{}' for both a and b".format(field))
    return c


def safe_assign(a, c, field, fallback):
    if field in a:
        c[field] = a[field]
    else:
        if verbose:
            print("[safe_assign: error] we have no {}.".format(field))
        c[field] = fallback
    return c


def reduce_sha1_infos_discarding(a,b):
    c = dict()
    if b:  # sha1 already existed
        if "info:image_discarded" in a or "info:image_discarded" in b:
            c["info:all_cdr_ids"] = []
            c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images_reduce)
        else:
            # KeyError: 'info:all_cdr_ids'. How could an image not have this field? 
            # Was pushed from MySQL and never indexed in CDR?..
            c = safe_reduce_infos(a, b, c, "info:all_cdr_ids")
        if "info:s3_url" in a and a["info:s3_url"] and a["info:s3_url"][0] and a["info:s3_url"][0]!=u'None':
            c["info:s3_url"] = a["info:s3_url"]
        else:
            if "info:s3_url" in b:
                c["info:s3_url"] = b["info:s3_url"]
            else:
                # That's bad...
                print("[reduce_sha1_infos_discarding: error] both a and b have no s3 url.")
                c["info:s3_url"] = [None]
        # need to keep info:cu_feat_id if it exists
        if "info:cu_feat_id" in b:
            c["info:cu_feat_id"] = b["info:cu_feat_id"]
    else: # brand new image
        c = safe_assign(a, c, "info:s3_url", [None])
        c = safe_assign(a, c, "info:all_cdr_ids", [])
    # should discard if bigger than max(max_images_hbase, max_images_dig)...
    if len(c["info:all_cdr_ids"]) > max_images_reduce:
        print("Discarding image with URL: {}".format(c["info:s3_url"][0]))
        c["info:all_cdr_ids"] = []
        c["info:image_discarded"] = 'discarded because has more than {} cdr_ids'.format(max_images_reduce)
    return c



def check_cdrids(hbase_man_in):
    in_rdd = hbase_man_in.read_hbase_table()
    nb_cdrids = in_rdd.count()
    print('We have {} images.'.format(nb_cdrids))
    cdrids_wsha1valid_rdd = in_rdd.filter(check_wsha1valid_cdrid)
    nb_cdrids_wsha1valid = cdrids_wsha1valid_rdd.count()
    print('We have {} valid images with SHA1 out of {} total images.'.format(nb_cdrids_wsha1valid, nb_cdrids))
    sha1_rdd = cdrids_wsha1valid_rdd.flatMap(cdrid_key_to_sha1_key).reduceByKey(reduce_sha1_infos_discarding)
    nb_sha1valid = sha1_rdd.count()
    print('We have {} valid unique SHA1 images.'.format(nb_sha1valid))
    print("[check_cdrids] DONE.")


if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_cdrid_name = job_conf["tab_cdrid_name"]
    hbase_host = job_conf["hbase_host"]
    sc = SparkContext(appName='check_cdrids_'+tab_cdrid_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_cdrid_name)
    check_cdrids(hbase_man_in)
