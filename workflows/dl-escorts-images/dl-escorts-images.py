import json
from StringIO import StringIO
from pyspark import SparkContext, SparkConf

image_column = "image"

def get_list_value(json_x,field_tuple):
    return [x["value"] for x in json_x if x["columnFamily"]==field_tuple[0] and x["qualifier"]==field_tuple[1]]


def image_not_present(data):
    return True
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        row_image = get_list_value(json_x,("info",image_column))[0]
        # Check for None here just to be safe
        if row_image is None:
            raise ValueError("Image invalid.")
        # Image is there and valid, skipping to avoid re-download
        return False
    except: # Image is missing, we need to download
        return True


def download_image(data):
    import image_dl
    key = data[0]
    json_x = [json.loads(x) for x in data[1].split("\n")]
    try:
        # First check if we can get s3 url
        s3_url = get_list_value(json_x,("info","s3_url"))[0].strip()
        # Check for None here just to be safe
        if s3_url is None or s3_url == u'None' or s3_url == 'None':
            raise ValueError('[download_image: error] s3_url is None.')
        # Download the image
        data = image_dl.get_image_from_URL(unicode(s3_url))
        # Sanity check of sha1
        sha1 = image_dl.get_SHA1_from_data(data)
        if sha1 != key:
            # May be due to an original image in png and s3 in .jpg?
            raise ValueError('[download_image: error] sha1 has not the expected value {} != {}.'.format(sha1,key))
            #print('[download_image: error] sha1 has not the expected value {} != {}.'.format(sha1,key))
        return [(key, [key, "info", image_column, StringIO(data).getvalue()])]
    except Exception as inst: 
        print(inst)
    return []


def fill_binary_image(hbase_man_in, hbase_man_out, nb_images_by_partition):
    import numpy as np
    in_rdd = hbase_man_in.read_hbase_table()
    images_to_dl_rdd = in_rdd.filter(image_not_present)
    nb_images_to_dl = images_to_dl_rdd.count()
    nb_partitions = int(np.ceil(nb_images_to_dl/nb_images_by_partition))
    print('We have {} images, we want a maximum of {} images by partition. So we will partition in {} partitions.'.format(nb_images_to_dl, nb_images_by_partition, nb_partitions))
    out_rdd = images_to_dl_rdd.partitionBy(nb_partitions).flatMap(lambda x: download_image(x))
    hbase_man_out.rdd2hbase(out_rdd)
    nb_images_dled = out_rdd.count()
    print('We have downloaded {} images.'.format(nb_images_dled))
    print("[fill_binary_image] DONE.")


if __name__ == '__main__':
    from hbase_manager import HbaseManager
    job_conf = json.load(open("job_conf.json","rt"))
    print job_conf
    tab_sha1_name = job_conf["tab_sha1_name"]
    hbase_host = job_conf["hbase_host"]
    nb_images_by_partition = job_conf["nb_images_by_partition"]
    sc = SparkContext(appName='dl_images_'+tab_sha1_name)
    sc.setLogLevel("ERROR")
    conf = SparkConf()
    hbase_man_in = HbaseManager(sc, conf, hbase_host, tab_sha1_name, columns=["info:image", "info:s3_url"])
    hbase_man_out = HbaseManager(sc, conf, hbase_host, tab_sha1_name)
    fill_binary_image(hbase_man_in, hbase_man_out, nb_images_by_partition)
