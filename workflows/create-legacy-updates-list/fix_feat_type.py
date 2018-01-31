"""
Force the features of an extraction type to be stored as a specific numpy type.

Used to solve discrepancy between legacy dlib features and new features computed from HG crawls.
"""

from __future__ import print_function

import json
from pyspark import SparkContext, SparkConf


VALID_EXTR_TYPE = ["dlib_feat_dlib_face"]
PROCESSED_SUFFIX = "_processed"
UPDATEID_SUFFIX = "_updateid"
EXTR_COLUMN_FAMILY = "ext"


def get_tuple_list_value_start(json_x, field_tuple):
    """Retrieves list of tuples from 'json_x' that are of column family field_tuple[0] and have
    a column qualifier starting with field_tuple[1]

    :param json_x: decoded hbase row columns and values
    :param field_tuple: target (column_family, start of column_qualifier)
    :return: list of tuples (column_qualifier, value)
    """
    # return [(x["qualifier"], x["value"]) for x in json_x if x["columnFamily"] == field_tuple[0] and
    #         x["qualifier"].startswith(field_tuple[1])]
    # Necessary?
    return [(x["qualifier"].decode('utf-8'), x["value"].decode('utf-8'))
            for x in json_x if x["columnFamily"] == field_tuple[0] and
            x["qualifier"].startswith(field_tuple[1])]


def get_np_type(str_np_type):
    """Get numpy type from string.

    :param str_np_type: string of numpy type
    :return: numpy type
    """
    import numpy as np
    if str_np_type == "float32":
        return np.float32
    elif str_np_type == "float64":
        return np.float64
    else:
        raise ValueError("[ERROR] Unsupported numpy type: {}".format(str_np_type))


def update_one_feat(feat_b64):
    """Decode 'feat_b64' and re-encode it after changing CURRENT_FEAT_NUMPY_TYPE to
    TARGET_FEAT_NUMPY_TYPE.

    :param feat_b64: base64 encoded feature with type CURRENT_FEAT_NUMPY_TYPE
    :return: base64 encoded feature with type TARGET_FEAT_NUMPY_TYPE
    """
    import base64
    import numpy as np

    try:
        feat = np.frombuffer(base64.b64decode(feat_b64), dtype=get_np_type(CURRENT_FEAT_NUMPY_TYPE))
    except Exception as inst:
        err_msg = "[ERROR] Could not decode feature from value {}. {}"
        print(err_msg.format(feat_b64, inst))
        raise inst

    err_msg = "[ERROR] Feature does not have expected shape {} vs. {}"
    if feat.shape[-1] != FEAT_DIMENSION:
        raise ValueError(err_msg.format(feat.shape[-1], FEAT_DIMENSION)+' before transformation')
    else:
        feat = feat.astype(get_np_type(TARGET_FEAT_NUMPY_TYPE))
    if feat.shape[-1] != FEAT_DIMENSION:
        raise ValueError(err_msg.format(feat.shape[-1], FEAT_DIMENSION)+' after transformation')
    return base64.b64encode(feat)


def fix_one_row(row):
    """Fix feature type for one row for EXTR_TYPE.

    :param row: input row as loaded by hbase_manager
    :return: updated values (if any)
    """
    json_x = [json.loads(x) for x in row[1].split("\n")]
    # Get all extraction columns related to extr_type
    extr_tuple_list_value = get_tuple_list_value_start(json_x, (EXTR_COLUMN_FAMILY, EXTR_TYPE))
    # Properly prepare updated output, should just contain the updated columns
    updated_values = []
    # Update each saved feature
    for tlv in extr_tuple_list_value:
        #print(row[0], tlv)
        # Encoding issue for this test?
        if tlv[0] != EXTR_TYPE+PROCESSED_SUFFIX and \
                tlv[0] != EXTR_TYPE+UPDATEID_SUFFIX:
            try:
                updated_feat = update_one_feat(tlv[1])
                # Add update feature
                updated_values.append((row[0], [row[0], EXTR_COLUMN_FAMILY, tlv[0], updated_feat]))
            except ValueError as inst:
                err_msg = "[ERROR] When reading feature from column {} for key {}. {}"
                print(err_msg.format(tlv[0], row[0], inst))

    return updated_values


def fix_feat_type():
    """Fix feature type in table TAB_SHA1_INFOS_NAME for extraction EXTR_TYPE.
    """
    # Fix each image features
    in_rdd = HBASE_MAN_IMAGES_IN.read_hbase_table()
    fixed_rdd = in_rdd.flatMap(fix_one_row)
    HBASE_MAN_IMAGES_OUT.rdd2hbase(fixed_rdd)


if __name__ == '__main__':
    # Read configuration file
    JOB_CONF = json.load(open("job_conf_dlib_fixfeat.json", "rt"))
    print(JOB_CONF)
    TAB_SHA1_INFOS_NAME = JOB_CONF["tab_sha1_infos"]
    HBASE_HOST = JOB_CONF["hbase_host"]
    # Should be "dlib_feat_dlib_face"
    EXTR_TYPE = JOB_CONF["extr_type"]
    CURRENT_FEAT_NUMPY_TYPE = JOB_CONF["current_feat_numpy_type"]
    TARGET_FEAT_NUMPY_TYPE = JOB_CONF["target_feat_numpy_type"]
    FEAT_DIMENSION = int(JOB_CONF["feat_dimension"])
    if EXTR_TYPE not in VALID_EXTR_TYPE:
        raise ValueError("Unexpected extr_type: {}".format(EXTR_TYPE))

    # Set up spark related objects
    from hbase_manager import HbaseManager
    SC = SparkContext(appName='fix_'+EXTR_TYPE+'_legacy_'+TAB_SHA1_INFOS_NAME)
    SC.setLogLevel("ERROR")
    CONF = SparkConf()
    HBASE_MAN_IMAGES_IN = HbaseManager(SC, CONF, HBASE_HOST, TAB_SHA1_INFOS_NAME)
    HBASE_MAN_IMAGES_OUT = HbaseManager(SC, CONF, HBASE_HOST, TAB_SHA1_INFOS_NAME)

    # Run
    fix_feat_type()
