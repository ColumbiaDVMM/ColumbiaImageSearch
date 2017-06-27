def udf(sc, data_path, sampling_ratio, seed, args=None, repartition=False):
    """
    MEMEX UDF function to load testing data. 
    Loads data from a sequence file containing JSON formatted data with 
    a base64-encoded numpy arrays in field 'feat_field' maintaining each sample id.
    """
    from memex_udf import memex_udf_wid
    feat_field = 'featnorm_tf'
    return memex_udf_wid(sc, data_path, sampling_ratio, seed, feat_field, args, repartition)