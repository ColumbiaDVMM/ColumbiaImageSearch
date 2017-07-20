
def read_features_from_tsv(tsv_file):

    # Initialize
    images_sha1s = []
    images_urls = []
    faces_bbox = []
    faces_feats = []

    # Read every line
    with open(tsv_file, 'rt') as infile:
        for line in infile:
            # Expected line format is:
            # sha1\ts3_url\t./local_path_to_be_ignored.jpg\tface_top\tface_bottom\tface_left\tface_right\tfeat_val1\t...\n
            fields = line.strip().split('\t')

            images_sha1s.append(fields[0])
            images_urls.append(fields[1])
            # cast face bbox coordinates to integers
            tmp_bbox = [int(x) for x in fields[3:7]]
            # reorder in left, top, right, bottom
            faces_bbox.append(tuple([tmp_bbox[2], tmp_bbox[0], tmp_bbox[3], tmp_bbox[1]]))
            # cast face features to floats
            faces_feats.append(list([float(x) for x in fields[7:]]))

    return images_sha1s, images_urls, faces_bbox, faces_feats