from cufacesearch.featurizer.featsio import read_features_from_tsv
from cufacesearch.detector.utils import show_bbox_from_URL

if __name__ == "__main__":
    feats_path = "../data/part-00000"
    #feats_path = "../data/sample_data_ben/chunk1/part-00000"
    images_sha1s, images_urls, faces_bbox, faces_feats = read_features_from_tsv(feats_path)
    print "Read {} faces features of {} dimensions.".format(len(images_sha1s), len(faces_feats[0]))

    for img_i,sha1 in enumerate(images_sha1s):
        print sha1, faces_bbox[img_i]
        show_bbox_from_URL(images_urls[img_i], faces_bbox[img_i], close_after=1)
