#!/usr/bin/env python

"""
    face2feat-spark.py

    !! All of the `sys.path.insert` business is to get around `dlib` not being installed
    on the nodes.  It's inelegant, but seems to work.
"""

import os
import json
import pickle
from time import time
from pyspark import SparkContext
from argparse import ArgumentParser

# --
# Helpers

def detect_faces(img_url, args):
  # Get detector
  global detector
  try:
    _ = detector
  except:
    from cufacesearch.detector.dblib_detector import DLibFaceDetector
    detector = DLibFaceDetector()

  # Detect
  out = []
  try:
    # returns: sha1, img_type, width, height, img, list_det_info_dict
    out.append(detector.detect_from_url(img_url, up_sample=args.up_sample, image_dl_timeout=args.image_dl_timeout))
  except Exception as inst:
    print "Failed to detect face in image at URL: {}. Error ({}) was: {}".format(img_url, type(inst), inst)

  return out

def fill_face_out_dict(sha1, img_type, width, height):
  out_dict = dict()
  out_dict['sha1'] = sha1
  out_dict['img_info'] = dict()
  out_dict['img_info']['format'] = img_type
  out_dict['img_info']['size'] = dict()
  out_dict['img_info']['size']['width'] = width
  out_dict['img_info']['size']['height'] = height
  return out_dict

def build_face_detect_disk_output(data):
  # Format of input data should be: sha1, img_type, width, height, img, list_det_info_dict
  sha1, img_type, width, height, _, list_det_info_dict = data
  faceout_dict = fill_face_out_dict(sha1, img_type, width, height)
  faceout_dict['faces'] = list_det_info_dict
  return json.dumps(faceout_dict)

def featurize_faces(data, conf):
  sha1, img_type, width, height, img, det_info_dict = data

  # Get featurizer
  global featurizer
  try:
    _ = featurizer
  except:
    from cufacesearch.featurizer.dblib_featurizer import DLibFeaturizer
    featurizer = DLibFeaturizer(conf)

  out = []
  # Featurize
  for k, d in enumerate(det_info_dict):
    try:
      fvec = featurizer.featurize(img, d)
      out.append((sha1, img_type, width, height, d, fvec),)
    except Exception as inst:
      print "Failed to featurize face in image {}. Error was: {}".format(sha1, inst)
      pass

  return out

def build_face_feat_disk_output(data):
  #print "[build_face_feat_disk_output.log] data: {}".format(data)
  # Format of input data should be: sha1, img_type, width, height, img, d, fvec
  from cufacesearch.featurizer.featsio import featB64encode
  sha1, img_type, width, height, d, fvec = data
  faceout_dict = fill_face_out_dict(sha1, img_type, width, height)
  faceout_dict['face'] = d
  faceout_dict['feat'] = featB64encode(fvec)
  return json.dumps(faceout_dict)

# --
# Extract faces


if __name__ == "__main__":

  # Options to set path for
  # - shape_predictor_68_face_landmarks.dat
  # - dlib_face_recognition_resnet_model_v1.dat
  # - input
  # - detection_output
  # - features_output
  parser = ArgumentParser()
  #
  parser.add_argument("--base_model_dir", dest='base_model_dir', type=str, default="/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/ColumbiaFaceSearch/www/data/")
  parser.add_argument("--shapepred_model", dest='shapepred_model', type=str, default="shape_predictor_68_face_landmarks.dat")
  parser.add_argument("--facerec_model", dest='facerec_model', type=str, default="dlib_face_recognition_resnet_model_v1.dat")
  parser.add_argument("--input", dest='input', type=str, required=True)
  parser.add_argument("--detection_output", dest='detection_output', type=str, default=None)
  parser.add_argument("--features_output", dest='features_output', type=str, required=True)
  parser.add_argument("--nb_partitions", dest='nb_partitions', type=int, default=1)
  parser.add_argument("--up_sample", dest='up_sample', type=int, default=1)
  parser.add_argument("--image_dl_timeout", dest='image_dl_timeout', type=int, default=20)

  args = parser.parse_args()
  print "Got options:", args

  # Build config from arguments
  # 'pred_path'
  # 'rec_path'
  conf = dict()
  conf['DLIBFEAT_pred_path'] = os.path.join(args.base_model_dir, args.shapepred_model)
  conf['DLIBFEAT_rec_path'] = os.path.join(args.base_model_dir, args.facerec_model)
  print "Conf is:", conf

  # Setup Spark Context
  sc = SparkContext(appName='faces_detect_featurize')

  # Read input, parallelize
  # Input should be a list of (ids, URLs)
  # for now pickle file of list of (sha1, URLs) generated from 'get_new_images_testhappybase.py'
  input_sha1_url_list = pickle.load(open(args.input,'rb'))
  print "input_sha1_url_list: {}".format(input_sha1_url_list.keys())
  # estimated args.nb_partitions from len of input_sha1_url_list?
  rdd_input = sc.parallelize(input_sha1_url_list['update_images'][:10], args.nb_partitions)
  print "rdd_input.first: {}".format(rdd_input.first())

  # Detect
  start_time = time()
  # Should also think about marking detection as processed even if no face were found.
  rdd_face = rdd_input.flatMap(lambda x: detect_faces(x[1], args))

  if args.detection_output:
    print "Saving detections to {}".format(args.detection_output)
    # This discards the image object 'img' of the RDD to just save the info
    rdd_face.map(build_face_detect_disk_output).saveAsTextFile(args.detection_output)

  # Timing only true if we save to disk
  print "Detection run in %d minutes" % int((time() - start_time) / 60)

  # Featurize
  start_time = time()
  rdd_face_feat = rdd_face.flatMap(lambda x: featurize_faces(x, conf))
  rdd_face_feat.map(build_face_feat_disk_output).saveAsTextFile(args.features_output)
  # Should we push to HBase?
  print "Featurization run in %d minutes" % int((time() - start_time) / 60)