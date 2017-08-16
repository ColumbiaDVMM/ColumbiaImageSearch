#!/usr/bin/env python

import os
import sys
import json
import pickle
from time import time
from pyspark import SparkContext
from argparse import ArgumentParser

# --
# Detect faces

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
    sys.stdout.flush()
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
  out = ""
  try:
    sha1, img_type, width, height, _, list_det_info_dict = data
    faceout_dict = fill_face_out_dict(sha1, img_type, width, height)
    faceout_dict['faces'] = list_det_info_dict
    out = json.dumps(faceout_dict)
  except Exception as inst:
    print "Failed to build face detect output. Error ({}) was: {}".format(type(inst), inst)
    sys.stdout.flush()
  return out

# --
# Extract faces features

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
      print "Failed to featurize face in image {}. Error ({}) was: {}".format(sha1, type(inst), inst)
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


def process_one_url(img_url, args, conf):
  out = []
  det_out = detect_faces(img_url, args)
  if det_out:
    for det_data in det_out:
      feat_out = featurize_faces(det_data, conf)
      for feat_data in feat_out:
        out.append(build_face_feat_disk_output(feat_data))
  return out


# --

if __name__ == "__main__":

  # Options to set path for
  # - shape_predictor_68_face_landmarks.dat
  # - dlib_face_recognition_resnet_model_v1.dat
  # - input
  # - detection_output
  # - features_output
  parser = ArgumentParser()
  #parser.add_argument("--base_model_dir", dest='base_model_dir', type=str, default="/Users/svebor/Documents/Workspace/CodeColumbia/MEMEX/ColumbiaFaceSearch/www/data/")
  parser.add_argument("--base_model_dir", dest='base_model_dir', type=str, default="hdfs://memex/user/skaraman/data/facesearch/")
  parser.add_argument("--shapepred_model", dest='shapepred_model', type=str, default="shape_predictor_68_face_landmarks.dat")
  parser.add_argument("--facerec_model", dest='facerec_model', type=str, default="dlib_face_recognition_resnet_model_v1.dat")
  parser.add_argument("--input", dest='input', type=str, required=True)
  parser.add_argument("--detection_output", dest='detection_output', type=str, default=None)
  parser.add_argument("--features_output", dest='features_output', type=str, required=True)
  parser.add_argument("--nb_partitions", dest='nb_partitions', type=int, default=20000)
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

  start_time = time()

  # Read input, parallelize
  # Input should be a list of (ids, URLs)
  # for now pickle file of list of (sha1, URLs) generated from 'get_new_images_testhappybase.py'
  input_sha1_url_list = pickle.load(open(args.input,'rb'))
  print "input_sha1_url_list: {}".format(input_sha1_url_list.keys())

  # sc.parallelize(input_sha1_url_list['update_images'], args.nb_partitions)\
  #   .flatMap(lambda x: detect_faces(x[1], args))\
  #   .flatMap(lambda x: featurize_faces(x, conf))\
  #   .map(build_face_feat_disk_output)\
  #   .saveAsTextFile(args.features_output)

  sc.parallelize(input_sha1_url_list['update_images'], args.nb_partitions) \
    .flatMap(lambda x: process_one_url(x[1], args, conf)) \
    .saveAsTextFile(args.features_output)

  # Should we push to HBase?
  print "Detection adn featurization run in %d minutes" % int((time() - start_time) / 60)

