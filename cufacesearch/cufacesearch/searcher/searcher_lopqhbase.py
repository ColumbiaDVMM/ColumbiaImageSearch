import os
import time
import json
import numpy as np
from collections import OrderedDict

from output_mapping import DictOutput


START_HDFS = '/user/'


class SearcherLOPQHBase():

  def __init__(self, global_conf_filename, prefix="FSE_"):
    self.global_conf_filename = global_conf_filename
    self.prefix = prefix
    self.global_conf = json.load(open(global_conf_filename,'rt'))
    self.searcher_lopq = None
    self.verbose = 1
    self.read_conf()
    self.init_lopq()
    self.init_hbaseindexer()
    self.init_detector()
    self.init_featurizer()
    # should codes path be a list to deal with updates?
    # should we store that list in HBase?
    self.codes_path = self.get_param('codes_path')
    dict_output_type = self.get_param('dict_output_type')
    if dict_output_type:
      self.do = DictOutput(dict_output_type)
    else:
      self.do = DictOutput()
    self.load_codes()
    self.url_field = 'info:s3_url'
    self.needed_output_columns = [self.url_field]

  def get_param(self, param):
    key_param = self.prefix+param
    if key_param in self.global_conf:
      return self.global_conf[key_param]
    if self.verbose:
      print '[get_param: info] could not find {} in configuration'.format(key_param)


  def read_conf(self):
    # these parameters may be overwritten by web call
    self.sim_limit = self.get_param('sim_limit')
    self.quota = self.sim_limit*10
    self.near_dup = bool(self.get_param('near_dup'))
    self.near_dup_th =  self.get_param('near_dup_th')
    self.ratio = self.get_param('ratio')
    self.top_feature = 0
    tmp_top_feature = self.get_param('top_feature')
    if tmp_top_feature:
        self.top_feature = int(tmp_top_feature)
        
  def init_lopq(self):
    """ Initialize LOPQ model and searcher from `global_conf` value.
    """
    import pickle
    # Get model type from conf file
    lopq_model_type = self.get_param('lopq')
    lopq_model = None
    # Deal with potential different LOPQ model types
    if not lopq_model_type:
      raise ValueError("[Searcher: error] 'lopq' is not defined in configuration file.")
    elif lopq_model_type == "lopq_pca":
      # this is from our modified LOPQ package...
      # https://github.com/ColumbiaDVMM/ColumbiaImageSearch/tree/master/workflows/build-lopq-index/lopq/python
      # 'LOPQModelPCA' will be the type of the model loaded from pickle file
      from lopq.model import LOPQModelPCA
      lopq_model_path = self.get_param('lopqmodel')
      if lopq_model_path:
        # deal with HDFS path
        if lopq_model_path.startswith(START_HDFS):
          from lopq.utils import copy_from_hdfs
          import shutil
          filename = copy_from_hdfs(lopq_model_path)
          lopq_model = pickle.load(filename)
          try:
            shutil.rmtree(os.path.dirname(filename))
          except:
            pass
        else:
          # local path in config
          lopq_model = pickle.load(open(lopq_model_path,"rb"))
      else:
        print "[SearcherLOPQHBase: info] Emtpy lopq model path"
    else:
      raise ValueError("[SearcherLOPQHBase: error] Unknown 'lopq' type {}.".format(lopq_model_type))

    # Setup searcher with LOPQ model
    if lopq_model:
      from lopq.search import LOPQSearcher
      self.searcher_lopq = LOPQSearcher(lopq_model)
    # NB: an empty lopq_model would make sense only if we just want to detect...

  def init_hbaseindexer(self):
    """ Initialize HBase Indexer from `global_conf` value.
    """
    # Get indexed type from conf file
    indexer_type = self.get_param('indexer')
    if not indexer_type:
        raise ValueError("[SearcherLOPQHBase: error] 'indexer' is not defined in configuration file.")
    elif indexer_type == "hbase_indexer_minimal":
        from ..indexer.hbase_indexer_minimal import HBaseIndexerMinimal
        self.indexer = HBaseIndexerMinimal(self.global_conf_filename)
    else:
        raise ValueError("[SearcherLOPQHBase: error] unkown 'indexer' {}.".format(indexer_type))

  def init_detector(self):
    """ Initialize Face Detector from `global_conf` value.
    """
    # Get indexed type from conf file
    detector_type = self.get_param('detector')
    if not detector_type:
      raise ValueError("[SearcherLOPQHBase: error] 'detector' is not defined in configuration file.")
    elif detector_type == "dblib_detector":
      from ..detector import dblib_detector
      self.detector = dblib_detector.DLibFaceDetector()
    else:
      raise ValueError("[SearcherLOPQHBase: error] unkown 'detector' {}.".format(detector_type))


  def init_featurizer(self):
    """ Initialize Feature Extractor from `global_conf` value.
    """
    featurizer_type = self.get_param('featurizer')
    if not featurizer_type:
      raise ValueError("[SearcherLOPQHBase: error] 'featurizer' is not defined in configuration file.")
    elif featurizer_type == "dlib_featurizer":
      from ..featurizer.dblib_featurizer import DLibFeaturizer
      self.featurizer = DLibFeaturizer(self.global_conf_filename)
    else:
      raise ValueError("[SearcherLOPQHBase: error] unkown 'featurizer' {}.".format(featurizer_type))

  def load_codes(self):
    # TODO: how to deal with updates?
    if self.codes_path:
      if self.codes_path.startswith(START_HDFS):
        self.searcher_lopq.add_codes_from_hdfs(self.codes_path)
      else:
        self.searcher_lopq.add_codes_from_local(self.codes_path)
    else:
      print "[Searcher: info] 'codes_path' is not defined or empty in configuration file."

  def check_ratio(self):
    '''Check if we need to set the ratio based on top_feature.'''
    if self.top_feature > 0:
      self.ratio = self.top_feature*1.0/len(self.searcher_lopq.nb_indexed)
      log_msg = "[Searcher.check_ratio: log] Set ratio to {} as we want top {} images out of {} indexed."
      print log_msg.format(self.ratio, self.top_feature, len(self.searcher_lopq.nb_indexed))


  def format_output(self, dets, sim_images, sim_faces, sim_score, options_dict=dict()):
    # read lopq similarity results and get 'cached_image_urls', 'ads_cdr_ids'
    # and filter out if near_dup is activated
    print "[SearcherLOPQHBase.format_output: log] options are: {}".format(options_dict)
    start_build_output = time.time()
    output = []
    images_query = set()
    nb_faces_query = 0
    nb_faces_similar = 0

    if self.verbose > 0:
      print dets, sim_images, sim_faces, sim_score

    for i in range(len(dets)):

      # No face detected in query
      if not dets[i][1]:
        output.append(dict())
        out_i = len(output) - 1
        output[out_i][self.do.map['query_sha1']] = dets[i][0]
        if dets[i][2]:
          output[out_i][self.do.map['query_url']] = dets[i][2]
        output[out_i][self.do.map['img_info']] = dets[i][3:5]
        images_query.add(dets[i][0])
        output[out_i][self.do.map['similar_faces']] = OrderedDict([[self.do.map['number_faces'], 0],
                                                               [self.do.map['image_sha1s'], []],
                                                               [self.do.map['faces'], []],
                                                               [self.do.map['cached_image_urls'], []],
                                                               [self.do.map['distances'], []]])
        continue

      # We found some faces...
      for j, face_bbox in enumerate(dets[i][1]):
        nb_faces_query += 1

        # Add one output for each face query
        output.append(dict())
        out_i = len(output) - 1
        output[out_i][self.do.map['query_sha1']] = dets[i][0]
        output[out_i][self.do.map['query_face']] = dets[i][1][j]
        if dets[i][2]:
          output[out_i][self.do.map['query_url']] = dets[i][2]
        output[out_i][self.do.map['img_info']] = dets[i][3:]
        images_query.add(dets[i][0])

        nb_faces = 0
        if sim_faces[i]:
          if sim_faces[i][j]:
            nb_faces = len(sim_faces[i][j])

        output[out_i][self.do.map['similar_faces']] = OrderedDict([[self.do.map['number_faces'], nb_faces],
                                                              [self.do.map['image_sha1s'], []],
                                                              [self.do.map['faces'], []],
                                                              [self.do.map['img_info'], []],
                                                              [self.do.map['cached_image_urls'], []],
                                                              [self.do.map['distances'], []]])
        # Explore list of similar faces
        for jj in range(nb_faces):
          sim_face = sim_faces[i][j][jj]
          nb_faces_similar += 1
          output[out_i][self.do.map['similar_faces']][self.do.map['image_sha1s']].append(sim_images[i][j][jj][0].strip())
          output[out_i][self.do.map['similar_faces']][self.do.map['cached_image_urls']].append(sim_images[i][j][jj][1][self.url_field].strip())
          output[out_i][self.do.map['similar_faces']][self.do.map['faces']].append('-'.join(sim_face.split('_')[1:]))
          # this is not in HBase for all/most images...
          #osf_imginfo = sim_images[i][j][jj][1][self.img_info_field].strip()
          output[out_i][self.do.map['similar_faces']][self.do.map['img_info']].append('')
          output[out_i][self.do.map['similar_faces']][self.do.map['distances']].append(sim_score[i][j][jj])

    outp = OrderedDict([[self.do.map['number_images'], len(images_query)],
                        [self.do.map['number_faces'], nb_faces_query],
                        [self.do.map['number_similar_faces'], nb_faces_similar],
                        [self.do.map['all_similar_faces'], output]])

    print "[SearcherLOPQHBase.format_output: log] build_output took: {}".format(time.time() - start_build_output)
    return outp

  def search_image_list(self, image_list, options_dict=dict()):
    return self._search_from_any_list(image_list, self.detector.detect_from_url, options_dict)

  def search_imageB64_list(self, imageB64_list, options_dict=dict()):
    return self._search_from_any_list(imageB64_list, self.detector.detect_from_b64, options_dict)


  def _search_from_any_list(self, image_list, detect_fn, options_dict):
    dets = []
    feats = []
    # for each image
    for image in image_list:
      # first detect faces
      sha1, img_type, width, height, img, faces = detect_fn(image)
      if image.startswith('http'):
        dets.append((sha1, faces, image, img_type, width, height))
      else:
        dets.append((sha1, faces, None, img_type, width, height))
      # if we found faces, get features for each face
      faces_feats = []
      # check if we were asked only to perform detection
      if "detect_only" not in options_dict or not options_dict["detect_only"]:
        for one_face in faces:
          print one_face
          one_feat = self.featurizer.featurize(img, one_face)
          faces_feats.append(one_feat)
      feats.append(faces_feats)

    # search from all faces features
    return self.search_from_feats(dets, feats, options_dict)


  def search_from_feats(self, dets, feats, options_dict=dict()):
    sim_images = []
    sim_faces = []
    sim_score = []
    # check what is the near duplicate config
    filter_near_dup = False
    if (self.near_dup and "near_dup" not in options_dict) or ("near_dup" in options_dict and options_dict["near_dup"]):
      filter_near_dup = True
      if "near_dup_th" in options_dict:
        near_dup_th = options_dict["near_dup_th"]
      else:
        near_dup_th = self.near_dup_th
    # query for each feature
    for i in range(len(dets)):

      for j in range(len(dets[i][1])):
        results = []
        if "detect_only" not in options_dict or not options_dict["detect_only"]:
          norm_feat = np.linalg.norm(feats[i][j])
          if self.searcher_lopq:
            pca_projected_feat = np.squeeze(self.searcher_lopq.model.apply_PCA(feats[i][j]/norm_feat))
            #print "[SearcherLOPQHBase.search_from_feats: log] pca_projected_feat.shape: {}".format(pca_projected_feat.shape)
            # format of results is a list of namedtuples as: namedtuple('Result', ['id', 'code', 'dist'])
            results, visited = self.searcher_lopq.search(pca_projected_feat,
                                                         quota=self.quota,
                                                         limit=self.sim_limit,
                                                         with_dists=True)
            print "[SearcherLOPQHBase.search_from_feats: log] got {} results, first one is: {}".format(len(results), results[0])
        tmp_img_sim = []
        tmp_face_sim_ids = []
        tmp_face_sim_score = []
        for res in results:
          if (filter_near_dup and res.dist<=near_dup_th) or not filter_near_dup:
            tmp_face_sim_ids.append(res.id)
            # here id would be face_id that we could build as sha1_facebbox?
            tmp_img_sim.append(str(res.id).split('_')[0])
            tmp_face_sim_score.append(res.dist)
        if tmp_img_sim:
          rows = self.indexer.get_columns_from_sha1_rows(tmp_img_sim, self.needed_output_columns)
          # rows should contain id, s3_url of images
          sim_images.append(rows)
          sim_faces.append(tmp_face_sim_ids)
          sim_score.append(tmp_face_sim_score)
        else:
          sim_images.append([])
          sim_faces.append([])
          sim_score.append([])

    # format output
    return self.format_output(dets, sim_images, sim_faces, sim_score, options_dict)