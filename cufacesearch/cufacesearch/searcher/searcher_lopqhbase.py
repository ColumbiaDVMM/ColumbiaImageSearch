import os
import numpy as np

from generic_searcher import GenericSearcher

START_HDFS = '/user/'

class SearcherLOPQHBase(GenericSearcher):

  def set_pp(self):
    self.pp = "SearcherLOPQHBase"

  def init_searcher(self):
    """ Initialize LOPQ model and searcher from `global_conf` value.
    """
    import pickle
    # Get model type from conf file
    lopq_model_type = self.get_param('lopq')
    lopq_model = None
    # Deal with potential different LOPQ model types
    if not lopq_model_type:
      raise ValueError("[{}: error] 'lopq' is not defined in configuration file.".format(self.pp))
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
          lopq_model = pickle.load(open(lopq_model_path, "rb"))
      else:
        print "[{}: info] Emtpy lopq model path".format(self.pp)
    else:
      raise ValueError("[{}: error] Unknown 'lopq' type {}.".format(self.pp, lopq_model_type))

    # Setup searcher with LOPQ model
    if lopq_model:
      from lopq.search import LOPQSearcher
      self.searcher = LOPQSearcher(lopq_model)
      # NB: an empty lopq_model would make sense only if we just want to detect...

  def load_codes(self):
    # TODO: how to deal with updates?
    if self.codes_path:
      if self.codes_path.startswith(START_HDFS):
        self.searcher.add_codes_from_hdfs(self.codes_path)
      else:
        self.searcher.add_codes_from_local(self.codes_path)
    else:
      print "[{}: info] 'codes_path' is not defined or empty in configuration file.".format(self.pp)

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
          if self.searcher:
            pca_projected_feat = np.squeeze(self.searcher.model.apply_PCA(feats[i][j] / norm_feat))
            # print "[SearcherLOPQHBase.search_from_feats: log] pca_projected_feat.shape: {}".format(pca_projected_feat.shape)
            # format of results is a list of namedtuples as: namedtuple('Result', ['id', 'code', 'dist'])
            results, visited = self.searcher.search(pca_projected_feat,
                                                         quota=self.quota,
                                                         limit=self.sim_limit,
                                                         with_dists=True)
            print "[{}.search_from_feats: log] got {} results, first one is: {}".format(self.pp, len(results), results[0])
        tmp_img_sim = []
        tmp_face_sim_ids = []
        tmp_face_sim_score = []
        for res in results:
          if (filter_near_dup and res.dist <= near_dup_th) or not filter_near_dup:
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
    return self.do.format_output(dets, sim_images, sim_faces, sim_score, options_dict)
