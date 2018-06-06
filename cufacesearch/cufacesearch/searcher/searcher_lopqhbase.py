from __future__ import print_function

import sys
import time
from datetime import datetime

import lmdb
import numpy as np

#from thriftpy.thrift.transport.TTransport import TTransportException
#from ..indexer.hbase_indexer_minimal import column_list_sha1s, update_str_processed
from generic_searcher import GenericSearcher
from ..featurizer.generic_featurizer import get_feat_size
from ..featurizer.featsio import get_feat_dtype
from ..storer.s3 import S3Storer
from ..common.error import full_trace_error

START_HDFS = '/user/'
DATE_DB_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

default_prefix = "SEARCHLOPQ_"


class SearcherLOPQHBase(GenericSearcher):
  """SearcherLOPQHBase
  """

  def __init__(self, global_conf_in, prefix=default_prefix):
    """SearcherLOPQHBase constructor

    :param global_conf_in: configuration file or dictionary
    :type global_conf_in: str, dict
    :param prefix: prefix in configuration
    :type prefix: str
    """
    # number of processors to use for parallel computation of codes
    self.num_procs = 8  # could be read from configuration
    self.model_params = None
    self.get_pretrained_model = True
    self.nb_train_pca = 100000
    self.last_refresh = datetime.now()
    # NB: in load_codes full_refresh default is false...
    self.last_full_refresh = datetime.now()
    self.last_indexed_update = None
    self.pca_model_str = None
    self.skipfailed = False
    # making LOPQSearcherLMDB the default LOPQSearcher
    self.lopq_searcher = "LOPQSearcherLMDB"
    super(SearcherLOPQHBase, self).__init__(global_conf_in, prefix)
    self.set_pp(pp="SearcherLOPQHBase")

  def get_model_params(self):
    """Reads model parameters from configuration

    Required parameters:

    - ``lopq_V``
    - ``lopq_M``
    - ``lopq_subq``
    - ``lopq_pcadims``
    - ``nb_train_pca``

    Optional parameters:

    - ``nb_min_train_pca``
    - ``lopq_searcher``
    - ``skipfailed``
    """
    V = self.get_required_param('lopq_V')
    M = self.get_required_param('lopq_M')
    subq = self.get_required_param('lopq_subq')
    # we could use that for a more fine grained model naming...
    self.model_params = {'V': V, 'M': M, 'subq': subq}
    if self.model_type == "lopq_pca":
      # Number of dimensions to keep after PCA
      self.model_params['pca'] = self.get_required_param('lopq_pcadims')
      self.nb_train_pca = self.get_required_param('nb_train_pca')
      nb_min_train_pca = self.get_param('nb_min_train_pca')
      if nb_min_train_pca:
        self.nb_min_train_pca = nb_min_train_pca
    self.lopq_searcher = self.get_param('lopq_searcher', default="LOPQSearcherLMDB")
    self.skipfailed = self.get_param('skipfailed', default=False)

  def build_pca_model_str(self):
    """Build PCA model string

    :return: PCA model string
    :rtype: str
    """
    # Use feature type, self.nb_train_pca and pca_dims
    if self.pca_model_str is None:
      # We could add some additional info: model parameters, number of samples used for training...
      self.pca_model_str = self.build_extr_str() + "_pca" + str(self.model_params['pca'])
      self.pca_model_str += "_train" + str(self.nb_train_pca)
    return self.pca_model_str

  def init_searcher(self):
    """ Initialize LOPQ model and searcher from configuration values
    """
    try:
      # Try to load pretrained model from storer
      # This can fail with error "exceptions.MemoryError" ?
      lopq_model = self.storer.load(self.build_model_str())
      if lopq_model is None:
        raise ValueError("Could not load model from storer.")
      # if self.verbose > 1:
      #   print("pca_mu.shape: {}".format(lopq_model.pca_mu.shape))
      #   print("pca_P.shape: {}".format(lopq_model.pca_P.shape))
    except Exception as inst:
      if type(inst) != ValueError:
        full_trace_error(inst)
      print("[{}: log] Looks like model was not trained yet ({})".format(self.pp, inst))

      self.loaded_pretrain_model = False
      # Try to get it from public bucket e.g.:
      # https://s3-us-west-2.amazonaws.com/dig-cu-imagesearchindex/sbpycaffe_feat_full_image_lopq_pca-pca256-subq256-M8-V256_train100000
      if self.get_pretrained_model:
        log_msg = "[{}: log] Trying to retrieve pre-trained model {} from s3"
        print(log_msg.format(self.pp, self.build_model_str()))
        from ..common.dl import download_file
        import pickle
        try:
          # TODO: fallback bucket_name could be loaded dynamically from conf file...
          base_model_path = "https://s3-us-west-2.amazonaws.com/dig-cu-imagesearchindex/"
          # This can fail with a "retrieval incomplete: got only" ...
          # Or can stall... why?
          download_file(base_model_path + self.build_model_str(), self.build_model_str())
          lopq_model = pickle.load(open(self.build_model_str(), 'rb'))
          # Avoid overwritting the model in s3 with s3storer using dig-cu-imagesearchindex bucket
          is_s3_storer = isinstance(self.storer, S3Storer)
          if is_s3_storer and self.storer.bucket_name == "dig-cu-imagesearchindex":
            log_msg = "[{}: log] Skipping saving model {} back to s3"
            print(log_msg.format(self.pp, self.build_model_str()))
          else:
            log_msg = "[{}: log] Saving model {} to storer"
            print(log_msg.format(self.pp, self.build_model_str()))
            self.storer.save(self.build_model_str(), lopq_model)
          log_msg = "[{}: log] Loaded pretrained model {} from s3"
          print(log_msg.format(self.pp, self.build_model_str()))
          self.loaded_pretrain_model = True
        except Exception as inst:
          log_msg = "[{}: log] Could not loaded pretrained model {} from s3: {}"
          #print(log_msg.format(self.pp, self.build_model_str(), inst))
          full_trace_error(log_msg.format(self.pp, self.build_model_str(), inst))
          sys.stdout.flush()
      else:
        log_msg = "[{}: log] Skipped retrieving pre-trained model from s3 as requested."
        print(log_msg.format(self.pp, self.build_model_str()))

      if not self.loaded_pretrain_model:
        # This is from our modified LOPQ package...
        # https://github.com/ColumbiaDVMM/ColumbiaImageSearch/tree/master/workflows/build-lopq-index/lopq/python
        # 'LOPQModelPCA' could be the type of the model loaded from pickle file
        # from lopq.model import LOPQModel, LOPQModelPCA
        # Size of DB should depend on nb_train... How should we properly set the size of this?
        # It should be nb_train_pca * size_feat + nb_train * size_feat_pca
        feat_size = get_feat_size(self.featurizer_type)
        if self.model_type == "lopq_pca":
          map_size = self.nb_train_pca * feat_size * 4 * 8
          map_size += self.nb_train * self.model_params['pca'] * 4 * 8
        else:
          map_size = self.nb_train * feat_size * 4 * 8
        self.save_feat_env = lmdb.open('/data/lmdb_feats_' + self.build_model_str(),
                                       map_size=int(1.1 * map_size),
                                       writemap=True, map_async=True, max_dbs=2)

        # Train and save model in save_path folder
        lopq_model = self.train_index()
        # TODO: we could build a more unique model identifier
        # (using domain information? sha1/md5 of model parameters? using date of training?)
        # that would also mean we should list from the storer and guess
        # (based on date of creation) the correct model above...
        self.storer.save(self.build_model_str(), lopq_model)

    # Setup searcher with LOPQ model
    if lopq_model:
      # LOPQSearcherLMDB is now the default, as it makes the index more persistent
      # and potentially more easily usable with multiple processes.
      if self.lopq_searcher == "LOPQSearcherLMDB":
        from lopq.search import LOPQSearcherLMDB
        # TODO: should we get path from a parameter? and/or add model_str to it?
        # path are inside the docker container only...
        self.searcher = LOPQSearcherLMDB(lopq_model,
                                         lmdb_path='/data/lmdb_index_' + self.build_model_str(),
                                         id_lambda=str)
        # How could we properly set the size of this?
        up_map_size = 1024 * 1000000 * 1
        # Again (see lopq.search LOPQSearcherLMDB), should we use writemap=True or not
        self.updates_env = lmdb.open('/data/lmdb_updates_' + self.build_model_str(),
                                     map_size=up_map_size, writemap=True, map_async=True, max_dbs=1)
        self.updates_index_db = self.updates_env.open_db("updates")
      elif self.lopq_searcher == "LOPQSearcher":
        from lopq.search import LOPQSearcher
        self.searcher = LOPQSearcher(lopq_model)
      else:
        raise ValueError("Unknown 'lopq_searcher' type: {}".format(self.lopq_searcher))
    # NB: an empty lopq_model would make sense only if we just want to detect...

  def get_feats_from_lmbd(self, feats_db, nb_features, dtype):
    """Get features from LMBD database

    :param feats_db: features database
    :type feats_db: str
    :param nb_features: number fo features
    :type nb_features: int
    :param dtype: numpy type
    :type dtype: :class:`numpy.dtype`
    :return: features
    :rtype: :class:`numpy.ndarray`
    """
    nb_saved_feats = self.get_nb_saved_feats(feats_db)
    nb_feats_to_read = min(nb_saved_feats, nb_features)
    feats = None
    if nb_feats_to_read > 0:
      with self.save_feat_env.begin(db=feats_db, write=False) as txn:
        with txn.cursor() as cursor:
          if cursor.first():
            first_item = cursor.item()
            first_feat = np.frombuffer(first_item[1], dtype=dtype)
            feats = np.zeros((nb_feats_to_read, first_feat.shape[0]))
            print("[get_feats_from_lmbd] Filling up features matrix: {}".format(feats.shape))
            sys.stdout.flush()
            for i, item in enumerate(cursor.iternext()):
              if i >= nb_feats_to_read:
                break
              feats[i, :] = np.frombuffer(item[1], dtype=dtype)
    return feats

  def save_feats_to_lmbd(self, feats_db, samples_ids, np_features, max_feats=0):
    """Save features to LMDB database

    :param feats_db: features database name
    :type feats_db: str
    :param samples_ids: samples ids
    :type samples_ids: list
    :param np_features: features
    :type np_features: :class:`numpy.ndarray`
    :param max_feats: maximum number of features to store in database
    :type max_feats: int
    :return: total number of features in database
    :rtype: int
    """
    with self.save_feat_env.begin(db=feats_db, write=True) as txn:
      for i, sid in enumerate(samples_ids):
        txn.put(bytes(sid), np_features[i, :].tobytes())
        nb_feats = txn.stat()['entries']
        if max_feats and nb_feats >= max_feats:
          return nb_feats
    return nb_feats

  def get_nb_saved_feats(self, feats_db):
    """Get number of features in LMBD database ``feats_db``

    :param feats_db: features database name
    :type feats_db: str
    :return: number of features
    :rtype: int
    """
    with self.save_feat_env.begin(db=feats_db, write=False) as txn:
      return txn.stat()['entries']

  def get_train_features(self, nb_features, lopq_pca_model=None, nb_min_train=None):
    """Get training features

    :param nb_features: number of features
    :type nb_features: int
    :param lopq_pca_model: whether model is lopq_pca_model
    :type lopq_pca_model: bool
    :param nb_min_train: minimum
    :type nb_min_train: int
    :return: features
    :rtype: :class:`numpy.ndarray`
    """
    if nb_min_train is None:
      nb_min_train = nb_features
    if lopq_pca_model:
      feats_db = self.save_feat_env.open_db("feats_pca")
      dtype = np.float32
    else:
      feats_db = self.save_feat_env.open_db("feats")
      from ..featurizer.featsio import get_feat_dtype
      dtype = get_feat_dtype(self.featurizer_type)
    nb_saved_feats = self.get_nb_saved_feats(feats_db)
    nb_features_to_read = nb_features

    seen_updates = set()

    if nb_saved_feats < nb_features:
      print("[{}: log] Gathering {} training samples...".format(self.pp, nb_features))
      sys.stdout.flush()
      start_date = "1970-01-01"
      done = False
      # Accumulate until we have enough features, or we have read all features if 'wait_for_nbtrain'
      # is false and we have at least nb_min_train
      while not done:
        for batch_updates in self.indexer.get_updates_from_date(start_date=start_date,
                                                                extr_type=self.build_extr_str()):
          # for updates in batch_updates:
          #  for update in updates:

          for update in batch_updates:
            try:
              # We could check if update has been processed, but if not we won't get features anyway
              update_id = update[0]
              #if column_list_sha1s in update[1]:
              if self.indexer.get_col_listsha1s() in update[1]:
                if update_id not in seen_updates:
                  sha1s = update[1][self.indexer.get_col_listsha1s()]
                  sids, features = self.indexer.get_features_from_sha1s(sha1s.split(','),
                                                                        self.build_extr_str())
                  if features:
                    # Apply PCA to features here to save memory
                    if lopq_pca_model:
                      np_features = lopq_pca_model.apply_PCA(np.asarray(features))
                    else:
                      np_features = np.asarray(features)
                    log_msg = "[{}: log] Got features {} from update {}"
                    print(log_msg.format(self.pp, np_features.shape, update_id))
                    sys.stdout.flush()
                    # just appending like this does not account for duplicates...
                    # train_features.extend(np_features)
                    nb_saved_feats = self.save_feats_to_lmbd(feats_db, sids, np_features)
                    seen_updates.add(update_id)
                  else:
                    if self.verbose > 3:
                      log_msg = "[{}: log] Did not get features from update {}"
                      print(log_msg.format(self.pp, update_id))
                      sys.stdout.flush()
                  if nb_saved_feats >= nb_features:
                    done = True
                    break
              else:
                warn_msg = "[{}: warning] Update {} has no list of images associated to it."
                print(warn_msg.format(self.pp, update_id))
                sys.stdout.flush()
            except Exception as inst:
              from cufacesearch.common.error import full_trace_error
              err_msg = "[{}: error] Failed to get features: {} {}"
              full_trace_error(err_msg.format(self.pp, type(inst), inst))
              sys.stdout.flush()
            else:
              if self.verbose > 4:
                print("[{}: log] Got {} training samples so far...".format(self.pp, nb_saved_feats))
                sys.stdout.flush()
            if done:
              nb_features_to_read = nb_saved_feats
              break
        else:
          if not done:
            # Wait for new updates...
            # TODO: could be optional
            if self.wait_for_nbtrain:
              if nb_saved_feats >= nb_min_train:
                log_msg = "[{}: log] Gathered minimum number of training features ({})..."
                print(log_msg.format(self.pp, nb_min_train))
                sys.stdout.flush()
                break
              else:
                log_msg = "[{}: log] Waiting for new updates. Got {} training samples so far..."
                print(log_msg.format(self.pp, nb_saved_feats))
                sys.stdout.flush()
                time.sleep(60)
            else:
              log_msg = "[{}: log] Gathered all available features ({})..."
              print(log_msg.format(self.pp, self.get_nb_saved_feats(feats_db)))
              sys.stdout.flush()
              break

    return self.get_feats_from_lmbd(feats_db, nb_features_to_read, dtype)

  def train_index(self):
    """Train search index

    :return: search index
    :rtype: LOPQModel, LOPQModelPCA
    """

    if self.model_type == "lopq":
      train_np = self.get_train_features(self.nb_train, nb_min_train=self.nb_min_train)
      print("Got train features array with shape: {}".format(train_np.shape))
      nb_train_feats = train_np.shape[0]
      sys.stdout.flush()

      if nb_train_feats >= self.nb_train:
        from lopq.model import LOPQModel
        # we could have default values for those parameters and/or heuristic to estimate them based on data count...
        lopq_model = LOPQModel(V=self.model_params['V'], M=self.model_params['M'],
                               subquantizer_clusters=self.model_params['subq'])
        # we could have separate training/indexing features
        msg = "[{}.train_model: info] Starting local training of 'lopq' model with parameters {} using {} features."
        print(msg.format(self.pp, self.model_params, nb_train_feats))
        start_train = time.time()
        # specify a n_init < 10 (default value) to speed-up training?
        lopq_model.fit(train_np, verbose=True)
        # save model
        self.storer.save(self.build_model_str(), lopq_model)
        msg = "[{}.train_model: info] Trained lopq model in {}s."
        print(msg.format(self.pp, time.time() - start_train))
        return lopq_model
      else:
        msg = "[{}.train_model: error] Could not train model, not enough training samples."
        print(msg.format(self.pp))

    elif self.model_type == "lopq_pca":
      # lopq_pca training.
      from lopq.model import LOPQModelPCA
      # we could have default values for those parameters
      # and/or heuristic to estimate them based on data count...
      lopq_model = LOPQModelPCA(V=self.model_params['V'], M=self.model_params['M'],
                                subquantizer_clusters=self.model_params['subq'], renorm=True)
      # pca loading/training first
      pca_model = self.storer.load(self.build_pca_model_str())
      if pca_model is None:
        train_np = self.get_train_features(self.nb_train_pca, nb_min_train=self.nb_min_train_pca)
        msg = "[{}.train_model: info] Training PCA model, keeping {} dimensions from features {}."
        print(msg.format(self.pp, self.model_params['pca'], train_np.shape))
        sys.stdout.flush()
        start_train_pca = time.time()
        lopq_model.fit_pca(train_np, pca_dims=self.model_params['pca'])
        info_msg = "[{}.train_model: info] Trained pca model in {}s."
        print(info_msg.format(self.pp, time.time() - start_train_pca))
        del train_np
        self.storer.save(self.build_pca_model_str(),
                         {"P": lopq_model.pca_P, "mu": lopq_model.pca_mu})
      else:
        lopq_model.pca_P = pca_model["P"]
        lopq_model.pca_mu = pca_model["mu"]
      # train model
      train_np = self.get_train_features(self.nb_train, lopq_pca_model=lopq_model,
                                         nb_min_train=self.nb_min_train)
      msg = "[{}.train_model: info] Training 'lopq_pca' model with parameters {} using features {}"
      print(msg.format(self.pp, self.model_params, train_np.shape))
      sys.stdout.flush()
      start_train = time.time()
      # specify a n_init < 10 (default value) to speed-up training?
      lopq_model.fit(train_np, verbose=True, apply_pca=False, train_pca=False)
      # TODO: we could evaluate model based on reconstruction of some randomly sampled features?
      # save model
      self.storer.save(self.build_model_str(), lopq_model)
      info_msg = "[{}.train_model: info] Trained lopq model in {}s."
      print(info_msg.format(self.pp, time.time() - start_train))
      sys.stdout.flush()
      return lopq_model
      # err_msg = "[{}.train_model: error] Local training of 'lopq_pca' model not yet implemented."
      # raise NotImplementedError(err_msg.format(self.pp))
    else:
      err_msg = "[{}.train_model: error] Unknown 'lopq' type {}."
      raise ValueError(err_msg.format(self.pp, self.model_type))
    # print train_features_path, os.path.exists(train_features_path), lopq_params

  # TODO: should we try to evaluate index by pushing train_features to a temporary searcher
  #    - compute exhaustive search for some randomly selected samples
  #    - analyze retrieval performance of approximate search?
  # technically we could even explore different configurations...

  def compute_codes(self, det_ids, data, codes_path=None):
    """Compute codes for features in ``data`` corresponding to samples ``det_ids``

    :param det_ids: samples ids
    :type det_ids: list
    :param data: features
    :type data: list(:class:`numpy.ndarray`)
    :param codes_path: path to use to save codes using storer
    :type codes_path: str
    :return: codes dictionary
    :rtype: dict
    """
    # Compute codes for each update batch and save them
    from lopq.utils import compute_codes_parallel
    msg = "[{}.compute_codes: log] Computing codes for {} ({} unique) {}s from {} features"
    print(msg.format(self.pp, len(det_ids), len(set(det_ids)), self.input_type, len(data)))

    # That keeps the ordering intact, but output is a chain
    codes = compute_codes_parallel(data, self.searcher.model, self.num_procs)

    # Build dict output
    codes_dict = dict()
    count_codes = 0
    for i, code in enumerate(codes):
      count_codes += 1
      codes_dict[det_ids[i]] = [code.coarse, code.fine]

    # Save
    if codes_path:
      # # Some old updates have duplicate sha1s...
      # if self.verbose > 1 and len(codes_dict) < len(det_ids):
      #   msg = "[{}.compute_codes: log] Saving only {} of {} codes. det_ids: {}"
      #   print(msg.format(self.pp, len(codes_dict), count_codes, det_ids))
      self.storer.save(codes_path, codes_dict)

    return codes_dict

  def add_update(self, update_id, date_db=None):
    """Add update id ``update_id`` to the database or list of update ids

    :param update_id: update id
    :type update_id: str
    :param date_db: datetime to save for that update
    :type date_db: datetime
    """
    if date_db is None:
      date_db = datetime.now()
    else:
      if self.verbose > 4:
        msg = "[{}.add_update: log] Saving update {} with date {}"
        print(msg.format(self.pp, update_id, date_db))
    if self.lopq_searcher == "LOPQSearcherLMDB":
      # Use another LMDB to store updates indexed
      with self.updates_env.begin(db=self.updates_index_db, write=True) as txn:
        txn.put(bytes(update_id), bytes(date_db))
    else:
      self.indexed_updates.add(update_id)
    if self.last_indexed_update is None or update_id > self.last_indexed_update:
      self.last_indexed_update = update_id

  def get_update_date_db(self, update_id):
    """Get update id ``update_id`` saved date in database

    :param update_id: update id
    :type update_id: str
    :raises TypeError: if self.lopq_searcher is not "LOPQSearcherLMDB"
    :raises ValueError: if ``update_id`` is not in database
    """
    if self.lopq_searcher == "LOPQSearcherLMDB":
      with self.updates_env.begin(db=self.updates_index_db, write=False) as txn:
        found_update = txn.get(bytes(update_id))
        if found_update:
          # parse found_update as string
          if self.verbose > 4:
            msg = "[{}.get_update_date_db: log] update {} date_db in database is: {}"
            print(msg.format(self.pp, update_id, found_update))
          return datetime.strptime(str(bytes(found_update)), DATE_DB_FORMAT)
        else:
          msg = "[{}.get_update_date_db: error] update {} is not in database"
          raise ValueError(msg.format(self.pp, update_id))
    else:
      msg = "[{}.get_update_date_db: error] lopq_searcher is not of type \"LOPQSearcherLMDB\""
      raise TypeError(msg)

  def skip_update(self, update_id, dtn):
    """Check if we should skip loading update ``update_id`` because it has been marked as fully
    processed and indexed already (using a date in the future)

    :param update_id: update id
    :type update_id: str
    :param dtn: datetime of now
    :type dtn: :class:`datetime.datetime`
    :return: whether this update should be skipped
    :rtype: bool
    """
    try:
      read_date_db = self.get_update_date_db(update_id)
      if self.verbose > 5:
        msg = "[{}: log] Check whether to skip update {} (date_db: {}, dtn: {})"
        print(msg.format(self.pp, update_id, read_date_db, dtn))
      if read_date_db.year > dtn.year:
        if self.verbose > 4:
          msg = "[{}: log] Skipping update {} marked with a future date: {}."
          print(msg.format(self.pp, update_id, read_date_db))
        return True
      return False
    except Exception as inst:
      if self.verbose > 1:
        print(inst)
      return False

  def is_update_indexed(self, update_id):
    """Check whether update ``update_id`` has already been indexed

    :param update_id: update id
    :type update_id: str
    :return: True (if indexed), False (if not)
    :rtype: bool
    """
    if self.lopq_searcher == "LOPQSearcherLMDB":
      with self.updates_env.begin(db=self.updates_index_db, write=False) as txn:
        found_update = txn.get(bytes(update_id))
        if found_update:
          return True
        else:
          return False
    else:
      return update_id in self.indexed_updates

  def is_update_processed(self, update_cols):
    """Check whether update columns ``update_cols`` contain the flag indicating that the update
    has been processed

    :param update_cols: update columns dictionary
    :type update_cols: dict
    :return: True (if processed), False (if not)
    :rtype: bool
    """
    if self.indexer.get_col_upproc() in update_cols:
      return True
    return False

  def get_latest_update_suffix(self):
    """Get latest update suffix

    :return: latest update suffix
    :rtype: str
    """
    if self.last_indexed_update is None:
      if self.lopq_searcher == "LOPQSearcherLMDB":
        # Try to get in from DB
        with self.updates_env.begin(db=self.updates_index_db, write=False) as txn:
          with txn.cursor() as cursor:
            if cursor.last():
              item = cursor.item()
              self.last_indexed_update = item[0]
              suffix = '_'.join(self.last_indexed_update.split('_')[6:])
            else:  # Would happen on empty db?
              suffix = "1970-01-01"
      else:
        suffix = "1970-01-01"
    else:
      suffix = '_'.join(self.last_indexed_update.split('_')[6:])
    return suffix

  def load_codes(self, full_refresh=False):
    """Load codes

    :param full_refresh: wheter to perform a full refresh or not
    :type full_refresh: bool
    """
    # Calling this method can also perfom an update of the index
    if not self.searcher:
      info_msg = "[{}.load_codes: info] Not loading codes as searcher is not initialized."
      print(info_msg.format(self.pp))
      return

    start_load = time.time()
    total_compute_time = 0

    try:
      # if self.searcher.nb_indexed == 0:
      #   # We should try to load a concatenation of all unique codes that also contains a list of the corresponding updates...
      #   # fill codes and self.indexed_updates
      #   self.load_all_codes()
      # TODO: try to get date of last update
      start_date = "1970-01-01"
      if not full_refresh:
        start_date = self.get_latest_update_suffix()
      extr_str = self.build_extr_str()
      feat_size = get_feat_size(self.featurizer_type)
      feat_type = get_feat_dtype(self.featurizer_type)

      # Get all updates ids for the extraction type
      # TODO: this scan makes the API unresponsive for ~2 minutes during the update process...
      for batch_updates in self.indexer.get_updates_from_date(start_date=start_date,
                                                              extr_type=extr_str):
        for update in batch_updates:
          update_id = update[0]
          if self.is_update_indexed(update_id) and not full_refresh:
            if self.verbose > 4:
              print("[{}: log] Skipping update {} already indexed.".format(self.pp, update_id))
              continue
          else:
            dtn = datetime.now()
            if self.is_update_processed(update[1]) and not self.skip_update(update_id, dtn):
              print("[{}: log] Looking for codes of update {}".format(self.pp, update_id))
              # Get this update codes
              codes_string = self.build_codes_string(update_id)
              try:
                # Check for precomputed codes
                codes_dict = self.storer.load(codes_string, silent=True)
                if codes_dict is None:
                  msg = "[{}: log] Could not load codes from {}"
                  raise ValueError(msg.format(self.pp, codes_string))
                # If full_refresh, check that we have as many codes as available features
                if full_refresh:
                  if self.indexer.get_col_listsha1s() in update[1]:
                    set_sha1s = set(update[1][self.indexer.get_col_listsha1s()].split(','))
                    sids, _ = self.indexer.get_features_from_sha1s(list(set_sha1s), extr_str)
                    if len(set(sids)) > len(codes_dict):
                      msg = "[{}: log] Update {} has {} new features"
                      diff_count = len(set(sids)) - len(codes_dict)
                      raise ValueError(msg.format(self.pp, update_id, diff_count))
                    else:
                      msg = "[{}: log] Skipping update {} indexed with all {}/{} features"
                      print(msg.format(self.pp, update_id, len(codes_dict), len(set(sids))))
                      miss_extr = self.indexer.get_missing_extr_sha1s(list(set_sha1s), extr_str,
                                                                      skip_failed=self.skipfailed)
                      # If all sha1s have been processed, no need to ever check that update again
                      # Store that information as future date_db to avoid ever checking again...
                      if not miss_extr and self.lopq_searcher == "LOPQSearcherLMDB":
                        dtn = dtn.replace(year=9999)
              except Exception as inst:
                # Update codes not available
                if self.verbose > 3:
                  print(inst)
                # Compute codes for update not yet processed and save them
                start_compute = time.time()
                # Get detections (if any) and features
                if self.indexer.get_col_listsha1s() in update[1]:
                  list_sha1s = list(set(update[1][self.indexer.get_col_listsha1s()].split(',')))
                  sids, fts = self.indexer.get_features_from_sha1s(list_sha1s, extr_str, feat_type)
                  if fts:
                    if fts[0].shape[-1] != feat_size:
                      msg = "[{}.load_codes: error] Invalid feature size {} vs {} expected"
                      raise ValueError(msg.format(fts[0].shape[-1], feat_size))
                    codes_dict = self.compute_codes(sids, fts, codes_string)
                    update_compute_time = time.time() - start_compute
                    total_compute_time += update_compute_time
                    if self.verbose > 0:
                      log_msg = "[{}: log] Update {} codes computation done in {}s"
                      print(log_msg.format(self.pp, update_id, update_compute_time))
                  else:
                    print("[{}: warning] Update {} has no features.".format(self.pp, update_id))
                    continue
                else:
                  print("[{}: warning] Update {} has no list of images.".format(self.pp, update_id))
                  continue

              # Use new method add_codes_from_dict of searcher
              self.searcher.add_codes_from_dict(codes_dict)
              self.add_update(update_id, date_db=dtn)

      total_load = time.time() - start_load
      self.last_refresh = datetime.now()

      print("[{}: log] Total udpates computation time is: {}s".format(self.pp, total_compute_time))
      print("[{}: log] Total udpates loading time is: {}s".format(self.pp, total_load))

    except Exception as inst:
      full_trace_error("[{}: error] Could not load codes. {}".format(self.pp, inst))
      #load_codesprint("[{}: error] Could not load codes. {}".format(self.pp, inst))

  # def load_all_codes(self):
  #   # load self.indexed_updates, self.searcher.index and self.searcher.nb_indexed
  #   # NOT for LOPQSearcherLMDB
  #   pass
  #
  # def save_all_codes(self):
  #   # we should save self.indexed_updates, self.searcher.index and self.searcher.nb_indexed
  #   # self.searcher.index could be big, how to save without memory issue...
  #   # NOT for LOPQSearcherLMDB
  #   pass

  def search_from_feats(self, dets, feats, options_dict=dict()):
    """Search the index using features ``feats`` of samples ``dets``

    :param dets: list of query samples, images or list of detections in each image
    :type dets: list
    :param feats: list of features
    :type feats: list
    :param options_dict: options dictionary
    :type options_dict: dict
    :return: formatted output
    :rtype: collections.OrderedDict
    """
    # NB: dets is a list of list
    import time
    start_search = time.time()
    all_sim_images = []
    all_sim_dets = []
    all_sim_score = []

    # check what is the near duplicate config
    filter_near_dup = False
    if (self.near_dup and "near_dup" not in options_dict) or (
            "near_dup" in options_dict and options_dict["near_dup"]):
      filter_near_dup = True
      if "near_dup_th" in options_dict:
        near_dup_th = options_dict["near_dup_th"]
      else:
        near_dup_th = self.near_dup_th

    max_returned = self.sim_limit
    if "max_returned" in options_dict:
      max_returned = options_dict["max_returned"]
    # this should be set with a parameter either in conf or options_dict too.
    # should we use self.quota here? and potentially overwrite from options_dict
    quota = min(1000 * max_returned, 10000)

    # print dets
    if self.detector is not None:
      # query for each feature
      for i in range(len(dets)):

        sim_images = []
        sim_dets = []
        sim_score = []

        for j in range(len(dets[i][1])):
          results = []
          if "detect_only" not in options_dict or not options_dict["detect_only"]:
            if self.searcher:
              # Normalize feature first as it is how it is done during extraction...
              norm_feat = np.linalg.norm(feats[i][j])
              normed_feat = np.squeeze(feats[i][j] / norm_feat)
              results, visited = self.searcher.search(normed_feat, quota=quota, limit=max_returned,
                                                      with_dists=True)
              res_msg = "[{}.search_from_feats: log] got {} results by visiting {} cells, first one is: {}"
              print(res_msg.format(self.pp, len(results), visited, results[0]))

          # If reranking, get features from hbase for detections using res.id
          #   we could also already get 's3_url' to avoid a second call to HBase later...
          if self.reranking:
            try:
              res_list_sha1s = [str(x.id).split('_')[0] for x in results]
              res_samples_ids, res_features = self.indexer.get_features_from_sha1s(res_list_sha1s,
                                                                                   self.build_extr_str())
              # FIXME: dirty fix for dlib features size issue.
              # To be removed once workflow applied on all legacy data
              if res_features is not None and res_features[0].shape[-1] < 128:
                res_samples_ids, res_features = self.indexer.get_features_from_sha1s(res_list_sha1s,
                                                                                     self.build_extr_str(),
                                                                                     "float32")
                if res_features:
                  forced_msg = "Forced decoding of features as float32. Got {} samples, features with shape {}"
                  print(forced_msg.format(len(res_samples_ids), res_features[0].shape))
            except Exception as inst:
              err_msg = "[{}: error] Could not retrieve features for re-ranking. {}"
              print(err_msg.format(self.pp, inst))

          tmp_img_sim = []
          tmp_dets_sim_ids = []
          tmp_dets_sim_score = []
          for ires, res in enumerate(results):
            dist = res.dist
            # if reranking compute actual distance
            if self.reranking:
              try:
                pos = res_samples_ids.index(res.id)
                dist = np.linalg.norm(normed_feat - res_features[pos])
                # print "[{}: res_features[{}] approx. dist: {}, rerank dist: {}".format(res.id, pos, res.dist, dist)
              except Exception as inst:
                # Means feature was not saved to backend index...
                err_msg = "Could not compute reranking distance for sample {}, error {} {}"
                print(err_msg.format(res.id, type(inst), inst))
            if (filter_near_dup and dist <= near_dup_th) or not filter_near_dup:
              if not max_returned or (max_returned and ires < max_returned):
                tmp_dets_sim_ids.append(res.id)
                # here id would be face_id that we could build as sha1_facebbox?
                tmp_img_sim.append(str(res.id).split('_')[0])
                tmp_dets_sim_score.append(dist)

          # If reranking, we need to reorder
          if self.reranking:
            sids = np.argsort(tmp_dets_sim_score, axis=0)
            rerank_img_sim = []
            rerank_dets_sim_ids = []
            rerank_dets_sim_score = []
            for si in sids:
              rerank_img_sim.append(tmp_img_sim[si])
              rerank_dets_sim_ids.append(tmp_dets_sim_ids[si])
              rerank_dets_sim_score.append(tmp_dets_sim_score[si])
            tmp_img_sim = rerank_img_sim
            tmp_dets_sim_ids = rerank_dets_sim_ids
            tmp_dets_sim_score = rerank_dets_sim_score

          # print tmp_img_sim
          if tmp_img_sim:
            rows = []
            try:
              rows = self.indexer.get_columns_from_sha1_rows(tmp_img_sim, self.needed_output_columns)
            except Exception as inst:
              err_msg = "[{}: error] Could not retrieve similar images info from indexer. {}"
              print(err_msg.format(self.pp, inst))
            # rows should contain id, s3_url of images
            # print rows
            if not rows:
              sim_images.append([(x,) for x in tmp_img_sim])
            elif len(rows) < len(tmp_img_sim) or not rows:
              # fall back to just sha1s... but beware to keep order...
              dec = 0
              fixed_rows = []
              for pos, sha1 in tmp_img_sim:
                if rows[pos - dec][0] == sha1:
                  fixed_rows.append(rows[pos - dec])
                else:
                  dec += 1
                  fixed_rows.append((sha1,))
              sim_images.append(fixed_rows)
            else:
              sim_images.append(rows)
            sim_dets.append(tmp_dets_sim_ids)
            sim_score.append(tmp_dets_sim_score)
          else:
            sim_images.append([])
            sim_dets.append([])
            sim_score.append([])

        all_sim_images.append(sim_images)
        all_sim_dets.append(sim_dets)
        all_sim_score.append(sim_score)
    else:
      # No detection
      results = []
      sim_images = []
      sim_score = []

      for i in range(len(feats)):
        if self.searcher:
          # Normalize feature first as it is how it is done during extraction...
          norm_feat = np.linalg.norm(feats[i])
          normed_feat = np.squeeze(feats[i] / norm_feat)
          results, visited = self.searcher.search(normed_feat, quota=quota, limit=max_returned,
                                                  with_dists=True)
          res_msg = "[{}.search_from_feats: log] got {} results by visiting {} cells, first one is: {}"
          print(res_msg.format(self.pp, len(results), visited, results[0]))

        # Reranking, get features from hbase for detections using res.id
        if self.reranking:
          try:
            res_list_sha1s = [str(x.id) for x in results]
            res_samples_ids, res_features = self.indexer.get_features_from_sha1s(res_list_sha1s,
                                                                               self.build_extr_str())
          except Exception as inst:
            err_msg = "[{}: error] Could not retrieve features for re-ranking. {}"
            print(err_msg.format(self.pp, inst))

        tmp_img_sim = []
        tmp_sim_score = []
        for ires, res in enumerate(results):
          dist = res.dist
          if self.reranking:
            # If reranking compute actual distance
            try:
              pos = res_samples_ids.index(res.id)
              dist = np.linalg.norm(normed_feat - res_features[pos])
              # print "[{}: res_features[{}] approx. dist: {}, rerank dist: {}".format(res.id, pos, res.dist, dist)
            except Exception as inst:
              err_msg = "Could not compute reranked distance for sample {}, error {} {}"
              print(err_msg.format(res.id, type(inst), inst))
          if (filter_near_dup and dist <= near_dup_th) or not filter_near_dup:
            if not max_returned or (max_returned and ires < max_returned):
              tmp_img_sim.append(str(res.id))
              tmp_sim_score.append(dist)

        # If reranking, we need to reorder
        if self.reranking:
          sids = np.argsort(tmp_sim_score, axis=0)
          rerank_img_sim = []
          rerank_sim_score = []
          for si in sids:
            rerank_img_sim.append(tmp_img_sim[si])
            rerank_sim_score.append(tmp_sim_score[si])
          tmp_img_sim = rerank_img_sim
          tmp_sim_score = rerank_sim_score

        if tmp_img_sim:
          rows = []
          try:
            rows = self.indexer.get_columns_from_sha1_rows(tmp_img_sim, self.needed_output_columns)
          except Exception as inst:
            err_msg = "[{}: error] Could not retrieve similar images info from indexer. {}"
            print(err_msg.format(self.pp, inst))
          # rows should contain id, s3_url of images
          # print rows
          sim_images.append(rows)
          sim_score.append(tmp_sim_score)
        else:
          sim_images.append([])
          sim_score.append([])

      all_sim_images.append(sim_images)
      all_sim_dets.append([])
      all_sim_score.append(sim_score)

    search_time = time.time() - start_search
    print("[{}: log] Search performed in {:0.3}s.".format(self.pp, search_time))

    # format output
    # print "all_sim_images",all_sim_images
    # print "all_sim_dets",all_sim_dets
    # print "all_sim_score",all_sim_score
    return self.do.format_output(dets, all_sim_images, all_sim_dets, all_sim_score, options_dict,
                                 self.input_type)
