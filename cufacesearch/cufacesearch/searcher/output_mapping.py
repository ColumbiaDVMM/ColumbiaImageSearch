class DictOutput():

  def __init__(self, mode='CamelCase'):
    self.map = dict()
    if mode == 'CamelCase':
        self.fillDictCamelCase()
    else:
        self.fillDictOld()

  def fillDictCamelCase(self):
    self.map['query_sha1'] = "QuerySha1"
    self.map['query_url'] = "QueryURL"
    self.map['query_face'] = "QueryFace"
    self.map['image_sha1s'] = "ImageSha1s"
    self.map['similar_faces'] = "SimilarFaces"
    self.map['faces'] = "Faces"
    self.map['img_info'] = "ImgInfo"
    self.map['distances'] = "Distances"
    self.map['number_images'] = "NumberImages"
    self.map['number_faces'] = "NumberFaces"
    self.map['number_similar_faces'] = "NumberSimilarFaces"
    self.map['all_similar_faces'] = "AllSimilarFaces"
    self.map['cached_image_urls'] = "CachedImageURLs"

  def fillDictOld(self):
    self.map['query_sha1'] = "query_sha1"
    self.map['query_url'] = "query_url"
    self.map['query_face'] = "query_face"
    self.map['image_sha1s'] = "image_sha1s"
    self.map['similar_faces'] = "similar_faces"
    self.map['faces'] = "faces"
    self.map['img_info'] = "img_info"
    self.map['distances'] = "distances"
    self.map['number_images'] = "number_images"
    self.map['number_faces'] = "number_faces"
    self.map['number_similar_faces'] = "number_similar_faces"
    self.map['all_similar_faces'] = "all_similar_faces"
    self.map['cached_image_urls'] = "cached_image_urls"

  def format_output(self, dets, sim_images, sim_faces, sim_score, options_dict=dict()):
    import time
    from collections import OrderedDict
    print "[format_output: log] options are: {}".format(options_dict)
    start_build_output = time.time()
    output = []
    images_query = set()
    nb_faces_query = 0
    nb_faces_similar = 0

    for i in range(len(dets)):

      # No face detected in query
      if not dets[i][1]:
        output.append(dict())
        out_i = len(output) - 1
        output[out_i][self.map['query_sha1']] = dets[i][0]
        if dets[i][2]:
          output[out_i][self.map['query_url']] = dets[i][2]
        output[out_i][self.map['img_info']] = dets[i][3:5]
        images_query.add(dets[i][0])
        output[out_i][self.map['similar_faces']] = OrderedDict([[self.map['number_faces'], 0],
                                                               [self.map['image_sha1s'], []],
                                                               [self.map['faces'], []],
                                                               [self.map['cached_image_urls'], []],
                                                               [self.map['distances'], []]])
        continue

      # We found some faces...
      for j, face_bbox in enumerate(dets[i][1]):
        nb_faces_query += 1

        # Add one output for each face query
        output.append(dict())
        out_i = len(output) - 1
        output[out_i][self.map['query_sha1']] = dets[i][0]
        output[out_i][self.map['query_face']] = dets[i][1][j]
        if dets[i][2]:
          output[out_i][self.map['query_url']] = dets[i][2]
        output[out_i][self.map['img_info']] = dets[i][3:]
        images_query.add(dets[i][0])

        nb_faces = 0
        if sim_faces[i]:
          if sim_faces[i][j]:
            nb_faces = len(sim_faces[i][j])

        output[out_i][self.map['similar_faces']] = OrderedDict([[self.map['number_faces'], nb_faces],
                                                              [self.map['image_sha1s'], []],
                                                              [self.map['faces'], []],
                                                              [self.map['img_info'], []],
                                                              [self.map['cached_image_urls'], []],
                                                              [self.map['distances'], []]])
        # Explore list of similar faces
        for jj in range(nb_faces):
          sim_face = sim_faces[i][j][jj]
          nb_faces_similar += 1
          output[out_i][self.map['similar_faces']][self.map['image_sha1s']].append(sim_images[i][j][jj][0].strip())
          output[out_i][self.map['similar_faces']][self.map['cached_image_urls']].append(sim_images[i][j][jj][1][self.url_field].strip())
          output[out_i][self.map['similar_faces']][self.map['faces']].append('-'.join(sim_face.split('_')[1:]))
          # this is not in HBase for all/most images...
          #osf_imginfo = sim_images[i][j][jj][1][self.img_info_field].strip()
          output[out_i][self.map['similar_faces']][self.map['img_info']].append('')
          output[out_i][self.map['similar_faces']][self.map['distances']].append(sim_score[i][j][jj])

    outp = OrderedDict([[self.map['number_images'], len(images_query)],
                        [self.map['number_faces'], nb_faces_query],
                        [self.map['number_similar_faces'], nb_faces_similar],
                        [self.map['all_similar_faces'], output]])

    print "[format_output: log] build_output took: {}".format(time.time() - start_build_output)
    return outp
