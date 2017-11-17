class DictOutput():

  def __init__(self, mode='CamelCase'):
    self.map = dict()
    self.coord_map = ["left", "top", "right", "bottom"]
    # add your input type if you create one.
    self.input_types = ["image", "face"]

    if mode == 'CamelCase':
        self.fillDictCamelCase()
    else:
        self.fillDictOld()

  def fillInputFieldsCamelCase(self, input, input_str):
    self.map['query_' + input] = "Query" + input_str
    self.map['similar_' + input + 's'] = "Similar" + input_str + "s"
    self.map[input + 's'] = input_str + "s"
    self.map['number_'+input+'s'] = "Number" + input_str + "s"
    self.map['number_similar_'+input+'s'] = "NumberSimilar" + input_str + "s"
    self.map['all_similar_'+input+'s'] = "AllSimilar" + input_str + "s"

  def fillInputFieldsOld(self, input, input_str):
    self.map['query_' + input] = "query_" + input_str
    self.map['similar_' + input + 's'] = "similar_" + input_str + "s"
    self.map[input + 's'] = input_str + "s"
    self.map['number_'+input+'s'] = "number_" + input_str + "s"
    self.map['number_similar_'+input+'s'] = "number_similar_" + input_str + "s"
    self.map['all_similar_'+input+'s'] = "all_similar_" + input_str + "s"

  def fillDictCamelCase(self):
    self.map['query_sha1'] = "QuerySha1"
    self.map['query_url'] = "QueryURL"
    self.map['image_sha1s'] = "ImageSha1s"
    self.map['img_info'] = "ImgInfo"
    self.map['distances'] = "Distances"
    self.map['cached_image_urls'] = "CachedImageURLs"
    for input in self.input_types:
      input_str = input.title()
      self.fillInputFieldsCamelCase(input, input_str)

    # self.map['query_face'] = "QueryFace"
    # self.map['query_image'] = "QueryImage"
    # self.map['similar_faces'] = "SimilarFaces"
    # self.map['similar_images'] = "SimilarImages"
    # self.map['faces'] = "Faces"
    # self.map['images'] = "Images"
    # self.map['number_faces'] = "NumberFaces"
    # self.map['number_images'] = "NumberImages"
    # self.map['number_similar_faces'] = "NumberSimilarFaces"
    # self.map['number_similar_images'] = "NumberSimilarImages"
    # self.map['all_similar_faces'] = "AllSimilarFaces"
    # self.map['all_similar_images'] = "AllSimilarImages"


  def fillDictOld(self):
    self.map['query_sha1'] = "query_sha1"
    self.map['query_url'] = "query_url"
    self.map['image_sha1s'] = "image_sha1s"
    self.map['img_info'] = "img_info"
    self.map['distances'] = "distances"
    self.map['cached_image_urls'] = "cached_image_urls"
    for input in self.input_types:
      self.fillInputFieldsOld(input, input)

    # self.map['query_face'] = "query_face"
    # self.map['query_image'] = "query_image"
    # self.map['similar_faces'] = "similar_faces"
    # self.map['similar_images'] = "similar_images"
    # self.map['faces'] = "faces"
    # self.map['number_images'] = "number_images"
    # self.map['number_faces'] = "number_faces"
    # self.map['number_similar_faces'] = "number_similar_faces"
    # self.map['number_similar_images'] = "number_similar_images"
    # self.map['all_similar_faces'] = "all_similar_faces"
    # self.map['all_similar_images'] = "all_similar_images"


  def format_output(self, dets, sim_images, sim_dets, sim_score, options_dict=dict(), input_type="image"):
    import time
    from collections import OrderedDict
    print "[format_output: log] options are: {}".format(options_dict)
    start_build_output = time.time()
    output = []
    images_query = set()
    nb_faces_query = 0
    nb_faces_similar = 0
    nb_images_similar = 0

    #print 'dets',len(dets),dets
    #print 'sim_dets', len(sim_dets), sim_dets

    if input_type != "image":

      for i in range(len(dets)):

        # No detection in query
        if not dets[i][1]:
          output.append(dict())
          out_i = len(output) - 1
          output[out_i][self.map['query_sha1']] = dets[i][0]
          if dets[i][2]:
            output[out_i][self.map['query_url']] = dets[i][2]
          output[out_i][self.map['img_info']] = dets[i][3:5]
          images_query.add(dets[i][0])
          output[out_i][self.map['similar_'+input_type+'s']] = OrderedDict([[self.map['number_'+input_type+'s'], 0],
                                                                 [self.map['image_sha1s'], []],
                                                                 [self.map[input_type+'s'], []],
                                                                 [self.map['cached_image_urls'], []],
                                                                 [self.map['distances'], []]])
          continue

        # We found some faces...
        #print len(dets[i][1])
        for j, face_bbox in enumerate(dets[i][1]):
          nb_faces_query += 1

          # Add one output for each face query
          output.append(dict())
          out_i = len(output) - 1
          output[out_i][self.map['query_sha1']] = dets[i][0]
          output[out_i][self.map['query_'+input_type]] = dets[i][1][j]
          if dets[i][2]:
            output[out_i][self.map['query_url']] = dets[i][2]
          output[out_i][self.map['img_info']] = dets[i][3:]
          images_query.add(dets[i][0])

          nb_dets = 0
          #print "sim_dets[i]",sim_dets[i]

          if sim_dets[i]:
            if len(sim_dets[i])>j and sim_dets[i][j]:
              #print "sim_dets[i][j]",sim_dets[i][j]
              nb_dets = len(sim_dets[i][j])

          output[out_i][self.map['similar_'+input_type+'s']] = OrderedDict([[self.map['number_'+input_type+'s'], nb_dets],
                                                                [self.map['image_sha1s'], []],
                                                                [self.map[input_type+'s'], []],
                                                                [self.map['img_info'], []],
                                                                [self.map['cached_image_urls'], []],
                                                                [self.map['distances'], []]])

          #print 'nb_faces: %d' % nb_faces

          # Explore list of similar faces
          for jj in range(nb_dets):
            sim_det = sim_dets[i][j][jj]
            nb_faces_similar += 1
            print sim_images[i][j][jj]
            output[out_i][self.map['similar_'+input_type+'s']][self.map['image_sha1s']].append(sim_images[i][j][jj][0].strip())
            if sim_images[i][j][jj] > 1:
              output[out_i][self.map['similar_'+input_type+'s']][self.map['cached_image_urls']].append(sim_images[i][j][jj][1][self.url_field].strip())

            tmp_face_dict = dict()
            for tfi, tfcoord in enumerate(sim_det.split('_')[1:]):
              tmp_face_dict[self.coord_map[tfi]] = int(tfcoord)
            output[out_i][self.map['similar_'+input_type+'s']][self.map[input_type+'s']].append(tmp_face_dict)
            # this is not in HBase for all/most images...
            #osf_imginfo = sim_images[i][j][jj][1][self.img_info_field].strip()
            output[out_i][self.map['similar_'+input_type+'s']][self.map['img_info']].append('')
            output[out_i][self.map['similar_'+input_type+'s']][self.map['distances']].append(sim_score[i][j][jj])

      outp = OrderedDict([[self.map['number_images'], len(images_query)],
                          [self.map['number_'+input_type+'s'], nb_faces_query], # this would overwrite number_images if input_type is image...
                          [self.map['number_similar_'+input_type+'s'], nb_faces_similar],
                          [self.map['all_similar_'+input_type+'s'], output]])

    else:
      # no detections, dets contains list of query images sha1

      for i in range(len(dets)):
        sha1 = dets[i][0]

        # Add one output for each image query
        output.append(dict())
        out_i = len(output) - 1
        output[out_i][self.map['query_sha1']] = sha1
        if dets[i][1]:
          output[out_i][self.map['query_url']] = dets[i][1]

        if sim_images[i][0]:
          nb_images = len(sim_images[i][0])

        output[out_i][self.map['similar_images']] = OrderedDict([[self.map['number_images'], nb_images],
                                                              [self.map['image_sha1s'], []],
                                                              [self.map['cached_image_urls'], []],
                                                              [self.map['distances'], []]])

        # Explore list of similar images
        for j in range(nb_images):
          #print "sim_images[i][j]",sim_images[i][0][j]
          nb_images_similar += 1
          output[out_i][self.map['similar_images']][self.map['image_sha1s']].append(sim_images[i][0][j][0].strip())
          output[out_i][self.map['similar_images']][self.map['cached_image_urls']].append(sim_images[i][0][j][1][self.url_field].strip())
          output[out_i][self.map['similar_images']][self.map['distances']].append(float(sim_score[i][0][j]))

      outp = OrderedDict([[self.map['number_images'], len(dets)],
                          [self.map['number_similar_images'], nb_images_similar],
                          [self.map['all_similar_images'], output]])

    print "[format_output: log] build_output took: {}".format(time.time() - start_build_output)
    #print outp
    return outp
