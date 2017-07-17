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