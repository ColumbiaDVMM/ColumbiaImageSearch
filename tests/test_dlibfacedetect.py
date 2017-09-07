from cufacesearch.detector.dlib_detector import DLibFaceDetector

if __name__ == "__main__":

  dfd = DLibFaceDetector()

  img_url = "http://www.cs.cmu.edu/~chuck/lennapg/len_top.jpg"
  out = dfd.detect_from_url(img_url)
  print out