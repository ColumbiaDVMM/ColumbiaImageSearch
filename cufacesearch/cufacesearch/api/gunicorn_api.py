from __future__ import print_function

import os
import sys
import time
from datetime import datetime
from flask import Flask
from flask_restful import Api

from cufacesearch.api import api
from cufacesearch.searcher import searcher_lopqhbase

template_dir = os.path.abspath('../../../www/templates')
app = Flask(__name__, template_folder=template_dir)
#app = Flask(__name__)
DEFAULT_PORT = 5000

# DEFAULT_ENDPOINT = "cuimgsearch"
# DEFAULT_INPUT = "image"

def setup_app(app):
  print("[gunicorn_api] Setting up application...")
  app.secret_key = "secret_key"
  app.config['SESSION_TYPE'] = 'filesystem'

  searchapi = Api(app)
  app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

  # Set parameters
  options = dict()
  # Get parameters from environment variables
  options["conf"] = os.environ['SEARCH_CONF_FILE']
  options["endpoint"] = os.environ['SEARCH_ENDPOINT']
  options["input"] = os.environ['SEARCH_INPUT']
  options["port"] = os.getenv('SEARCH_PORT', DEFAULT_PORT)
  print("[gunicorn_api] options: ",options)

  api.global_start_time = datetime.now()
  # This will change the name of the fields in the output, i.e. AllSimilarImages vs AllSimilarFaces
  api.input_type = options["input"]

  # Initialize searcher object only once
  while True:
    try:
      api.global_searcher = searcher_lopqhbase.SearcherLOPQHBase(options["conf"])
      break
    except Exception as inst:
      err_msg = "Failed to initialized searcher ({}): {}".format(type(inst), inst)
      print(err_msg)
      sys.stdout.flush()
      time.sleep(60)

  # Setup API
  searchapi.add_resource(api.APIResponder, '/' + options["endpoint"] + '/<string:mode>')

  # Would that help prevent double requests from browser?
  #searchapi.add_url_rule('/favicon.ico', redirect_to=url_for('static', filename='favicon.ico'))

  @app.after_request
  def after_request(response):
    """ Adds appropriate headers to the HTTP response.

    :type response: HTTP response object.
    """
    response.headers.add('Access-Control-Allow-Headers',
                         'Keep-Alive,User-Agent,If-Modified-Since,Cache-Control,x-requested-with,Content-Type,origin,authorization,accept,client-security-token')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response


setup_app(app)


if __name__ == '__main__':
  app.run()
