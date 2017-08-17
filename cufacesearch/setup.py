from setuptools import setup, find_packages

setup(
  name='cufacesearch',
  version='0.2',
  packages=find_packages(exclude=['contrib', 'docs', 'tests']),
  install_requires=['numpy', 'scikit-image', 'dlib', 'kafka-python', 'happybase', 'flask', 'flask_restful', 'requests', 'matplotlib', 'gevent'],
  url='',
  license='Apache',
  author='Svebor Karaman',
  author_email='svebor.karaman@columbia.edu',
  description='Face indexing'
)
