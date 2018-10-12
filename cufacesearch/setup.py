from setuptools import setup, find_packages
try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

# Could we have different requirements file for different dockers to only install what is needed?...
install_reqs = parse_requirements('cufacesearch/requirements.txt', session='hack')
reqs = [str(ir.req) for ir in install_reqs]

setup(
  name='cufacesearch',
  version='0.2',
  packages=find_packages(exclude=['contrib', 'docs', 'tests']),
  install_requires=reqs,
  url='',
  license='Apache',
  author='Svebor Karaman',
  author_email='svebor.karaman@columbia.edu',
  description='Image and Face Indexing for MEMEX'
)

#install_requires=['numpy', '' 'scikit-image>=0.13', 'dlib', 'elasticsearch', 'kafka-python', 'happybase', 'flask', 'flask_restful', 'requests', 'matplotlib', 'gevent'],