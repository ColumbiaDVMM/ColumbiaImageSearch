# Building the documentation

This codebase is for python 2.7. We detail here how to build the documentation with anaconda:

- Setup a python 2.7 virtual environment: `conda create --name cis_py27 python=2.7`
- Activate the environment: `conda activate cis_py27`
- Install the `cufacesearch` requirements [requirements.txt](../cufacesearch/requirements.txt): ` pip install -r ../cufacesearch/requirements.txt`
- Install the `lopq` requirements [requirements.txt](../../lopq/requirements.txt): ` pip install -r ../../lopq/requirements.txt`
- Install the `cufacesearch` package: `pip install -e ../../cufacesearch/`  

Then, in this folder, run:
- ``make html``: to build the web documentation.
- ``make latexpdf``: to build the documentation in PDF.