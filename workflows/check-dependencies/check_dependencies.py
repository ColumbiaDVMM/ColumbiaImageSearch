import json
import importlib
from pyspark import SparkContext, SparkConf


def get_package_version(package_line):
  """ Extract package name and version requirement from package requirement line.
  """
  # would fail if requirement is strict "<" or ">"
  package_split = line_req.split("=")
  package = package_split[0].strip("><").strip()

  version = None
  req_type = None
  version_str = ""

  if len(package_split)>1:
    if package_split[0][-1] == ">":
      req_type = "at_least"
    if package_split[0][-1] == "=":
      req_type = "equal"
    for tmp_ver in package_split[1:]:
      if tmp_ver != ">" and tmp_ver != "=":
        version = tmp_ver.strip()
        version_str = " {} version {}".format(req_type, version)
  return package, version, req_type, version_str


def validate_package(package, version, req_type, version_str):
  """ Check package can be imported and version satisfies requirement.
  """

  package_ok = False
  print "Checking package {}{}:".format(package, version_str),
  try:
    imported_package = importlib.import_module(package)
    if version is not None:
      #print imported_package.__version__,version,req_type
      if req_type == "equal":
        if imported_package.__version__ == version:
          package_ok = True
      elif req_type == "at_least":
        inst_version_split = imported_package.__version__.split('.')
        req_version_split = version.split('.')
        max_ver_split = max(len(inst_version_split), len(req_version_split))
        # find if version installed is equal or superior
        for i in range(max(len(inst_version_split), len(req_version_split))):
          if int(req_version_split[i])<int(inst_version_split[i]):
            package_ok = True
            break
          if int(req_version_split[i])>int(inst_version_split[i]):
            package_ok = False
            break
          if i == max_ver_split-1 and int(req_version_split[i])==int(inst_version_split[i]):
            package_ok = True
    else:
      package_ok = True
    if package_ok:
      print "OK ({} {})".format(package, imported_package.__version__)
      return package_ok
  except Exception as inst:
    err_msg = "{}".format(inst)
    print "NOT INSTALLED ({})".format(err_msg.strip())
    return package_ok
  print "VERSION REQUIREMENT NOT STATISFIED: {} vs {}".format(imported_package.__version__, version)
  return package_ok


if __name__ == '__main__':
  with open('requirements.txt','rt') as req:
    missing_packages = []

    # check every line of the requirements file
    for line_req in req:
      #print "Checking requirement: {}".format(line_req.strip())
      package, version, req_type, version_str = get_package_version(line_req)
      package_ok = validate_package(package, version, req_type, version_str)
      if not package_ok:
        missing_packages.append(package)

    if len(missing_packages):
      print "The following packages do not meet the requirements: {}".format(missing_packages)