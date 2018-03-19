import logging
import copy
import pandas as pd
import datetime
import dateutil.parser
import numpy as np
from sklearn import preprocessing

logger = logging.getLogger(__name__)

BAD_CHARS = ['@', '#', '%']

"""
remove correlated fields
remove zero columns
figure out how to properly bucket categoricals
create time variables
figure out which fields to remove
"""

class DataSet:
  def __init__(self, records, fields):
    self.records = records
    self.count = len(self.records)
    self.fields = fields
    for i in self.fields:
      logger.debug('field: ', i)
      if self.fields[i] == 'string':
        setattr(self, i, pd.get_dummies([ getattr(j, i) for j in self.records]))
      else:
        setattr(self, i, pd.DataFrame({ i: [ getattr(j, i) for j in self.records]}))

  def get_array(self):
    return pd.concat( [ getattr(self, i) for i in self.fields ], axis=1)


class ELBLogCreationException(Exception): pass


class ELBLog:

  """all_fields = ['httpversion', 'path', 'urihost', 'elb', 'clientport',
  'port', 'clientip', 'backend_response', 'backend_processing_time',
  'timestamp', 'message', 'received_bytes', 'geoip', 'request', 'type', 'backendport',
  'proto', '@timestamp', 'bytes', 'verb', 'backendip', 'response', '@version',
  'response_processing_time', 'request_processing_time']"""

  included_fields = {
    'received_bytes': 'float',
    'bytes': 'float',
    'timestamp': 'float',
    'verb': 'string',
    'response_processing_time': 'float',
    'path': 'string',
    'request_processing_time': 'float',
    'response': 'string',
    'urihost': 'string',
    'elb': 'string',
    'backend_processing_time': 'float'
  }

  @property
  def timestamp(self):
    return self._timestamp

  @timestamp.setter
  def timestamp(self, t):
    self._timestamp = dateutil.parser.parse(t).timestamp()

  @property
  def path(self):
    return self._path

  @path.setter
  def path(self, p):
    # simplify it down to first element of path
    self._path = p.split('/')[1]

  def __init__(self, src):
    for i in ELBLog.included_fields:
      if i not in src:
        logger.error("%s field not in record" % i)
        raise ELBLogCreationException
        setattr(self, i, src[i])


def contains_bad_chars(it):
  for i in BAD_CHARS:
    if i in it:
      return True
  return False


def remove_bad_chars(it):
  repl = ''
  for i in BAD_CHARS:
    it = it.replace(i, repl)
  return it


def sanitize_fields(obj):
  """ remove bad characters from field names --
      this will overwrite fields if conflicts occur """

  obj = copy.deepcopy(obj)
  for k in list(obj):
    if contains_bad_chars(k):
      old_k = k
      k = remove_bad_chars(k)
      if k in obj:
        logger.warn("Overwriting key %s" % k)
      obj[k] = obj[old_k]
      del obj[old_k]
    if isinstance(obj[k], dict):
      obj[k] = sanitize_fields(obj[k])
  return obj


def one_hot(x):
  """ broken """
  enc = preprocessing.LabelEncoder()
  ohe = preprocessing.OneHotEncoder()
  x = x.flatten()
  x = enc.fit_transform(x)
  np.set_printoptions(threshold=np.nan)
  x = ohe.fit_transform(x.reshape(-1, 1)).toarray()
  return x


def unix_milli(t):
  epoch = datetime.datetime.utcfromtimestamp(0)
  return (t - epoch).total_seconds() * 1000.0


def parse_http_code(_code):
  code = int(_code)
  if code in http_codes:
    return code
  # if not standard, round down to nearest 100
  return math.floor(code/100) * 100


def get_elb_logs(gen):
  """ raw json
  everything but ['_source'] stripped from json
  field names sanitized -- ie, '@timestamp' becomes 'timestamp'
  ELBLog objects created -- still raw data from json otherwise (ie, fields not one-hot encoded or otherwise engineered)
  DataFrame created -- field names corresponding to fields of ELBLog, with proper encoding
  DataFrame with highly collinear / zero fields removed
  Analysis """
  for i in gen:
    cleaned_record = sanitize_fields(i['_source'])
    try:
      yield ELBLog(cleaned_record)
    except ELBLogCreationException:
      print("Failed to create record: ", i)
      continue


def get_data_set(gen, included_fields):
  return DataSet([i for i in gen], included_fields)


def write_to_disk(gen, file):
  with open(file, 'w') as f:
    for i in gen:
      f.write(i)

