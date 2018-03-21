import dateutil
import dateutil.parser
import logging
import pandas as pd

from .util import *

logger = logging.getLogger(__name__)


class ELBLogCreationException(Exception): pass


class ELBLog:

  """all_fields = ['httpversion', 'path', 'urihost', 'elb', 'clientport',
  'port', 'clientip', 'backend_response', 'backend_processing_time',
  'timestamp', 'message', 'received_bytes', 'geoip', 'request', 'type', 'backendport',
  'proto', '@timestamp', 'bytes', 'verb', 'backendip', 'response', '@version',
  'response_processing_time', 'request_processing_time']"""

  initial_fields = {
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

  final_fields = {
    'received_bytes': 'float',
    'bytes': 'float',
    'verb': 'string',
    'response_processing_time': 'float',
    'path': 'string',
    'request_processing_time': 'float',
    'response': 'string',
    'urihost': 'string',
    'elb': 'string',
    'backend_processing_time': 'float',
    'second_sin': 'float',
    'second_cos': 'float',
    'month_sin': 'float',
    'month_cos': 'float',
  }

  @property
  def timestamp(self):
    return self._timestamp

  @timestamp.setter
  def timestamp(self, t):
    t = dateutil.parser.parse(t)
    # is this even needed
    self._timestamp = t.timestamp()
    sec = seconds_since_midnight(t)
    month = t.month
    self.second_sin = second_sin(sec)
    self.second_cos = second_cos(sec)
    self.month_sin = month_sin(month)
    self.month_cos = month_cos(month)

  @property
  def path(self):
    return self._path

  @path.setter
  def path(self, p):
    # simplify it down to first element of path
    self._path = p.split('/')[1]

  def __init__(self, src):
    for i in ELBLog.initial_fields:
      if i not in src:
        logger.error("%s field not in record" % i)
        raise ELBLogCreationException
      setattr(self, i, src[i])


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


def get_data_set(gen, included_fields):
  return DataSet([i for i in gen], included_fields)


def get_elb_logs(gen):
  for i in gen:
    cleaned_record = sanitize_fields(i['_source'])
    try:
      log = ELBLog(cleaned_record)
      yield log
    except ELBLogCreationException:
      logger.error("Failed to create record: ", i)
      continue
