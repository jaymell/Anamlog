from elasticsearch import Elasticsearch
from elasticsearch import helpers
import datetime
import json
import logging

from .util import *

logger = logging.getLogger(__name__)


def get_elasticsearch_records(url, index_pattern, n_mins_ago=None):
  """ return generator """

  es = Elasticsearch(url, verify_certs=False)


  if n_mins_ago is None:
    from_time = 0
  else:
    from_time = unix_milli(datetime.datetime.now() \
      - datetime.timedelta(minutes=n_mins_ago))

  to_time = unix_milli(datetime.datetime.now())

  request = """{
    "version": true,
    "sort": [
      {
        "@timestamp": {
          "order": "desc",
          "unmapped_type": "boolean"
        }
      }
    ],
    "stored_fields": [
      "_source"
    ],
    "script_fields": {},
    "docvalue_fields": [
      "@timestamp"
    ],
    "query": {
      "bool": {
        "must": [
          {
            "query_string": {
              "query": "NOT tags: _grokparsefailure",
              "analyze_wildcard": true,
              "default_field": "*"
            }
          },
          {
            "range": {
              "@timestamp": {
                "gte": %s,
                "lte": %s,
                "format": "epoch_millis"
              }
            }
          }
        ]
      }
    }
  }""" % (from_time, to_time)

  gen = helpers.scan(es,
    index=index_pattern,
    query=json.loads(request),
    # _source_include=[ i for i in fields],
    scroll='2m',
    size=10000)

  return gen

