from src.data import *
from src.data import elasticsearch as es
import configparser
import os


def load_config(config_file="config"):
  p = configparser.ConfigParser()
  p.read(config_file)

  config = {}
  config['ELASTIC_URL'] = os.environ.get('ELASTIC_URL', p.get('elastic', 'url'))
  config['ELASTIC_INDEX_PATTERN'] = os.environ.get('ELASTIC_INDEX_PATTERN', p.get('elastic', 'index_pattern'))
  config['NUM_MINS'] = int(os.environ.get('NUM_MINS', p.get('elastic', 'num_mins')))

  return config


def load_data_set(config):
  elk_gen = es.get_records(config['ELASTIC_URL'], config['ELASTIC_INDEX_PATTERN'], config['NUM_MINS'])
  elb_log_gen = get_elb_logs(elk_gen)
  df = get_data_set(elb_log_gen, ELBLog.included_fields)
  return df


def main():
  config = load_config()
  df = load_data_set(config)


if __name__ == '__main__':
  main()