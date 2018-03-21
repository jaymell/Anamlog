from src.data import *
import datetime


def load_config(config_file="config"):
  p = configparser.ConfigParser()
  p.read(config_file)

  config = {}

  config['ELASTIC_URL'] = os.environ.get('ELASTIC_URL',
    p.get('elastic', 'url'))
  config['ELASTIC_INDEX_PATTERN'] = os.environ.get('ELASTIC_INDEX_PATTERN',
    p.get('elastic', 'index_pattern'))
  config['NUM_MINS'] = int(os.environ.get('NUM_MINS',
    p.get('elastic', 'num_mins')))

  return config


def save_raw_data(elastic_url, index_pattern, num_mins, out_file=None):
  if out_file is None:
    out_file = "./data/out-%s.json" % datetime.datetime.now().timestamp()
  elk_gen = get_elasticsearch_records(elastic_url, index_pattern, num_mins)
  write_json_to_disk(elk_gen, out_file)


def load_raw_data(in_file):
  with open(in_file, 'r') as f:
    return json.load(f)


def load_live_df(elastic_url, index_pattern, num_mins):
  elk_gen = get_elasticsearch_records(elastic_url, index_pattern, num_mins)
  elb_log_gen = get_elb_logs(elk_gen)
  df = get_data_set(elb_log_gen, ELBLog.included_fields)
  return df


def get_training_set(save_raw=True):
  config = load_config()
  out_file = "./data/datetime.dat"
  if save_raw:
    save_raw_data(config['ELASTIC_URL'], config['ELASTIC_INDEX_PATTERN'], config['NUM_MINS'], out_file)
  raw_data = load_raw_data(out_file)
  elb_log_gen = get_elb_logs(raw_data)
  #df = sample_from_disk('./data/2018-03-17-last-7-days.csv', 50000)
  return get_data_set(elb_log_gen, ELBLog.final_fields)
