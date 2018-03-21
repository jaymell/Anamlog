import configparser
import os
import datetime
import json
import copy
import logging
import numpy as np

logger = logging.getLogger(__name__)

BAD_CHARS = ['@', '#', '%']

seconds_in_day = 24*60*60
second_sin = lambda s: np.sin(2*np.pi*s/seconds_in_day)
second_cos = lambda s: np.cos(2*np.pi*s/seconds_in_day)
month_sin = lambda m: np.sin(2*np.pi*m/12)
month_cos = lambda m: np.cos(2*np.pi*m/12)
dow_sin = lambda d: np.sin(2*np.pi*d/7)
dow_cos = lambda d: np.cos(2*np.pi*d/7)
seconds_since_midnight = lambda t: (t - t.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()


def unix_milli(t):
  epoch = datetime.datetime.utcfromtimestamp(0)
  return (t - epoch).total_seconds() * 1000.0


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
  """ recursively remove bad characters from field names --
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


def parse_http_code(_code):
  code = int(_code)
  if code in http_codes:
    return code
  # if not standard, round down to nearest 100
  return math.floor(code/100) * 100


def write_to_disk(gen, file):
  with open(file, 'w') as f:
    for i in gen:
      f.write(i)


def write_json_to_disk(gen, file):
  with open(file, 'w') as f:
    for i in gen:
      json.dump(i, f)


def sample_df_from_disk(file, sample_size):
  n = sum(1 for line in open(file)) - 1 #number of records in file (excludes header)
  skip = sorted(random.sample(range(1,n+1),n-sample_size))
  return pd.read_csv(file, skiprows=skip)


# https://stackoverflow.com/questions/17778394/list-highest-correlation-pairs-from-a-large-correlation-matrix-in-pandas
### instead of get_redundant_pairs(df),
### you can use "cor.loc[:,:] = np.tril(cor.values, k=-1)"
### and then "cor = cor[cor>0]"
def get_redundant_pairs(df):
    '''Get diagonal and lower triangular pairs of correlation matrix'''
    pairs_to_drop = set()
    cols = df.columns
    for i in range(0, df.shape[1]):
        for j in range(0, i+1):
            pairs_to_drop.add((cols[i], cols[j]))
    return pairs_to_drop


def get_top_abs_correlations(df, n=5):
    au_corr = df.corr().abs().unstack()
    labels_to_drop = get_redundant_pairs(df)
    au_corr = au_corr.drop(labels=labels_to_drop).sort_values(ascending=False)
    return au_corr[0:n]
