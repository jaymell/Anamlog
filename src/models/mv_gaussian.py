from scipy.stats import multivariate_normal
import numpy as np


def get_mean(x, axis=0):
  return x.mean(axis=axis)


def get_sigma(x):
  # Sigma = 1/m(Sigma((x^i-mu))(x^i-mu)^T)
  return np.cov(x, rowvar=False)


def get_probabilities(x, mu, sigma):
  return multivariate_normal.pdf(x, mean=mu, cov=sigma)

