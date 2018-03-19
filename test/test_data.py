from src.data import *
import unittest


class TestDataFunctions(unittest.TestCase):

  def test_sanitizer(self):
    bad_record = {
      '@one': 1,
      '@two': 2,
      '@three': {
        '@one': 1,
        '@two': 2,
        '@three': 3
      }
    }

    sanitized = sanitize_fields(bad_record)
    expected = {
      'one': 1,
      'two': 2,
      'three':
        { 'one': 1,
          'two': 2,
          'three': 3,
        }
    }

    self.assertDictEqual(sanitized, expected)
