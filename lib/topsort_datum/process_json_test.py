import unittest

import pandas as pd
import pytest
from ucimlrepo import fetch_ucirepo

from lib.topsort_datum.topsort_datum import process_json


class MyTestCase(unittest.TestCase):

    def test_something(self):
        # Fetch a dataset (e.g., Heart Disease, ID=45)
        dataset = fetch_ucirepo(id=186)

        try:

            df, order = process_json(dataset.metadata)
            print(df.to_json(indent=1))
        except Exception as e:
            pytest.fail(str(e))


if __name__ == '__main__':
    unittest.main()
