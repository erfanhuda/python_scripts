import unittest
from algobro import (DB_WRITE_ERROR, DB_READ_ERROR, transaction)

class TransactionTest(unittest.TestCase):
    def test_input_data(self):
        pass

if __name__ == "__main__":
    print(transaction.DEFAULT_INPUT_DATA_PATH)
    unittest.main()
