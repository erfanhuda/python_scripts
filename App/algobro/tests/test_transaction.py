import unittest
from algobro import (DB_WRITE_ERROR, DB_READ_ERROR, transaction, database)

class TransactionTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_input_data(self):
        pass
    
    def test_init_db(self):
        first_handler = database.RecordHandler()
        first_handler._init_database()


if __name__ == "__main__":
    unittest.main()