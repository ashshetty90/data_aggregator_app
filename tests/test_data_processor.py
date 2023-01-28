import unittest
from itertools import groupby

from process_data import DataProcessor


class TestDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        self.user_csv_path = "../resources/test/test_users.csv"
        self.txn_csv_path = "../resources/test/test_transactions.csv"
        self.data_processor = DataProcessor(self.user_csv_path, self.txn_csv_path)

    def test_filter_data(self):
        resp = self.data_processor.get_filtered_data(self.data_processor.user_csv_path, 'is_active', "False")
        self.assertEqual([a['user_id'] for a in resp], ['827079b9-0f56-454e-a8b6-28d540640865'])

    def test_join_data_sets(self):
        user_iterable = self.data_processor.get_filtered_data(self.user_csv_path, "is_active", "True")
        transaction_iterable = self.data_processor.get_filtered_data(self.txn_csv_path, "is_blocked", "False")
        transaction_users_data_set = self.data_processor.join_data_sets(user_iterable, transaction_iterable, "user_id")
        self.assertEqual(transaction_users_data_set, [{'transaction_amount': '74.86',
                                                       'transaction_category_id': '2',
                                                       'user_id': '627c6ac7-0da0-4393-9882-fadede2f38d9',
                                                       'user_id_count': 1},
                                                      {'transaction_amount': '11.97',
                                                       'transaction_category_id': '9',
                                                       'user_id': 'a9babba4-6989-4882-a225-330cd8b867e0',
                                                       'user_id_count': 1}])

    def test_aggregate_data(self):
        user_iterable = self.data_processor.get_filtered_data(self.user_csv_path, "is_active", "True")
        transaction_iterable = self.data_processor.get_filtered_data(self.txn_csv_path, "is_blocked", "False")
        transaction_users_data_set = self.data_processor.join_data_sets(user_iterable, transaction_iterable, "user_id")
        aggregated_table_data = self.data_processor.aggregate_data(groupby(transaction_users_data_set,
                                                                           key=lambda x: (
                                                                               x['transaction_category_id'])))
        self.assertEqual(aggregated_table_data, [
            {'transaction_category_id': '2', 'aggregated_transaction_amount': 74.86, 'distinct_user_count': 1},
            {'transaction_category_id': '9', 'aggregated_transaction_amount': 11.97, 'distinct_user_count': 1}])


if __name__ == '__main__':
    unittest.main()
