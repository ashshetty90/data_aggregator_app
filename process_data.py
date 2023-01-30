import csv
from itertools import product
from itertools import groupby
from pprint import pprint

COLUMNS_TO_DROP = ['transaction_id', 'date', 'is_blocked', 'is_active']


class DataProcessor:
    def __init__(self, user_csv_path, txn_csv_path):
        self.user_csv_path = user_csv_path
        self.txn_csv_path = txn_csv_path

    @staticmethod
    def get_filtered_data(file_path, column_name, value):
        with open(file_path, 'r', newline="") as file:
            dict_reader = csv.DictReader(file)
            yield from filter(
                lambda r: r[column_name] == value,
                dict_reader)

    @staticmethod
    def join_data_sets(user_iter, txn_iter, join_key):
        merged_data_list = []
        for a, b in product(user_iter, txn_iter):
            if a[join_key] == b[join_key]:
                temp_dict = dict(a | b)
                [temp_dict.pop(key) for key in COLUMNS_TO_DROP]
                temp_dict["user_id_count"] = 1
                merged_data_list.append(temp_dict)
        return merged_data_list

    @staticmethod
    def aggregate_data(iterable_product):
        aggregated_data_set = []
        for txn_ctg_id, obj_list in iterable_product:
            obj_list_copy = list(obj_list).copy()
            txn_amount = sum(float(obj['transaction_amount']) for obj in obj_list_copy)
            distinct_user_count = sum(obj['user_id_count'] for obj in obj_list_copy)
            aggregated_data_set.append(
                {'transaction_category_id': txn_ctg_id,
                 'aggregated_transaction_amount': txn_amount,
                 'distinct_user_count': distinct_user_count
                 }
            )
        return aggregated_data_set

    def process(self):
        user_iterable = self.get_filtered_data(self.user_csv_path, "is_active", "True")
        transaction_iterable = self.get_filtered_data(self.txn_csv_path, "is_blocked", "False")

        transaction_users_data_set = self.join_data_sets(user_iterable, transaction_iterable, "user_id")
        aggregated_table_data = self.aggregate_data(groupby(transaction_users_data_set,
                                                            key=lambda x: (x['transaction_category_id'])))

        aggregated_table_data.sort(key=lambda x: x['aggregated_transaction_amount'], reverse=True)
        pprint(aggregated_table_data)
