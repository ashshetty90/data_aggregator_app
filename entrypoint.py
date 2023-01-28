import csv
from itertools import product
from itertools import groupby
from pprint import pprint

from process_data import DataProcessor

USERS_CSV_FILE_PATH = "resources/users.csv"
TRANSACTIONS_CSV_FILE_PATH = "resources/transactions.csv"


# def get_filtered_data(file_path, column_name, value):
#     with open(file_path, 'r', newline="") as file:
#         dict_reader = csv.DictReader(file)
#         yield from filter(
#             lambda r: r[column_name] == value,
#             dict_reader)
#
#
# def aggregate_data(iterable_product: object) -> object:
#     aggregated_data_set = []
#     for txn_ctg_id, obj_list in iterable_product:
#         txn_amount = 0
#         distinct_user_count = 0
#         for obj in obj_list:
#             txn_amount += float(obj['transaction_amount'])
#             distinct_user_count += obj['user_id_count']
#
#         aggregated_data_set.append({'transaction_category_id': txn_ctg_id, 'aggregated_transaction_amount': txn_amount,
#                                     'distinct_user_count': distinct_user_count})
#     return aggregated_data_set
#
#
# def join_data_sets(user_iter, txn_iter, join_key):
#     merged_data_list = []
#     for a, b in product(user_iter, txn_iter):
#         if a[join_key] == b[join_key]:
#             temp_2 = dict(a | b)
#             [temp_2.pop(key) for key in COLUMNS_TO_POP]
#             temp_2["user_id_count"] = 1
#             file_headers = temp_2.keys()
#             merged_data_list.append(temp_2)
#     return merged_data_list
#
#
# def process(users_csv_path, txn_csv_path):
#     file_headers = {}
#     user_iterable = get_filtered_data(users_csv_path, "is_active", "True")
#     transaction_iterable = get_filtered_data(txn_csv_path, "is_blocked", "False")
#
#     transaction_users_data_set = join_data_sets(user_iterable, transaction_iterable, "user_id")
#     final_dict = aggregate_data(groupby(transaction_users_data_set,
#                                         key=lambda x: (x['transaction_category_id'])))
#
#     final_dict.sort(key=lambda x: x['aggregated_transaction_amount'], reverse=True)
#     pprint(final_dict)


if __name__ == '__main__':
    data_processor = DataProcessor(USERS_CSV_FILE_PATH, TRANSACTIONS_CSV_FILE_PATH)
    data_processor.process()
