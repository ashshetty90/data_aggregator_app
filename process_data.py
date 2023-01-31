import csv
from itertools import product
from itertools import groupby
from pprint import pprint

COLUMNS_TO_DROP = ['transaction_id', 'date', 'is_blocked', 'is_active']


class DataProcessor:
    """
    This class allows the user to filter, join and aggregate two data and prints the sorted
    result out to the console.
    The class accepts CSV files as input and prints out the aggregated numbers based on
    certain filter criteria.
    """

    def __init__(self, user_csv_path, txn_csv_path):
        self.user_csv_path = user_csv_path
        self.txn_csv_path = txn_csv_path

    def get_filtered_data(self, file_path, column_name, column_value):
        """ Given a csv file path, column key and column value returns a filtered dataset

            Parameters
            ----------
            file_path : str
                The location of the csv file
            column_name : str
                The name of the column to filter the data on
            column_value : str
                The value of the column to filter the data on
            Returns
            -------
            generator
                a generator object containing the filtered data
            """

        with open(file_path, 'r', newline="") as file:
            dict_reader = csv.DictReader(file)
            yield from filter(
                lambda r: r[column_name] == column_value,
                dict_reader)

    def join_data_sets(self, user_iter, txn_iter, join_key):
        """ Based on the join_key parameter, joining two datasets

            Parameters
            ----------
            user_iter : generator
                Generator object containing user data
            txn_iter : generator
               Generator object containing transaction data
            join_key : str
               string to join the two datasets on

            Returns
            -------
            list
                a list of dictionaries containing the merged data
            """
        merged_data_list = []
        for a, b in product(user_iter, txn_iter):
            if a[join_key] == b[join_key]:
                temp_dict = dict(a | b)
                [temp_dict.pop(key) for key in COLUMNS_TO_DROP]
                temp_dict["user_id_count"] = 1
                merged_data_list.append(temp_dict)
        return merged_data_list

    def aggregate_data(self, iterable_product):
        """Returns aggregated data for the joined data set

            Parameters
            ----------
            iterable_product : GroupBy
                an iterableGroup class object
            Returns
            -------
            list
                a list of dictionary objects containing  aggregated data
            """

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
        """ Entrypoint to the application from the main class
        """

        user_iterable = self.get_filtered_data(self.user_csv_path, "is_active", "True")
        transaction_iterable = self.get_filtered_data(self.txn_csv_path, "is_blocked", "False")

        transaction_users_data_set = self.join_data_sets(user_iterable, transaction_iterable, "user_id")
        aggregated_table_data = self.aggregate_data(groupby(transaction_users_data_set,
                                                            key=lambda x: (x['transaction_category_id'])))

        aggregated_table_data.sort(key=lambda x: x['aggregated_transaction_amount'], reverse=True)
        pprint(aggregated_table_data)
