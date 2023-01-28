# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import csv
import itertools
import pprint
from collections import ChainMap
from functools import reduce
from itertools import dropwhile, takewhile
from operator import itemgetter

cod_enti_row_map = {}
cod_enti_row_map_2 = {}


def read_csv(file_path):
    # Use a breakpoint in the code line below to debug your script.
    with open(file_path, 'r') as csv_file:
        # datareader = csv.reader(csv_file) # use inex position if DictReader used. For Ex : row["0"]
        datareader = csv.DictReader(csv_file,
                                    skipinitialspace=True)  # use column name if DictReader used. For Ex : row["user_id"]
        ss = next(datareader)
        # print(ss)
        for row in datareader:
            # print(row)
            cod_enti = row["user_id"]
            cod_enti_row_map[cod_enti] = row
            # break
    # print(f"Map before join")
    # pprint.pprint(cod_enti_row_map, width=100, sort_dicts=False)
    # Press the green button in the gutter to run the script.

    with open("resources/users.csv", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            cod_enti = row["user_id"]

            # cod_enti = dict(filter(lambda elem: elem[1] == "True", cod_enti_row_map.items()))
            # cod_enti.write("test.csv")
            # skip cod_enti in enti.csv that is not in data.csv, like 209
            if cod_enti not in cod_enti_row_map:
                continue

            cod_enti_row_map[cod_enti].update(row)
            # cod_enti_row_map_2 = takewhile(
            #     lambda r: r['is_active'] == 'True',
            #     dropwhile(lambda r: r['is_blocked'] != 'True', reader))
            # cod_enti_row_map_2 = dict(filter(lambda elem: elem[1] == True, cod_enti_row_map.items()))
    # print(f"Map after join")
    print(f"Map after join and filter")
    # pprint.pprint(cod_enti_row_map, width=100, sort_dicts=True)
    # pprint.pprint(cod_enti_row_map_2, width=100, sort_dicts=True)


def filter_transactions_csv():
    with open("resources/transactions.csv", newline="") as f:
        reader = csv.DictReader(f)
        yield from filter(
            lambda r: r["is_blocked"] == "False",
            reader)


def filter_users_csv():
    with open("resources/users.csv", newline="") as f:
        reader = csv.DictReader(f)
        yield from filter(
            lambda r: r["is_active"] == "True",
            reader)
    # return
    # for row in reader:
    #     if row["is_active"] == "True":
    #         count += 1
    # print(count)


# def bleh():
#     for row in filter_users_csv():
#         yield row
#     return
# def read_csv_file(filename):
#     # read a csv file and return a list of lists
#     results = {}
#     with open(filename, 'r') as f:
#         myreader = csv.reader(f)
#         for row in myreader:
#             results.append(row)
#     return results

# def read_csv(file_path):
#     dict_reader =
#     with open(file_path, newline="") as f:
#         dict_reader = csv.DictReader(f)


def filter_csv(dict_reader, filter_key, filter_value):
    yield from filter(
        lambda r: r[filter_key] == filter_value,
        dict_reader)


if __name__ == '__main__':
    # read_csv("resources/transactions.csv")
    # read_csv("resources/users.csv")

    # for user in filter_users_csv():
    #     txn = list(filter_transactions_csv())
    #     uid = user["user_id"];
    #     if uid not in txn:
    #         continue
    #     cod_enti_row_map[uid].update(user)
    temp_dict = []
    file_headers = {}
    for a, b in itertools.product(filter_users_csv(), filter_transactions_csv()):
        uuid = a["user_id"]
        # print(type(a))
        # print(b)
        if a["user_id"] == b["user_id"]:
            # print("count before, ", count)
            # print("count after, ", count)
            # temp_dict = ChainMap(a, b)
            temp_2 = dict(a | b)
            temp_2.pop('transaction_id')
            temp_2.pop('date')
            temp_2.pop('is_blocked')
            temp_2.pop('is_active')
            temp_2["user_id_count"] = 1
            file_headers = temp_2.keys()
            temp_dict.append(temp_2)
    # print(temp_dict)
    # projuserDays = [[k, sum(float(v['transaction_amount']) for v in g)] for k, g in
    #                 itertools.groupby(temp_dict, key=lambda x: (x['transaction_category_id'], x['user_id']))]
    # projuserDays = itertools.groupby(temp_dict, key=lambda x: (x['transaction_category_id']))
    # print(projuserDays)
    # projuserDays2 = [[k, sum(float(v['transaction_amount']) for v in g)] for k, g in projuserDays]
    # print(projuserDays2)
    # projuserDays3 = [[k, sum(int(v['user_id_count']) for v in g)] for k, g in projuserDays]
    # print(projuserDays3)

    ###################v2#############################################
    # final_dict = []
    # for k, g in projuserDays:
    #     txn_amount = 0
    #     distinct_user_count = 0
    #     for v in g:
    #         # print(k)
    #         txn_amount += float(v['transaction_amount'])
    #         distinct_user_count += v['user_id_count']
    #         # print(v)
    #
    #     final_dict.append({'transaction_category_id': k, 'aggregated_transaction_amount': txn_amount,
    #                        'distinct_user_count': distinct_user_count})
    # final_dict.sort(key=lambda x: x['aggregated_transaction_amount'],reverse=True)
    ###################v2#############################################
    # projuserDays = [[k,(sum(float(v['transaction_amount']) for v in g)), (sum(w['user_id_count'])for w in g)] for k, g in
    #                 itertools.groupby(temp_dict, key=lambda x: (x['transaction_category_id']))]
    #
    # final_dict = [
    #     {'transaction_category_id': k,
    #      'aggregated_transaction_amount': sum(float(v['transaction_amount']) for v in g),
    #      'distinct_user_count': sum(s['user_id_count'] for s in g)}
    #     for k, g in itertools.groupby(temp_dict, key=lambda x: x['transaction_category_id'])
    # ]
    # final_dict.sort(key=lambda x: x['aggregated_transaction_amount'], reverse=True)
    # final_dict = [
    #     {'transaction_category_id': k,
    #      'aggregated_transaction_amount': sum(float(v['transaction_amount']) for v in g),
    #      'distinct_user_count': len(set(v['user_id'] for v in list(g)))}
    #     for k, g in itertools.groupby(temp_dict, key=lambda x: x['transaction_category_id'])
    # ]
    # final_dict = [
    #     {'transaction_category_id': k,
    #      'aggregated_transaction_amount': sum(float(v['transaction_amount']) for v in g),
    #      'distinct_user_count': sum(v['user_id_count'] for v in g)}
    #     for k, g in itertools.groupby(temp_dict, key=lambda x: x['transaction_category_id'])
    # ]
    # final_dict = [
    #     {'transaction_category_id': k,
    #      'aggregated_transaction_amount': sum(float(v['transaction_amount']) for v in g),
    #      'distinct_user_count': len(set(s['user_id_count']) for s in g)}
    #     for k, g in itertools.groupby(temp_dict, key=lambda x: x['transaction_category_id'])
    # ]
    # data = [
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 63.05, 3),
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 13.97, 4),
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 97.15, 4),
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 23.54, 5),
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 23.54, 5),
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 23.54, 5),
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 23.54, 5),
    #     ('b2d30a62-36bd-41c6-8221-987d5c4cd707', 23.54, 5)
    # ]
    # # distinct_user_count = len(set(row[0] for row in data))
    # result = []
    # for key, group in itertools.groupby(data, itemgetter(2)):
    #     group = list(group)
    #     distinct_user_count = len(set(row[0] for row in group))
    #     aggregated_transaction_amount = sum(row[1] for row in group)
    #     result.append({
    #         'transaction_category_id': key,
    #         'aggregated_transaction_amount': aggregated_transaction_amount,
    #         'distinct_user_count': distinct_user_count
    #     })
    #
    # print(result)
    # Keep track of unique user_ids for each transaction_category_id
    # unique_user_ids = {}
    # for d in temp_dict:
    #     if d['transaction_category_id'] not in unique_user_ids:
    #         unique_user_ids[d['transaction_category_id']] = set()
    #     unique_user_ids[d['transaction_category_id']].add(d['user_id'])
    #
    # final_dict = [
    #     {'transaction_category_id': k,
    #      'aggregated_transaction_amount': sum(float(v['transaction_amount']) for v in g),
    #      'distinct_user_count': len(unique_user_ids[k])}
    #     for k, g in itertools.groupby(temp_dict, key=lambda x: x['transaction_category_id'])
    # ]
    # final_dict.sort(key=lambda x: x['aggregated_transaction_amount'], reverse=True)

    # for s in v for k,v in projuserDays
    # print(final_dict)
    # aa = dict(itertools.chain(
    #     (k,((sum(float(s['transaction_amount'])), sum(s['user_id_count'])) for s in g) for k, g in projuserDays)))
    # aa = itertools.accumulate(
    #     (k,((sum(float(s['transaction_amount']))for s in g)) ) for k, g in projuserDays)
    #
    # for k, g in projuserDays:
    #     print(k)
    #     for v in g:
    #         print(sum(float(v['transaction_amount'])))
    #         print(sum(v['user_id']))
    # print(temp_dict)
    # itertools.chain()
    # itertools.count
    # total_sum = reduce(
    #     lambda x, y: {'transaction_amount': float(x['transaction_amount']) + float(y['transaction_amount'])}, temp_dict)
    # print(total_sum)
    # new_temp_dict = []
    # for row in temp_dict:
    #     new_temp_dict.append(row.('date'))
    #
    # print(new_temp_dict)
    # with open('mycsvfile.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
    #     w = csv.DictWriter(f, file_headers)
    #     w.writeheader()
    #     w.writerows(temp_dict)
    # for a, b in zip(filter_users_csv(),filter_transactions_csv()):
    #     # print(type(a))
    #     if a["user_id"] == b["user_id"]:
    #         print('hello')
    # print(b["user_id"])
    # if a["user_id"] != b["user_id"]:
    #     continue
    #
    # temp_dict[us_id].update(b)
    # break
    # print(temp_dict)

    # for x in filter_transactions_csv():
    #     count += 1
    # print(count)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
