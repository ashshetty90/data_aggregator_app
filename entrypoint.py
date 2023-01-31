
from process_data import DataProcessor

USERS_CSV_FILE_PATH = "resources/users.csv"
TRANSACTIONS_CSV_FILE_PATH = "resources/transactions.csv"

if __name__ == '__main__':
    data_processor = DataProcessor(USERS_CSV_FILE_PATH, TRANSACTIONS_CSV_FILE_PATH)
    data_processor.process()
