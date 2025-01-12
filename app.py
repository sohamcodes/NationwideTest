from task2 import upload_fraud_csv
from task3 import sanitize_transactions
from task4 import count_fraud


def main():
    # Inputs from the user 
    print()
    fraud_csv_file_path = input('Please Input file path of fraud.csv:  ')
    transaction001_csv_file_path= input('Please Input file path of transaction001.csv:  ')
    transaction002_csv_file_path= input('Please Input file path of transaction002.csv:  ')
    print ()

    # TASK 2 
    ''' Task 2 involves the following 
        a) Pre-process data in fraud.csv so that it has appropriate structure
        b) Output the processed data as a csv to a folder location
        c) Establish connection to a local PostgresDB Server 
        d) Create Table and Schema
        e) Insert processed data.csv to the table in local PostgresDB server
    '''
    print ("----------------------Task 2 -------------------")
    upload_fraud_csv(fraud_csv_file_path)
    print ()

   # TASK 3 
    '''Task 3 involves the following
    a) Create Table and schema in PostgreSQL for transactions_001 and transactions_002
    b) Insert CSV files into the the table 
    c) Perform sanitization on the two datasets '''
    
    print ("----------------------Task 3 ------------------")
    sanitize_transactions(transaction001_csv_file_path, transaction002_csv_file_path)
    print()
    
    # TASK 4
    '''Task 4 involves the following
    a) Join 'fraud' and 'santized_transaction_01' tables on 'credit_card_number' column and calculate total fraud on santized dataset 1
    b) Join 'fraud' and 'santized_transaction_02' tables on 'credit_card_number' column and calculate total fraud on santized dataset 2
    c) Report the total 'fraud' in sanitized transactions 
    '''
    print ("----------------------Task 4 -------------------")
    count_fraud()
    print()
    
    print ("----------------------END OF TASK -------------------")

if __name__ == "__main__":
    main()
