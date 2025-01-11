def sanitize_transactions(csv_file_path1, csv_file_path2):
    from dotenv import load_dotenv
    import os
    import json
    import psycopg2
    import pandas as pd

    load_dotenv()
    hostname = os.environ.get("hostname")
    database= os.environ.get("database")
    username= os.environ.get("username")
    pwd=os.environ.get("pwd")
    port_id=os.environ.get("port_id")
    conn=None
    cur=None
     
    try: 
        conn= psycopg2.connect(
            host=hostname,
            dbname= database,
            user = username, 
            password=pwd,
            port=port_id)

        print ("Connection to local PostgresDB server successfully established")
        print ()
        cur=conn.cursor()

        # Create Table 
        CREATE_SCRIPT_1 = '''
        CREATE TABLE IF NOT EXISTS Transactions_001(
            credit_card_number bigint,
            ipv4 inet,
            state varchar(40)
        )
        '''
        
        CREATE_SCRIPT_2 = '''
        CREATE TABLE IF NOT EXISTS Transactions_002(
            credit_card_number bigint,
            ipv4 inet,
            state varchar(40)
        )
        '''
        cur.execute(CREATE_SCRIPT_1)
        cur.execute(CREATE_SCRIPT_2)
        
        print (" Transaction table 1 Schema successfully created in PostgresDB" )
        print (" Transaction table 2 Schema successfully created in PostgresDB")
        print ()
        
        try:
            # Insert CSV file from location to table 
            with open(csv_file_path1, 'r') as f:
               cur.copy_expert(f"COPY Transactions_001 (credit_card_number, ipv4, state) FROM stdin WITH CSV HEADER", f)
        except FileNotFoundError:
            print(f"Error: The file '{csv_file_path1}' was not found. Please Ensure you input the right file path for transactions001.")
            print()
            print('Program will now terminate at Task 2.')
            exit(1)
            
        try: 
            with open(csv_file_path2, 'r') as f:
                cur.copy_expert(f"COPY Transactions_002 (credit_card_number, ipv4, state) FROM stdin WITH CSV HEADER", f)
        except FileNotFoundError:
            print(f"Error: The file '{csv_file_path2}' was not found. Please Ensure you input the right file path for transactions002.")
            print()
            print('Program will now terminate at Task 2.')
            exit(1)
    
        print ("transaction-001.csv successfully uploaded to local PostgresDB ")
        print ("transaction-002.csv successfully uploaded to local PostgresDB ")
        print ()
        
        # SQL statements to sanitize transactions 
        SANITIZATION_SCRIPT_1= '''
        CREATE TABLE s_transaction_001 AS
        SELECT * 
        FROM Transactions_001
        WHERE CAST(credit_card_number AS text) LIKE '5018%' 
        OR CAST(credit_card_number AS text) LIKE '5020%' 
        OR CAST(credit_card_number AS text) LIKE '5038%' 
        OR CAST(credit_card_number AS text) LIKE '56%' 
        OR CAST(credit_card_number AS text) LIKE '51%' 
        OR CAST(credit_card_number AS text) LIKE '52%' 
        OR CAST(credit_card_number AS text) LIKE '54%' 
        OR CAST(credit_card_number AS text) LIKE '55%' 
        OR CAST(credit_card_number AS text) LIKE '222%' 
        OR CAST(credit_card_number AS text) LIKE '4%' 
        OR CAST(credit_card_number AS text) LIKE '34%' 
        OR CAST(credit_card_number AS text) LIKE '37%' 
        OR CAST(credit_card_number AS text) LIKE '6011%' 
        OR CAST(credit_card_number AS text) LIKE '65%' 
        OR CAST(credit_card_number AS text) LIKE '300%' 
        OR CAST(credit_card_number AS text) LIKE '301%' 
        OR CAST(credit_card_number AS text) LIKE '304%' 
        OR CAST(credit_card_number AS text) LIKE '305%' 
        OR CAST(credit_card_number AS text) LIKE '36%' 
        OR CAST(credit_card_number AS text) LIKE '38%' 
        OR CAST(credit_card_number AS text) LIKE '35%' 
        OR CAST(credit_card_number AS text) LIKE '2131%' 
        OR CAST(credit_card_number AS text) LIKE '1800%';
        '''
        
        SANITIZATION_SCRIPT_2= '''
        CREATE TABLE s_transaction_002 AS
        SELECT * 
        FROM Transactions_002 
        WHERE CAST(credit_card_number AS text) LIKE '5018%' 
        OR CAST(credit_card_number AS text) LIKE '5020%' 
        OR CAST(credit_card_number AS text) LIKE '5038%' 
        OR CAST(credit_card_number AS text) LIKE '56%' 
        OR CAST(credit_card_number AS text) LIKE '51%' 
        OR CAST(credit_card_number AS text) LIKE '52%' 
        OR CAST(credit_card_number AS text) LIKE '54%' 
        OR CAST(credit_card_number AS text) LIKE '55%' 
        OR CAST(credit_card_number AS text) LIKE '222%' 
        OR CAST(credit_card_number AS text) LIKE '4%' 
        OR CAST(credit_card_number AS text) LIKE '34%' 
        OR CAST(credit_card_number AS text) LIKE '37%' 
        OR CAST(credit_card_number AS text) LIKE '6011%' 
        OR CAST(credit_card_number AS text) LIKE '65%' 
        OR CAST(credit_card_number AS text) LIKE '300%' 
        OR CAST(credit_card_number AS text) LIKE '301%' 
        OR CAST(credit_card_number AS text) LIKE '304%' 
        OR CAST(credit_card_number AS text) LIKE '305%' 
        OR CAST(credit_card_number AS text) LIKE '36%' 
        OR CAST(credit_card_number AS text) LIKE '38%' 
        OR CAST(credit_card_number AS text) LIKE '35%' 
        OR CAST(credit_card_number AS text) LIKE '2131%' 
        OR CAST(credit_card_number AS text) LIKE '1800%';
        '''

        cur.execute(SANITIZATION_SCRIPT_1)
        cur.execute(SANITIZATION_SCRIPT_2)
        # Execute 
        conn.commit()
        print ("Sanitizations successfully carried out.")
        print ("Sanitized table of transaction_001  can be viewed as s_transaction_001 in local PostgresDB ")
        print ("Sanitized table of transaction_002  can be viewed as s_transaction_002 in local PostgresDB ")
        print()
    except Exception as error:
        print(error)

    finally:
        if cur is not None:
            cur.close()
        if conn is not None: 
            conn.close()
