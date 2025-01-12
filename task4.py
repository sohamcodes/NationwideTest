def count_fraud():

    from dotenv import load_dotenv
    import os
    import json
    import psycopg2
    import pandas as pd
    # Load environment variables from .env file. These are the DB secrets.
    load_dotenv()
    hostname = os.environ.get("hostname")
    database= os.environ.get("database")
    username= os.environ.get("username")
    pwd=os.environ.get("pwd")
    port_id=os.environ.get("port_id")
    conn=None
    cur=None
    
    try: 
        # Establish connection to the local PostgresDB server 
        conn= psycopg2.connect(
            host=hostname,
            dbname= database,
            user = username, 
            password=pwd,
            port=port_id)
        
        print("Connection to Local PostgresDB server successfully established")
        print()
        
        cur=conn.cursor()

        # Write the SQL scripts to report frauds in santized_transaction_001 and sanitized_transaction_002
        SCRIPT_1= '''
            SELECT COUNT(*) AS total_fraudulent_transactions
            FROM fraud_001 f
            JOIN s_transaction_001 s
            ON f.credit_card_number = s.credit_card_number;
        '''
        
        SCRIPT_2='''
            SELECT COUNT(*) as total_fraudulent_transactions
            FROM fraud_001 f
            JOIN s_transaction_002 s
            on f.credit_card_number = s.credit_card_number 
        '''

        # Execute the scripts 
        cur.execute(SCRIPT_1)
        result_1 = cur.fetchone()  # Fetch the first row
        total_fraudulent_transactions_1 = result_1[0]  # Extract the count
        
        cur.execute(SCRIPT_2)
        result_2 = cur.fetchone()  # Fetch the first row
        total_fraudulent_transactions_2 = result_2[0]  # Extract the count

        # Report the total fraud 
        sum_fraudulent=total_fraudulent_transactions_1+total_fraudulent_transactions_2;
        print("Total fraudulent transactions from SCRIPT_1:", total_fraudulent_transactions_1)
        print("Total fraudulent transactions from SCRIPT_2:", total_fraudulent_transactions_2)
        print("Rsultant total:", sum_fraudulent)
        print() 
        
        conn.commit()
    except Exception as error:
        print(error)

    finally:
        # Close connection to local PostgresDB server 
        if cur is not None:
            cur.close()
        if conn is not None: 
            conn.close()






