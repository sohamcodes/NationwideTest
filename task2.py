
def upload_fraud_csv(input_csv_path):
    from dotenv import load_dotenv
    import os
    import json
    import psycopg2
    import pandas as pd

    # Load all the environment details from .env
    load_dotenv()
    hostname = os.environ.get("hostname")
    database= os.environ.get("database")
    username= os.environ.get("username")
    pwd=os.environ.get("pwd")
    port_id=os.environ.get("port_id")
    conn=None
    cur=None

    try:

        # Preprocess the file to make it consistent
        with open(input_csv_path, "r") as file:
            lines = file.readlines()

        # Identify the maximum number of columns
        max_columns = max(len(line.split(",")) for line in lines)

        processed_lines = []
        for line in lines:
            parts = line.strip().split(",")
            while len(parts) < max_columns:
                parts.append("")  # Add empty fields for missing columns
            processed_lines.append(",".join(parts))

        # Write to a temporary file
        processed_csv_path = "processed_file.csv"
        with open(processed_csv_path, "w") as file:
            file.write("\n".join(processed_lines))

        # Load the standardized file into Pandas
        df = pd.read_csv(processed_csv_path)

        # Fill missing values with NULL
        df.fillna("NULL", inplace=True)

        # Save the cleaned DataFrame to a new CSV file
        output_csv_path = "sanitized_fraud_output.csv"
        df.to_csv(output_csv_path, index=False)
        print("Pre-processing fraud.csv completed. Ready to be uploaded to PostgresDB")
        print()
        pass
    except FileNotFoundError:
        print(f"Error: The file '{input_csv_path}' was not found. Please Ensure you input the right file path.")
        print()
        print('Program will now terminate at Task 1.')
        exit(1)

    try: 
        # Establish connection to local PostgresDB server 
        conn= psycopg2.connect(
            host=hostname,
            dbname= database,
            user = username, 
            password=pwd,
            port=port_id)
        
        print("connection successfully established to Local PostgresDB server")
        print()

        # Start cursor within Local PostgresDB 
        cur=conn.cursor()

        # Create Table 
        CREATE_SCRIPT_1 = '''
        CREATE TABLE IF NOT EXISTS fraud_001(
            credit_card_number bigint,
            ipv4 inet,
            state varchar(40)
        )
        '''
        # Execute the SQL script 
        cur.execute(CREATE_SCRIPT_1)

        # Insert CSV file from location to table 
        with open(output_csv_path, 'r') as f:
            cur.copy_expert(f"COPY fraud_001 (credit_card_number, ipv4, state) FROM stdin WITH CSV HEADER", f)
        
        
        conn.commit()
        
        print ("Table inserted in Local PostgresDB")
    except Exception as error:
        print("Error in PostgresDB credentials. Please check and enter again.")
        print()
        print ("Program will now terminate at Task 1. Please try again with correct DB credentials.")
        exit(1)
        
    finally:
        # Close connection to local PostgresDB server 
        if cur is not None:
            cur.close()
        if conn is not None: 
            conn.close()
