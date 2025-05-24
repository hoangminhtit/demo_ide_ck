import pandas as pd
import sqlite3
import os

def write_sql_file(db_file, file_data):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    df = pd.read_csv(file_data)

    print("Create table")
    script = """
        CREATE TABLE IF NOT EXISTS price_gold (
            type_name VARCHAR(255),
            buy_price FLOAT,
            sell_price FLOAT,
            location VARCHAR(255)
        )        
    """
    cur.execute(script)

    print("Insert data to database")
    for _, data in df.iterrows():
        query = """
            INSERT INTO price_gold
            VALUES (?, ?, ?, ?)
        """
        values = (data['type_name'], data['buy_price'], data['sell_price'], data['location'])
        cur.execute(query, values)

    conn.commit()
    conn.close()
    print("Insert thành công!")

def create_db_file():
    with open('/var/tmp/sqlite/database.db', 'w') as f:
        f.write('')
    print("Create database.db file")

if __name__=='__main__':
    create_db_file()
    input_file = '/var/tmp/data/data_transform.csv'
    db_file = '/var/tmp/sqlite/database.db'
    write_sql_file(db_file, input_file)
