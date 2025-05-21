import pandas as pd
import psycopg2 as ps

def connect_database():
    try:
        conn = ps.connect(
            host='host.docker.internal',
            port=5432,
            database='postgres',
            user='postgres',
            password='postgres',
        )
        print("Connect to database")
        return conn
    except Exception as e:
        print("Error ", e)
        return None

def create_table():
    conn = connect_database()
    if conn is None:
        print("Không thể kết nối đến database, thoát khỏi create_table.")
        return
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS price_gold (
            type_name VARCHAR(255),
            buy_price FLOAT,
            sell_price FLOAT,
            location VARCHAR(255)
        )        
        """)
    conn.commit()
    print(f"Create table price_gold")

def insert_data_to_db():
    conn = connect_database()
    cur = conn.cursor()
    df = pd.read_csv('/var/tmp/data/data_transform.csv')
    for _, data in df.iterrows():  
        query = """
        INSERT INTO price_gold
        VALUES (%s, %s, %s, %s)
        """
        values = (data['type_name'], data['buy_price'], data['sell_price'], data['location'])
        cur.execute(query, values)
    conn.commit()
    conn.close()
    print("Insert data to database")


if __name__=='__main__':
    create_table()
    insert_data_to_db()
    
    