import pandas as pd
import re

def transform_data():
    df = pd.read_csv('/var/tmp/data/data_gold.csv', encoding='utf-8')
    # Convert data to float
    df['buy_price'] = df['buy_price'].apply(lambda x: re.sub(',', '.', x)).astype('float')
    df['sell_price'] = df['sell_price'].apply(lambda x: re.sub(',', '.', x)).astype('float')
    
    df.to_csv('/var/tmp/data/data_transform.csv', index=False)
    print("Transform data successfully.")

if __name__=='__main__':
    transform_data()
    