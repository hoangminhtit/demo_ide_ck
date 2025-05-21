import requests
import pandas as pd
from bs4 import BeautifulSoup

def crawl_price_gold(url):
    price_golds = []
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        for inform in data['data']:
            try:
                inform_gold = {
                    'type_name': inform['TypeName'],
                    'buy_price': inform['Buy'],
                    'sell_price': inform['Sell'],
                    'location': inform['BranchName']
                }
                price_golds.append(inform_gold)
            except Exception as e:
                print(f"Error {e}")
    return price_golds

if __name__=="__main__":
    url = r'https://sjc.com.vn/GoldPrice/Services/PriceService.ashx'
    data_extract = crawl_price_gold(url)
    df = pd.DataFrame(data_extract)
    df.to_csv('/var/tmp/data/data_gold.csv', index=False)
    print("Storage data successfully.")
