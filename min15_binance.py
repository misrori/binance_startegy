from binance import Client
import pandas as pd
import numpy as np
from tqdm import tqdm
client = Client()

def get_data(symbol, interval='15m', period="1000 day ago UTC"):
    try:
        klines = pd.DataFrame(client.get_historical_klines(symbol, interval, period))
        df = klines.iloc[:,:6]
        df.columns = ['date','open','high','low','close','volume']
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        df['ticker']= symbol
        df.reset_index(inplace=True, drop=True)
        df.to_csv(f'/home/mihaly/python_codes/binance_data/data/{symbol}_{interval}.csv', index=False)
        return None
    except:
        pass


coin_pairs = client.get_all_tickers()
usdt_pairs = [pair['symbol'] for pair in coin_pairs if pair['symbol'].endswith('USDT') and not (pair['symbol'].endswith('UPUSDT') or pair['symbol'].endswith('DOWNUSDT'))]
crypto_data= list(map(get_data, tqdm(usdt_pairs) ))