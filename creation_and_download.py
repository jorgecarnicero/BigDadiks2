from TradingviewData import TradingViewData,Interval
import boto3
from constantes import *


def load_data():

    request = TradingViewData()

    request.search('SOLUSD', 'BINANCE')

    solana_data = request.get_hist(
        symbol = SYMBOL, 
        exchange = EXCHANGE, 
        interval = Interval.daily, 
        n_bars = DAYS
    )

    return solana_data
    
def s3_creator():

    client = boto3.client('s3')

    client.create_bucket()



def main():
    
    data = load_data()



if __name__ == "__main__":
    main()