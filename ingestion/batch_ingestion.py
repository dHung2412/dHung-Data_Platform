from binance.client import Client

class BinanceBatchIngestion:
    def __init__(self, api_key: str = None, api_secret: str = None):
        self.client = Client(api_key, api_secret)
         