import threading
import time
import random

import requests
from lxml import html


class FinancePriceWorker(threading.Thread):
    def __init__(self, symbol, **kwargs):
        super(FinancePriceWorker, self).__init__(**kwargs)  # == super().__init__(**kwargs)
        self._symbol = symbol
        self._url = f"https://www.marketwatch.com/investing/stock/{self._symbol}?mod=quote_search"
        self.start()

    def run(self):
        # be nice, don't spam
        time.sleep(30 * random.random())
        response = requests.get(self._url)
        page_content = html.fromstring(response.text)
        price_string = page_content.xpath('/html/body/div[3]/div[2]/div[3]/div/div[2]/h2/bg-quote')[0].text
        price = float(price_string.replace(",", ""))
        print(price)
