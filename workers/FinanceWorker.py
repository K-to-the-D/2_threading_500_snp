import threading
import random
import time
from queue import Empty
from datetime import datetime

import requests
from lxml import html


# Thread
class FinancePriceScheduler(threading.Thread):
    def __init__(self, input_queue, output_queues, **kwargs):
        super(FinancePriceScheduler, self).__init__(**kwargs)  # == super().__init__(**kwargs)
        self._input_queue = input_queue
        # there can be one or several output queues. E.g.: to save into many DBs
        self._output_queues = output_queues if isinstance(output_queues, list) else [output_queues]
        self.start()

    def run(self):
        while True:
            try:
                input_kwargs = self._input_queue.get(timeout=10)  # blocking operation, it blocks loop until value is returned
            except Empty as e:
                print("Finance scheduler is empty. Stop Thread.", e)
                break
            if input_kwargs.get("DONE") is True:
                break

            finance_price_worker = FinancePriceWorker(symbol=input_kwargs.get("symbol"))
            output_kwargs = finance_price_worker.get_price()

            for output_queue in self._output_queues:
                output_queue.put(output_kwargs)
            time.sleep(random.random())  # be nice, don't spam

        # for output_queue in self._output_queues:
        #     for i in range(20):
        #         output_queue.put({"DONE": True})


# Extraction logic of thread "FinancePriceScheduler"
class FinancePriceWorker:
    def __init__(self, symbol, random_sleep=1, *args, **kwargs):
        self._symbol = symbol
        self._url = f"https://www.marketwatch.com/investing/stock/{self._symbol}?mod=quote_search"
        self._random_sleep = random_sleep

    def get_price(self):
        # be nice, don't spam
        time.sleep(self._random_sleep * random.random())

        response = requests.get(self._url)
        if response.status_code != 200:
            return {}

        page_content = html.fromstring(response.text)
        price_string = page_content.xpath('/html/body/div[3]/div[2]/div[3]/div/div[2]/h2/bg-quote')[0].text
        price = float(price_string.replace(",", ""))
        return {"symbol": self._symbol, "price": price, "extracted_time": datetime.utcnow()}

    def execute(self):
        return self.get_price()
