import threading

import requests
from bs4 import BeautifulSoup


class SymbolWorkerMasterScheduler(threading.Thread):
    def __init__(self, output_queues, **kwargs):
        if "input_queue" in kwargs:
            kwargs.pop("input_queue")
        # input_values are URLs to scrape
        self._input_values = kwargs.pop("input_values")
        self._output_queues = output_queues if isinstance(output_queues, list) else [output_queues]
        super(SymbolWorkerMasterScheduler, self).__init__(**kwargs)
        self.start()

    def run(self):
        for value in self._input_values:
            symbol_worker = SymbolWorker(value)
            for i, symbol in enumerate(symbol_worker.get_snp_500_companies()):
                for output_queue in self._output_queues:
                    output_queue.put(symbol)
                if i >= 5:
                    break
        #
        # for output_queue in self._output_queues:
        #     for i in range(20):
        #         output_queue.put({"DONE": True})


class SymbolWorker:
    def __init__(self, url, **kwargs):
        self._url = url

    @staticmethod
    def _extract_company_symbols(html_body):
        soup = BeautifulSoup(html_body, "html.parser")
        table = soup.find(id="constituents")
        for row in table.find_all("tr")[1:]:
            symbol = row.find("td").text.strip("\n")
            yield {"symbol": symbol}

    def get_snp_500_companies(self):
        response = requests.get(self._url)
        if response.status_code != 200:
            print("Could not get S&P 500 companies.")
            return {}
        yield from self._extract_company_symbols(response.text)


if __name__ == "__main__":

    symbolsWorker = SymbolWorker()
    symbol_list = []
    for symbol_ in symbolsWorker.get_snp_500_companies():
        symbol_list.append(symbol_)

    print(len(symbol_list), symbol_list[-1])
