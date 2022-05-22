import requests
from bs4 import BeautifulSoup


class SymbolsWorker:
    def __init__(self):
        self._url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    @staticmethod
    def _extract_company_symbols(html_body):
        soup = BeautifulSoup(html_body, "html.parser")
        table = soup.find(id="constituents")
        for row in table.find_all("tr")[1:]:
            symbol = row.find("td").text.strip("\n")
            yield symbol

    def get_snp_500_companies(self):
        response = requests.get(self._url)
        if response.status_code != 200:
            print("Could not get S&P 500 companies.")
            return []
        yield from self._extract_company_symbols(response.text)


if __name__ == "__main__":

    symbolsWorker = SymbolsWorker()
    symbol_list = []
    for symbol_ in symbolsWorker.get_snp_500_companies():
        symbol_list.append(symbol_)

    print(len(symbol_list), symbol_list[-1])
