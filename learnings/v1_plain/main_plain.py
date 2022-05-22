import time

from learnings.v1_plain.workers import SymbolsWorker
from learnings.v1_plain.workers.FinanceWorker_plain import FinancePriceWorker


def main():
    start_time = time.time()

    # scrapes wiki to get S&P 500 stock symbols
    symbols_worker = SymbolsWorker()
    current_workers = []

    for symbol in symbols_worker.get_snp_500_companies():
        finance_price_worker = FinancePriceWorker(symbol=symbol)
        current_workers.append(finance_price_worker)

    for thread in current_workers:
        thread.join()

    print("Extracting time:", round(time.time() - start_time, 1))


if __name__ == "__main__":
    main()
