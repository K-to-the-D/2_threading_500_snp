import time
from multiprocessing import Queue

from workers.SymbolWorker import SymbolWorker
from workers.FinanceWorker import FinancePriceScheduler, FinancePriceWorker
from workers.PostgresWorker import PostgresMasterScheduler, PostgresWorker
from workers.Scheduler import Scheduler


# MAIN THREAD
def main():
    symbol_queue = Queue()
    postgres_queue = Queue()
    start_time = time.time()

    # scrapes wiki to get S&P 500 stock symbols
    symbols_worker = SymbolWorker()

    # UPSTREAM WORKER
    # list contains all threads of type FinancePriceSchedulers
    finance_price_scheduler_threads = []
    num_finance_price_workers = 4
    for i in range(num_finance_price_workers):
        # Scheduler is a Thread(!) that has while-loop that waits for symbol_queue elements and requests them
        finance_price_thread = Scheduler(worker=FinancePriceWorker,
                                         input_queue=symbol_queue,
                                         output_queues=[postgres_queue])
        # finance_price_scheduler = FinancePriceScheduler(input_queue=symbol_queue, output_queue=[postgres_queue])
        finance_price_scheduler_threads.append(finance_price_thread)

    # DOWNSTREAM WORKER
    postgres_scheduler_threads = []
    num_postgres_workers = 2
    for i in range(num_postgres_workers):
        postgres_thread = Scheduler(worker=PostgresWorker,
                                    input_queue=postgres_queue)
        # postgres_scheduler = PostgresMasterScheduler(input_queue=postgres_queue)
        postgres_scheduler_threads.append(postgres_thread)

    for i, symbol in enumerate(symbols_worker.get_snp_500_companies()):
        symbol_queue.put(symbol)
        if i >= 5:
            break

    # put DONE in queue for each thread. otherwise would run forever as threads are joined as well as not daemons
    for i in range(len(finance_price_scheduler_threads)):
        symbol_queue.put({"DONE": True})

    for thread in finance_price_scheduler_threads:
        thread.join()

    for thread in postgres_scheduler_threads:
        thread.join()

    print("Extracting time:", round(time.time() - start_time, 1))


if __name__ == "__main__":
    # main()
    import yaml
    with open('pipelines/symbol_finance_scraper_pipeline.yaml', "r") as infile:
        yaml_data = yaml.safe_load(infile)
    print(yaml_data.keys())
