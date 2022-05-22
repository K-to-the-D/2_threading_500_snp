import threading
import os
from queue import Empty

from sqlalchemy import create_engine
from sqlalchemy.sql import text


class PostgresMasterScheduler(threading.Thread):
    def __init__(self, input_queue, **kwargs):
        if "output_queues" in kwargs:
            kwargs.pop("output_queues")
        super(PostgresMasterScheduler, self).__init__(**kwargs)
        self._input_queue = input_queue
        self.start()

    def run(self):
        while True:
            try:
                input_kwargs = self._input_queue.get(timeout=10)
            except Empty as e:
                print("Timeout reached in postgres scheduler. Stop Thread.", e)
                break
            if input_kwargs.get("DONE") is True:
                break
            # print("received: ", input_kwargs)
            symbol, price, extracted_time = input_kwargs.values()
            pg_worker = PostgresWorker(symbol, price, extracted_time)
            pg_worker.insert_into_db()


class PostgresWorker:
    def __init__(self, symbol, price, extracted_time, *args, **kwargs):
        self._symbol = symbol
        self._price = price
        self._extracted_time = extracted_time

        # todo: delete default values
        self._PG_USER = os.environ.get("PG_USER")
        self._PG_PW = os.environ.get("PG_PW")
        self._PG_HOST = os.environ.get("PG_HOST")
        self._PG_DB = os.environ.get("PG_DB")

        # instantiate engine object that enables DB connection later on, via engine.connect()
        self._engine = create_engine(f'postgresql://{self._PG_USER}:{self._PG_PW}@{self._PG_HOST}/{self._PG_DB}')

    @staticmethod
    def _create_insert_query():
        # sqlalchemy.sql.text allows assigning values to :symbol, :price, :extracted_time
        # this special text format adds protection against injection attacks
        SQL = f"""INSERT INTO prices (symbol, price, extracted_time) VALUES (:symbol, :price, :extracted_time)"""
        # type casting in SQL: e.g.: VALUES (:symbol, :price, CAST(:extracted_time AS timestamp) )
        return SQL

    def insert_into_db(self):
        insert_query = self._create_insert_query()

        # connect to DB & execute query
        with self._engine.connect() as conn:
            try:
                conn.execute(text(insert_query), {"symbol": self._symbol,
                                                  "price": self._price,
                                                  "extracted_time": str(self._extracted_time)})
            except Exception as e:
                print("ERROR: insert_into_db:", e)

    def execute(self):
        return self.insert_into_db()
