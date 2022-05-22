import importlib
from multiprocessing import Queue
import threading
import time

import yaml


class YamlPipelineExecutor(threading.Thread):
    def __init__(self, pipeline_location):
        super(YamlPipelineExecutor, self).__init__()
        self._pipeline_location = pipeline_location
        self._queues = {}
        self._workers = {}
        # assumption: each queue is consumed only from one worker class(!) with varying amount of instances
        self._queue_consumers = {}  # queue_name with number of workers that consume from it
        self._output_queues_of_worker = {}  # worker_name with it's output queues == downstream queues

    def _load_pipeline(self):
        with open(self._pipeline_location, "r") as pipeline:
            self._yaml_data = yaml.safe_load(pipeline)

    def _initialize_queues(self):
        for queue in self._yaml_data["queues"]:
            queue_name = queue["name"]
            self._queues[queue_name] = Queue()

    def _initialize_workers(self):
        for worker in self._yaml_data["workers"]:
            # import specific worker location with importlib: workers.FinanceWorker
            # getattr enables to dynamically import an attribute from the module...
            # here it takes class FinancePriceScheduler from workers.FinanceWorker module
            # behaves like: from workers.FinanceWorker import FinancePriceScheduler
            # also works for class attributes like:
            # FinancePriceScheduler._input_queues -> getattr(FinancePriceScheduler, "_input_queues")
            WorkerClass = getattr(importlib.import_module(worker["location"]), worker["class"])

            input_queue = worker.get("input_queue")
            output_queues = worker.get("output_queues", [])
            worker_name = worker["name"]
            num_instances = worker.get("instances", 1)

            self._output_queues_of_worker[worker_name] = output_queues
            if input_queue is not None:
                self._queue_consumers[input_queue] = num_instances

            init_params = {
                "input_queue": self._queues.get(input_queue),
                "output_queues": [self._queues.get(output_queue) for output_queue in output_queues if output_queue],
            }
            if worker.get("input_values"):
                init_params["input_values"] = worker["input_values"]

            # create number of specified workers
            self._workers[worker_name] = [WorkerClass(**init_params) for _ in range(num_instances)]

    def _join_workers(self):
        for worker_name in self._workers:
            for worker_thread in self._workers[worker_name]:
                worker_thread.join()

    def process_pipeline(self):
        self._load_pipeline()
        self._initialize_queues()
        self._initialize_workers()
        # self._join_workers()

    def run(self):
        self.process_pipeline()

        while True:
            worker_stats = []
            workers_to_delete = []
            total_workers_alive = 0
            for worker_name in self._workers.keys():

                total_worker_threads_alive = 0
                for worker_thread in self._workers[worker_name]:
                    if worker_thread.is_alive():
                        total_worker_threads_alive += 1

                if total_worker_threads_alive > 0:
                    total_workers_alive += total_worker_threads_alive

                else:
                    for output_queue in self._output_queues_of_worker[worker_name]:
                        number_of_consumers = self._queue_consumers[output_queue]
                        for _ in range(number_of_consumers):
                            self._queues[output_queue].put({"DONE": True})
                    workers_to_delete.append(worker_name)
                worker_stats.append([worker_name, total_worker_threads_alive])

            print(worker_stats)

            queue_stats = [[queue, self._queues[queue].qsize()] for queue in self._queues]
            print("\tqueues:\t", queue_stats)  # qsize() only works in linux! not mac

            if total_workers_alive <= 0:
                break
            for worker_name in workers_to_delete:
                del self._workers[worker_name]
            time.sleep(1)
