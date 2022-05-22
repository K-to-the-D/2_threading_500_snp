import threading
from queue import Empty


class Scheduler(threading.Thread):
    def __init__(self, worker, input_queue=None, output_queues=None, timeout=10, **kwargs):
        super(Scheduler, self).__init__(**kwargs)
        self._Worker = worker
        self._input_queue = input_queue
        self._output_queues = self._set_output_queues(output_queues)
        self._timeout = timeout
        self.start()

    @staticmethod
    def _set_output_queues(output_queues):
        if not output_queues:
            return []
        return output_queues if isinstance(output_queues, list) else [output_queues]

    def get_input_from_queue(self):
        try:
            # blocking operation, it blocks loop until value is returned
            input_kwargs = self._input_queue.get(timeout=self._timeout)
        except Empty as e:
            print(f"Stop thread. Input queue timeout:{self._timeout} \t {e}")
            input_kwargs = {"STOP": True}
        return input_kwargs

    def write_output_to_queues(self, output_kwargs):
        for output_queue in self._output_queues:
            output_queue.put(output_kwargs)

    def execute_worker(self, input_kwargs):
        worker = self._Worker(**input_kwargs)
        output_kwargs = worker.execute()
        return output_kwargs

    def run(self):
        while True:
            input_kwargs = self.get_input_from_queue()
            if input_kwargs.get("DONE") is True or input_kwargs.get("STOP") is True:
                # self.write_output_to_queues({"DONE": True})
                break
            output_kwargs = self.execute_worker(input_kwargs)
            self.write_output_to_queues(output_kwargs)
