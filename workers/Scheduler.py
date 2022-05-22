import threading


class Scheduler(threading.Thread):
    def __init__(self, worker, input_values=None, output_queues=None, **kwargs):
        super(Scheduler, self).__init__(**kwargs)
        self._Worker = worker
        self._input_values = self._set_input_values(input_values)
        self._output_queues = self._set_output_queues(output_queues)
        self.start()

    @staticmethod
    def _set_input_values(input_values):
        return input_values if isinstance(input_values, dict) else {}

    @staticmethod
    def _set_output_queues(output_queues):
        if not output_queues:
            return []
        return output_queues if isinstance(output_queues, list) else [output_queues]

    def _write_output_to_queues(self, output_kwargs):
        for output_queue in self._output_queues:
            output_queue.put(output_kwargs)

    def execute_worker(self):
        worker = self._Worker(**self._input_values)
        output_kwargs = worker.execute()
        return output_kwargs

    def run(self):
        output_kwargs = self.execute_worker()
        self._write_output_to_queues(output_kwargs)
