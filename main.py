import time
import os

from YamlPipelineExecutor import YamlPipelineExecutor


# MAIN THREAD
def main():
    pipeline_location = os.environ.get("PIPELINE_LOCATION")
    if pipeline_location is None:
        print("Pipeline location not defined")
        exit(1)
    start_time = time.time()

    yaml_pipeline_executor = YamlPipelineExecutor(pipeline_location=pipeline_location)
    yaml_pipeline_executor.start()  # Threading.thread.start() calls .run()
    yaml_pipeline_executor.join()  # our run() func only exits after all workers are done, hence we can use .join() to
                                   # measure computation time
    print("Extracting time:", round(time.time() - start_time, 1))


if __name__ == "__main__":
    main()
