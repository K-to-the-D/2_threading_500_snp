import time
import threading


def calculate_sum_squares(n):
    sum_squares = 0
    for i in range(n):
        sum_squares += i ** 2

    print(sum_squares)


def sleep_a_little(seconds):
    time.sleep(seconds)


def main():

    calc_start_time = time.time()

    current_threads = []
    for i in range(5):
        maximum_value = (i + 1) * 1000000
        t = threading.Thread(target=calculate_sum_squares, args=(maximum_value, ), daemon=False)
        t.start()
        # t.join()
        current_threads.append(t)
        # calculate_sum_squares(maximum_value)

    for thread in current_threads:
        thread.join()

    print("Calculating sum of squares took:", round(time.time() - calc_start_time, 1))
    sleep_start_time = time.time()

    current_threads = []
    for seconds in range(1, 6):
        t = threading.Thread(target=sleep_a_little, args=(seconds, ), daemon=True)  # daemon=True, daemon threads exist as long as main thread runs
        t.start()                                                                       # hence, if test_threading.py's main thread is finished,
        # t.join()                                               # all spawned threads stop running. as daemons are services in background
        # sleep_a_little(i)
        current_threads.append(t)

    for thread in current_threads[:-2]:
        thread.join()

    print("Calculating sum of sleep took:", round(time.time() - sleep_start_time, 1))


if __name__ == "__main__":
    main()
