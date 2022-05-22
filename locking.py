import threading
import time

# THREADING VERY GOOD FOR NETWORK I/O
# as waiting for network roundtrips is needed... here NOT use Lock!
# network waiting acts as some sort of blocking operation...

# if threading is used for local processing, locking is helpful

counter = 0


lock = threading.Lock()
# initiate lock to prevent race conditions
# lock.acquire()
# lock.release()


def increment():
    global counter
    for i in range(10**6):  # 10, 1000, race condition does not occur, but with 1000000 it occurs, hence we
                            # increased the number to ramp up the chances of entering a race condition for testing
        lock.acquire()
        counter += 1
        lock.release()
        # cleaner to use is context manager, as it releases automatically. but in testing takes little bit longer?!
        with lock:
            counter += 1


threads = []
for i in range(4):
    x = threading.Thread(target=increment)
    threads.append(x)

start_time = time.time()
for t in threads:
    t.start()

for t in threads:
    t.join()

print("Computation time:", time.time() - start_time)
print("Counter value:", counter)

# RACE CONDITION:
# object is accessed by multiple objects/processes/threads at the same time, they manipulate the value
# and this leads to errors.
# Example below, shows, that instead of counter being increased by +2, it gets only increased by +1
# to prevent this use threading.Lock() !!

# RACE CONDITION:
# counter = x
# thread  <- counter = x
# thread2 <- counter = x
# thread  -> counter = counter+1 -> counter = x+1
# thread2 -> counter = counter+1 -> counter = x+1

# print(counter) out: 1   .... instead of 2


# LOCKING:
# counter = x
# thread1 acquire Lock
# thread1 <- counter = x
# thread2 has to wait ... to acquire lock.
# thread1 -> counter = counter+1 -> counter = x+1
# thread1 release Lock

# thread2 acquire Lock
# thread2 <- coutner = x
# thread2 -> counter = counter+1 -> counter = x+1 +1
# thread2 release Lock

# print(counter) out: 2

