import multiprocessing as mp
import time

NUM_WORKERS = 32
ITERATIONS = 1000

def worker_func(worker_id, message_queue):
    """
    The task of a worker is to put data points into the message queue. Trainer process
    will dequeue these data points.
    :param worker_id:
    :param message_queue:
    :return:
    """
    average_put_time = 0
    for i in range(ITERATIONS):
        put_time = time.time()
        message_queue.put(i)
        average_put_time += time.time() - put_time
    average_put_time /= ITERATIONS
    print("Worker " + str(worker_id) + " average put time: " + str(average_put_time))


def start_workers(message_queue):
    """
    This function create child processes (workers) that will gather and send data.
    :param message_queue:
    :return: A list of all child processes (workers).
    """
    startTime = time.time()
    print("*********************************************************************")
    print("Initializing workers...")
    workers = []
    for i in range(NUM_WORKERS):
        worker = mp.Process(target=worker_func, args=(i, message_queue))

        worker.start()
        workers.append(p)
    print("Workers initialized.")
    print("Initialization time elapsed:   " + str.format('{0:.3f}', (time.time() - startTime)/1000) + "ms")
    print("*********************************************************************")
    return workers


def terminate_workers(workers):
    """
    This function terminates all workers.
    :param workers:
    :return:
    """
    print("*********************************************************************")
    print("Terminating collectors...")
    startTime = time.time()
    for worker in workers:
        worker.terminate()
        # if not p.is_alive():
        worker.join(timeout=0.001)
    print("Collectors terminated.")
    print("Termination time elapsed:   " + str.format('{0:.3f}', (time.time() - startTime)/1000) + "ms")
    print("*********************************************************************")


def trainer_func(message_queue):
    """
    Trainer dequeues data points from the message queue.
    :param message_queue:
    :return:
    """
    average_dequeue_time = 0
    trainer_start_time = time.time()
    data = []
    for i in range(ITERATIONS*NUM_WORKERS):
        dequeue_time = time.time()
        data_point = message_queue.get()
        data.append(data_point)
        average_dequeue_time += time.time() - dequeue_time
    average_dequeue_time /= ITERATIONS*NUM_WORKERS
    print("Trainer total dequeue time: " + str.format('{0:.3f}', (time.time() - trainer_start_time)/1000) + "ms" )
    print("Trainer average dequeue time: " + str.format('{0:.3f}', average_dequeue_time) + "ms")




def run():
    message_queue = mp.Queue()
    workers = start_workers(message_queue)
    trainer = mp.Process(target=trainer_func, args=(message_queue,))
    trainer.start()
    trainer.join()
    terminate_workers(workers)


if __name__ == '__main__':
    run()