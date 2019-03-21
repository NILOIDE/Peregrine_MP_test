import multiprocessing as mp
import time

NUM_WORKERS = 32
ITERATIONS = 1000


def create_data_point():
    time.sleep(0.00001)
    return 1


def worker_func(worker_id, message_queue, events):
    """
    The task of a worker is to put data points into the message queue and then signal the trainer
    that this worker has finished an iteration. Trainer process will dequeue these data points.
    While trainer is processing the data point, the worker will wait for a trainer signal before it
    goes into another iteration.
    :param worker_id:
    :param message_queue:
    :param events: A dictionary containing mp events for child processes to communicate
    :return:
    """
    average_iteration_time = 0
    for i in range(ITERATIONS):
        data_point = create_data_point()
        events["Workers_can_proceed"].clear()
        put_time = time.time()
        message_queue.put(data_point)
        # Signal trainer that this worker has placed its data point this iteration
        events[worker_id].set()
        # Have worker wait until trainer is done processing this iteration
        events["Workers_can_proceed"].wait()

        average_iteration_time += (time.time() - put_time)

    average_iteration_time /= ITERATIONS
    print("Worker " + str(worker_id) + " average put time: " + str.format('{0:.6f}', (average_iteration_time*1000)) + "ms")


def start_workers(message_queue, events):
    """
    This function create child processes (workers) that will gather and send data.
    :param message_queue:
    :param events: A dictionary containing mp events for child processes to communicate
    :return: A list of all child processes (workers).
    """
    start_time = time.time()
    print("*********************************************************************")
    print("Initializing workers...")
    workers = []
    for i in range(NUM_WORKERS):
        worker = mp.Process(target=worker_func, args=(i, message_queue, events))
        worker.start()
        workers.append(worker)
    print("Workers initialized.")
    print("Initialization time elapsed:   " + str.format('{0:.6f}', (time.time() - start_time)*1000) + "ms")
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
    start_time = time.time()
    for worker in workers:
        worker.terminate()
        worker.join(timeout=0.001)
    print("Collectors terminated.")
    print("Termination time elapsed:   " + str.format('{0:.6f}', (time.time() - start_time)*1000) + "ms")
    print("*********************************************************************")


def do_something_with_data(data):
    time.sleep(0.00001)


def trainer_func(message_queue, events):
    """
    Trainer waits for a iteration batch to be queued. The trainer then dequeues the batch of data points from the
    message queue. Once it is done processing the data batch, it signals the workers to proceed.
    :param message_queue:
    :param events: A dictionary containing mp events for child processes to communicate.
    :return:
    """
    average_iteration_time = 0
    trainer_start_time = time.time()
    data = []
    for i in range(ITERATIONS):
        # Wait for all workers to send their ready signals
        for worker_num in range(NUM_WORKERS):
            events[worker_num].wait()
            events[worker_num].clear()
        dequeue_time = time.time()
        # Dequeue batch of data from message queue and process it
        for _ in range(NUM_WORKERS):
            data_point = message_queue.get()
            data.append(data_point)
            do_something_with_data(data)
        print(i)
        # Signal to workers they are allow to proceed
        events["Workers_can_proceed"].set()
        average_iteration_time += time.time() - dequeue_time

    average_iteration_time /= ITERATIONS*NUM_WORKERS
    print("-------------------------------------")
    print("Trainer total dequeue time: " + str.format('{0:.6f}', (time.time() - trainer_start_time)*1000) + "ms")
    print("Trainer average dequeue time: " + str.format('{0:.6f}', average_iteration_time*1000) + "ms")
    print("-------------------------------------")


def create_events():
    """
    Create events for child processes to communicate between each other. Each worker will use one event to communicate
    with the trainer, and the trainer will have one event to communicate to all workers at once.
    :return: A dictionary containing all events.
    """
    events = {}
    events["Workers_can_proceed"] = mp.Event()
    for i in range(NUM_WORKERS):
        events[i] = mp.Event()
    return events


def run():
    total_time_start = time.time()
    events = create_events()
    message_queue = mp.Queue()
    workers = start_workers(message_queue, events)
    trainer = mp.Process(target=trainer_func, args=(message_queue, events))
    trainer.start()
    trainer.join()
    terminate_workers(workers)
    print("Total test elapsed time: " + str.format('{0:.6f}', (time.time() - total_time_start)*1000) + "ms")


if __name__ == '__main__':
    run()
