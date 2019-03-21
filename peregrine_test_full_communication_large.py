import multiprocessing as mp
import time

NUM_WORKERS = 32
ITERATIONS = 1000
MANAGER_SIZE = 20000


def create_data_point(shared_data):
    time.sleep(0.001)
    return 1


def worker_func(worker_id, w2t_m_queue, events, t2w_d_manager):
    """
    The task of a worker is to put data points into the message queue and then signal the trainer
    that this worker has finished an iteration. Trainer process will dequeue these data points.
    While trainer is processing the data point, the worker will wait for a trainer signal before it
    goes into another iteration.
    :param worker_id:
    :param w2t_m_queue: A queue workers use to leave data points for the trainer to dequeue.
    :param events: A dictionary containing mp events for child processes to communicate
    :param t2w_d_manager: A mp.Manager that shares a list of data across child processes. The trainer puts data
                        in here for workers to use.
    :return:
    """
    average_iteration_time = 0
    shared_data = []
    for i in range(ITERATIONS):
        data_point = create_data_point(shared_data)
        events["Workers_can_proceed"].clear()
        put_time = time.time()
        w2t_m_queue.put(data_point)
        # Signal trainer that this worker has placed its data point this iteration
        events[worker_id].set()
        # Have worker wait until trainer is done processing this iteration
        events["Workers_can_proceed"].wait()
        # Obtain data trainer has placed into shared manager
        shared_data = t2w_d_manager
        average_iteration_time += (time.time() - put_time)

    average_iteration_time /= ITERATIONS
    print("Worker " + str(worker_id) + " average put time: " + str.format('{0:.6f}', (average_iteration_time*1000)) + "ms")


def start_workers(w2t_m_queue, events, t2w_d_manager):
    """
    This function create child processes (workers) that will gather and send data.
    :param w2t_m_queue: A queue workers use to leave data points for the trainer to dequeue.
    :param events: A dictionary containing mp events for child processes to communicate.
    :param t2w_d_manager: A mp.Manager that shares a list of data across child processes. The trainer puts data
                        in here for workers to use.
    :return: A list of all child processes (workers).
    """
    start_time = time.time()
    print("*********************************************************************")
    print("Initializing workers...")
    workers = []
    for i in range(NUM_WORKERS):
        worker = mp.Process(target=worker_func, args=(i, w2t_m_queue, events, t2w_d_manager))
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
    time.sleep(0.001)
    return [1 for i in range(MANAGER_SIZE)]


def trainer_func(w2t_m_queue, events, t2w_d_manager):
    """
    Trainer waits for a iteration batch to be queued. The trainer then dequeues the batch of data points from the
    message queue. Once it is done processing the data batch, it signals the workers to proceed.
    :param w2t_m_queue: A queue workers use to leave data points for the trainer to dequeue.
    :param events: A dictionary containing mp events for child processes to communicate.
    :param t2w_d_manager: A mp.Manager that shares a list of data across child processes.
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
            data_point = w2t_m_queue.get()
            data.append(data_point)
        message_to_share = do_something_with_data(data)
        # Put data into manager
        t2w_d_manager = message_to_share
        # Signal to workers they are allow to proceed
        events["Workers_can_proceed"].set()
        average_iteration_time += time.time() - dequeue_time

    average_iteration_time /= ITERATIONS*NUM_WORKERS
    print("-------------------------------------")
    print("Trainer total dequeue time: " + str.format('{0:.6f}', (time.time() - trainer_start_time)) + "ms")
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
    worker_to_trainer_message_queue = mp.Queue()
    trainer_to_worker_data_manager = mp.Manager().list()
    workers = start_workers(worker_to_trainer_message_queue, events, trainer_to_worker_data_manager)
    trainer = mp.Process(target=trainer_func, args=(worker_to_trainer_message_queue, events, trainer_to_worker_data_manager))
    trainer.start()
    trainer.join()
    terminate_workers(workers)
    print("Total test elapsed time: " + str.format('{0:.6f}', (time.time() - total_time_start)*1000) + "ms")


if __name__ == '__main__':
    run()
