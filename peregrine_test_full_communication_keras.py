import multiprocessing as mp
import time
import numpy as np
import keras
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' #This suppresses tensorflow AVX warnings


NUM_WORKERS = 32
ITERATIONS = 1000
NETWORK_INPUT_SIZE = 100
NETWORK_OUTPUT_SIZE = 2


def create_neural_network():
    """
    Create a neural network using Keras that the worker will use to process data.
    :return: The neural network
    """
    network_input = keras.layers.Input((NETWORK_INPUT_SIZE,))
    network_layer = keras.layers.Dense(100, kernel_initializer='random_uniform', activation='tanh')(network_input)
    network_layer = keras.layers.Dense(100, kernel_initializer='random_uniform', activation='tanh')(network_layer)
    network_output = keras.layers.Dense(NETWORK_OUTPUT_SIZE, kernel_initializer='random_uniform', activation='linear')(network_layer)
    network = keras.models.Model(inputs=network_input, outputs=network_output)
    network.compile(loss="mse", optimizer="Adam")
    return network


def create_data_point(worker_nn):
    """
    Use worker's neural network to create a data point. A data point is the network's input and output.
    :param worker_nn:
    :return:
    """
    data_point = np.random.rand(1, NETWORK_INPUT_SIZE)
    _ = worker_nn.predict(data_point)
    return data_point


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
    worker_nn = create_neural_network()
    iteration_time = time.time()
    for i in range(ITERATIONS):
        data_point = create_data_point(worker_nn)
        events["Workers_can_proceed"].clear()
        w2t_m_queue.put(data_point)
        # Signal trainer that this worker has placed its data point this iteration
        events[worker_id].set()
        average_iteration_time += (time.time() - iteration_time)
        # Have worker wait until trainer is done processing this iteration
        events["Workers_can_proceed"].wait()
        iteration_time = time.time()
        # Obtain data trainer has placed into shared manager (data is weights of network)
        shared_data = t2w_d_manager[0]
        worker_nn.set_weights(shared_data)

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


def do_something_with_data(data, trainer_nn):
    """
    Network is trained.
    :param data:
    :param trainer_nn:
    :return: The trained network's weights, ready to be sent to the workers.
    """
    inputs = np.array(data)
    targets = np.ones((NUM_WORKERS, NETWORK_OUTPUT_SIZE))
    trainer_nn.train_on_batch(inputs, targets)
    network_weights = trainer_nn.get_weights()
    return network_weights


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
    trainer_nn = create_neural_network()
    t2w_d_manager.append(trainer_nn.get_weights())
    for i in range(ITERATIONS):
        # Wait for all workers to send their ready signals
        for worker_num in range(NUM_WORKERS):
            events[worker_num].wait()
            events[worker_num].clear()
        iteration_time = time.time()
        data = []
        # Dequeue batch of data from message queue and process it
        for _ in range(NUM_WORKERS):
            data_point = w2t_m_queue.get()
            data.append(data_point[0])
        message_to_share = do_something_with_data(data, trainer_nn)
        # Put weight data into manager
        t2w_d_manager[0] = message_to_share
        # Signal to workers they are allow to proceed
        events["Workers_can_proceed"].set()
        average_iteration_time += time.time() - iteration_time

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
    """
    The program consists of one child process that trains (trainer) and n amount of child processes that collect
    data (workers). Events are used to synchronize the workers and trainer such that while the trainer trains, the
    workers wait, and vice versa. Workers gather data in parallel. The workers place the data into a shared message
    queue, which the trainer dequeues from. After the trainer finishes one training iteration, it places the weights
    into the shared manager list, which the workers can access and update their networks.
    :return:
    """
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
