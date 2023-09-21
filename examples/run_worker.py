from rq import Connection, Queue, ForkWorker

if __name__ == '__main__':
    # Tell rq what Redis connection to use
    with Connection():
        q = Queue()
        ForkWorker(q).work()
