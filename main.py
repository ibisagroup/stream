import threading

from events.data import run as run_data
from events.stoppage import run as run_stoppage
from events.threshold import run as run_threshold


if __name__ == "__main__":
    threads = list()

    x = threading.Thread(target=run_data, args=())
    threads.append(x)
    x.start()

    x = threading.Thread(target=run_stoppage, args=())
    threads.append(x)
    x.start()

    x = threading.Thread(target=run_threshold, args=())
    threads.append(x)
    x.start()

    for index, thread in enumerate(threads):
        thread.join()