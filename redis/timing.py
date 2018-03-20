import logging
import threading
import time
from datetime import datetime, timedelta
from functools import wraps

metrics = logging.getLogger('metrics')


__all__ = ['timing']


class TimingThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.reset()
        self.lock = threading.Lock()
        self.daemon = True

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        end = time.time()
        delta = (end - self.start) * 1000
        with self.lock:
            self.total_time += delta
            self.max_time = max(delta, self.max_time)
            self.n_commands += 1

    def reset(self):
        self.n_commands = 0
        self.total_time = 0
        self.max_time = 0

    def log(self):
        with self.lock:
            avg = float((self.total_time / self.n_commands) if self.n_commands else 0)
            max = float(self.max_time)
            self.reset()
        metrics.info(' '.join([
            f'measure#redis.average={avg:.2}ms',
            f'measure#redis.max={max:.2}ms'
        ]))

    def delay(self):
        now = datetime.now()
        delta = (
            (now + timedelta(minutes=1)).replace(second=30, microsecond=0) -
            now
        )
        return delta.total_seconds()

    def run(self):
        time.sleep(self.delay())
        while True:
            self.log()
            time.sleep(self.delay())


timer = None
if timer is None:
    timer = TimingThread()
    timer.start()


def timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with timer:
            return func(*args, **kwargs)
    return wrapper
