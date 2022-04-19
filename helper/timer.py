import time

class timer(object):
    def __init__(self, func):
        self.func = func
    
    def __call__(self, *args, **kwargs):
        tic = time.time()
        result = self.func(*args, **kwargs)
        toc = time.time()
        time_taken = toc - tic
        print(f'Execution time of "{self.func.__name__}" function {time_taken}s')
        return result