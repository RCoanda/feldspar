import random
import time

from line_profiler import LineProfiler


def timerfunc(func):
    """A timer decorator.
    """

    def function_timer(*args, **kwargs):
        """
        A nested function for timing other functions
        """
        start = time.time()
        value = func(*args, **kwargs)
        end = time.time()
        runtime = end - start
        msg = "The runtime for {func} took {time} seconds to complete"
        print(msg.format(func=func.__name__,
                         time=runtime))
        return value

    return function_timer


def lineprofile(func):
    """A line_profiler decorator.
    """

    def function_profiler(*args, **kwargs):
        """
        A nested function for profiling other functions.
        """
        lp = LineProfiler()
        lp_wrapper = lp(func)
        value = lp_wrapper(*args, **kwargs)
        lp.print_stats()
        return value

    return function_profiler
