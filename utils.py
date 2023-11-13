import time
from log import logger


def timeit(method):
    def timed(*args, **kwargs):
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()

        logger.info(
            f"{method.__name__} took {end_time - start_time} seconds to complete")
        return result

    return timed
