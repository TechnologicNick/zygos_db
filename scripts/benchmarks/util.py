from typing import Callable, TypeVar
from time import time

T = TypeVar("T")

def measure_time(func: Callable[..., T], description: str, *args, **kwargs) -> T:
    start = time()
    result = func(*args, **kwargs)
    print(f"{description}:", time() - start)
    return result