from dataclasses import dataclass
from abc import ABC
from config import Config

@dataclass
class TestQuery:
    chromosome: int
    start: int
    end: int

class Test(ABC):
    def __init__(self, config: Config, name: str):
        self.config = config
        self.name = name

    def build(self):
        pass

    def setup(self):
        pass

    def run(self, queries: list[TestQuery], duration: float):
        pass
