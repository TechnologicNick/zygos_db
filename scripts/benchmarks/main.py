import random
from config import Config
from test_base import TestQuery
from test_zygos_db import TestZygosDB
from test_tabix import TestTabix

def draw_samples(positions_per_chromosome: dict[int, list[int]], window_size: int, num_samples: int):
    windows: list[TestQuery] = []

    for _ in range(num_samples):
        chromosome = random.choice(list(positions_per_chromosome.keys()))
        positions = positions_per_chromosome[chromosome]
        
        start_index = random.randint(0, len(positions) - window_size)
        end_index = start_index + window_size

        start = positions[start_index]
        end = positions[end_index]

        windows.append(TestQuery(chromosome, start, end))

    return windows

def run_benchmarks(tests: list[str], window_size: int, num_samples: int):
    config = Config()

    print("[+] Reading all chromosomes...")
    chromosomes = config.get_all_chromosomes()
    print("[+] Found chromosomes:", chromosomes)

    print("[+] Reading all positions...")
    positions_per_chromosome = { chromosome: config.get_all_positions(chromosome) for chromosome in chromosomes }

    print(f"[+] Drawing {num_samples} samples of size {window_size}...")
    samples = draw_samples(positions_per_chromosome, window_size, num_samples)
    
    # for sample in samples:
    #     print(sample)
    
    test_classes: list[TestZygosDB] = []
    for test in tests:
        if test == "zygos_db":
            test_classes.append(TestZygosDB(config))
        elif test == "tabix":
            test_classes.append(TestTabix(config))

    for test in test_classes:
        print(f"[{test.name}] Setting up...")
        test.setup(chromosomes)
    
    for test in test_classes:
        print(f"[{test.name}] Running...")
        test.run(samples)

    pass

if __name__ == "__main__":
    run_benchmarks(tests=["tabix"], window_size=100000, num_samples=1000)
