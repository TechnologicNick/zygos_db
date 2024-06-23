import gc
import math
import random
from config import Config
from test_base import TestQuery
from test_zygos_db import TestZygosDB
from test_tabix import TestTabix
from test_zygos_db_parallel import TestZygosDBParallel

def draw_samples(positions_per_chromosome: dict[int, list[int]], window_size: int, num_samples: int):
    windows: list[TestQuery] = []

    for _ in range(num_samples):
        chromosome = random.choice(list(positions_per_chromosome.keys()))
        positions = positions_per_chromosome[chromosome]

        assert len(positions) > window_size, f"Chromosome {chromosome} has less positions than window size {window_size}: {len(positions)}"
        
        start_index = random.randint(0, len(positions) - window_size)
        end_index = start_index + window_size - 1

        start = positions[start_index]
        end = positions[end_index]

        windows.append(TestQuery(chromosome, start, end))

    return windows

def run_benchmarks(tests: list[str], window_size: int, num_samples: int, duration: float, warmup: float = 0.0):
    config = Config()

    print("[+] Reading all chromosomes...")
    chromosomes = config.get_all_chromosomes()
    print("[+] Found chromosomes:", chromosomes)

    print("[+] Reading all positions...")
    positions_per_chromosome = { chromosome: config.get_all_positions(chromosome) for chromosome in chromosomes }

    print(f"[+] Drawing {num_samples} samples of size {window_size}...")
    samples = draw_samples(positions_per_chromosome, window_size, num_samples)

    print(f"[+] Drawing {math.ceil(num_samples * warmup / duration)} warmup samples...") if warmup > 0 else None
    warmup_samples = draw_samples(positions_per_chromosome, window_size, math.ceil(num_samples * warmup / duration)) if warmup > 0 else []
    
    # for sample in samples:
    #     print(sample)
    
    test_classes: list[TestZygosDB] = []
    for test in tests:
        if test == "zygos_db":
            test_classes.append(TestZygosDB(config))
        elif test == "tabix":
            test_classes.append(TestTabix(config))
        elif test.startswith("zygos_db_parallel_"):
            num_threads = int(test.split("_")[-1])
            test_classes.append(TestZygosDBParallel(config, num_threads))

    for test in test_classes:
        print(f"[{test.name}] Setting up...")
        test.setup(chromosomes)

    warmup_throughput = dict()
    test_throughput = dict()
    
    for test in test_classes:
        if warmup > 0:
            print(f"[{test.name}] ===== Warming up for {warmup} seconds...")

            try:
                gc.disable()
                gc.collect()
                throughput = test.run(warmup_samples, warmup)
                gc.enable()
                warmup_throughput[test.name] = throughput
            except RuntimeError as e:
                print("ERROR during warmup:", e)
                exit(1)

        print(f"[{test.name}] ===== Running for {duration} seconds...")

        try:
            gc.disable()
            gc.collect()
            throughput = test.run(samples, duration)
            gc.enable()
            test_throughput[test.name] = throughput
        except RuntimeError as e:
            print("ERROR:", e)
            exit(1)

    return (warmup_throughput, test_throughput)

if __name__ == "__main__":
    print(run_benchmarks(tests=["zygos_db_parallel_1", "zygos_db"], window_size=100000, num_samples=100000, duration=10, warmup=0))
    # results = []
    # results.append(run_benchmarks(tests=["zygos_db", "tabix"], window_size=1, num_samples=6000000, duration=60, warmup=10))
    # print(results)
    # results.append(run_benchmarks(tests=["zygos_db", "tabix"], window_size=10, num_samples=6000000, duration=60, warmup=10))
    # print(results)
    # results.append(run_benchmarks(tests=["zygos_db", "tabix"], window_size=100, num_samples=3000000, duration=60, warmup=10))
    # print(results)
    # results.append(run_benchmarks(tests=["zygos_db", "tabix"], window_size=1000, num_samples=1000000, duration=60, warmup=10))
    # print(results)
    # results.append(run_benchmarks(tests=["zygos_db", "tabix"], window_size=10000, num_samples=1000000, duration=60, warmup=10))
    # print(results)
    # results.append(run_benchmarks(tests=["zygos_db", "tabix"], window_size=100000, num_samples=1000000, duration=60, warmup=10))
    # print(results)
