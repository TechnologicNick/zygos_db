import gc
import json
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

    compression_algorithm = config.get_compression_algorithm()
    print("[+] Compression algorithm:", compression_algorithm)

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

    return (compression_algorithm, test_throughput)

if __name__ == "__main__":
    window_size = 100000
    num_samples = 100000
    duration = 10
    warmup = 0
    (compression_algorithm, results_parallel) = run_benchmarks(tests=[
        "zygos_db_parallel_1",
        "zygos_db_parallel_2",
        "zygos_db_parallel_3",
        "zygos_db_parallel_4",
        "zygos_db_parallel_5",
        "zygos_db_parallel_6",
        "zygos_db_parallel_7",
        "zygos_db_parallel_8",
        "zygos_db_parallel_9",
        "zygos_db_parallel_10",
        "zygos_db_parallel_11",
        "zygos_db_parallel_12",
        "zygos_db_parallel_13",
        "zygos_db_parallel_14",
        "zygos_db_parallel_15",
        "zygos_db_parallel_16",
        "zygos_db_parallel_17",
        "zygos_db_parallel_18",
        "zygos_db_parallel_19",
        "zygos_db_parallel_20",
        "zygos_db_parallel_21",
        "zygos_db_parallel_22",
        "zygos_db_parallel_23",
        "zygos_db_parallel_24",
        "zygos_db_parallel_25",
        "zygos_db_parallel_26",
        "zygos_db_parallel_27",
        "zygos_db_parallel_28",
        "zygos_db_parallel_29",
        "zygos_db_parallel_30",
        "zygos_db_parallel_31",
        "zygos_db_parallel_32",
    ], window_size=window_size, num_samples=num_samples, duration=duration, warmup=warmup)
    output = json.dumps({
        "window_size": window_size,
        "num_samples": num_samples,
        "duration": duration,
        "warmup": warmup,
        "results": results_parallel,
    }, indent=4)
    print(output)
    with open(f"./results/parallel/{compression_algorithm}.json", "w+") as f:
        f.write(output)

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
