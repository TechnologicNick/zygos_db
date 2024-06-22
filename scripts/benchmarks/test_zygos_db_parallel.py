import time

from zygos_db import DatabaseQueryClient

from util import measure_time
from config import Config
from test_base import Test

class TestZygosDBParallel(Test):
    table_indices: dict[int, object] = {}
    parallel_row_reader_s: dict[int, object] = {}

    def __init__(self, config: Config, num_threads: int):
        super().__init__(config, f"ZygosDB (threads={num_threads})")
        self.num_threads = num_threads

    def setup(self, chromosomes: list[int]):
        client = measure_time(lambda: DatabaseQueryClient(self.config.zygos_db_file), f"[{self.name}] Creating client")
        # print(client.header.datasets)

        for chromosome in chromosomes:
            table_index = measure_time(lambda: client.read_table_index(self.config.zygos_db_dataset, chromosome), f"[{self.name}] Reading table index for chromosome {chromosome}")
            # print(table_index)

            parallel_row_reader = table_index.create_query_parallel()
            # print(row_reader)

            self.table_indices[chromosome] = table_index
            self.parallel_row_reader_s[chromosome] = parallel_row_reader

    def run(self, queries, duration):
        total_rows = 0
        completed_queries = 0

        start_time = time.time()

        for query in queries:
            if time.time() - start_time > duration:
                break

            try:
                parallel_row_reader = self.parallel_row_reader_s[query.chromosome]
                rows = parallel_row_reader.query_range(query.start, query.end)
                total_rows += len(rows)
            except Exception as e:
                print(f"[{self.name}] Error executing query {query}: {e}")
                raise e

            completed_queries += 1

        end_time = time.time()

        print(f"[{self.name}] Querying {total_rows} rows took {end_time - start_time} seconds")
        print(f"[{self.name}] Average time per query: {(end_time - start_time) / completed_queries}")
        print(f"[{self.name}] Average rows per second: {total_rows / (end_time - start_time)}")

        if completed_queries >= len(queries):
            raise RuntimeError(f"[{self.name}] Completed all queries, increase num_samples or decrease duration.")

        return total_rows / (end_time - start_time)
