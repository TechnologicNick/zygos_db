import time

from zygos_db import DatabaseQueryClient

from util import measure_time
from config import Config
from test_base import Test

class TestZygosDB(Test):
    table_indices: dict[int, object] = {}
    row_readers: dict[int, object] = {}

    def __init__(self, config: Config):
        super().__init__(config, "ZygosDB")

    def setup(self, chromosomes: list[int]):
        client = measure_time(lambda: DatabaseQueryClient(self.config.zygos_db_file), f"[{self.name}] Creating client")
        # print(client.header.datasets)

        for chromosome in chromosomes:
            table_index = measure_time(lambda: client.read_table_index(self.config.zygos_db_dataset, chromosome), f"[{self.name}] Reading table index for chromosome {chromosome}")
            # print(table_index)

            row_reader = table_index.create_query()
            # print(row_reader)

            self.table_indices[chromosome] = table_index
            self.row_readers[chromosome] = row_reader

    def run(self, queries, duration):
        total_rows = 0
        completed_queries = 0

        start_time = time.time()

        for query in queries:
            if time.time() - start_time > duration:
                break

            row_reader = self.row_readers[query.chromosome]
            rows = row_reader.query_range(query.start, query.end)
            total_rows += len(rows)

            completed_queries += 1

        end_time = time.time()

        print(f"[{self.name}] Querying {total_rows} rows took {end_time - start_time} seconds")
        print(f"[{self.name}] Average time per query: {(end_time - start_time) / completed_queries}")
        print(f"[{self.name}] Average rows per second: {total_rows / (end_time - start_time)}")

        if completed_queries >= len(queries):
            raise RuntimeError(f"[{self.name}] Completed all queries, increase num_samples or decrease duration.")

        return total_rows
