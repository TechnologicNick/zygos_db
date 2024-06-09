import time

from zygos_db import DatabaseQueryClient

import config as config
from util import measure_time

def run(config=config):

    client = measure_time(lambda: DatabaseQueryClient(config.zygos_db_file), "Creating client")
    print(client.header.datasets)

    table_index = measure_time(lambda: client.read_table_index(config.zygos_db_dataset, config.query_chromosome), "Reading table index")
    print(table_index)

    row_reader = measure_time(lambda: table_index.create_query(), "Creating row reader")
    print(row_reader)

    rows = measure_time(lambda: row_reader.query_range(config.query_start, config.query_end), "Querying rows")
    print(rows[0:5])
    print("...")
    print(rows[-5:])

    print(len(rows))

    return rows

if __name__ == "__main__":
    start = time.time()
    rows = run()
    print("Total time taken:", time.time() - start)
