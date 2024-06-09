import sys
import time

from structures import Database
from zygos_db import DatabaseQueryClient

def main(file: str):
    db = Database.parse_file(filename=file)
    client = DatabaseQueryClient(file)

    query_start = 0
    query_end = 1000000000000

    index = client.read_table_index("alzheimer", 2)
    print(index.get_all())
    (_, start_offset) = index.get_range(query_start, query_end)[0]
    row_reader = index.create_query()

    time_start = time.time()
    print(len(row_reader.deserialize_range(start_offset, index.index_start_offset, query_end)))
    time_end = time.time()
    print("Time taken:", time_end - time_start)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python cli.py <filename>")
        sys.exit(1)
    main(sys.argv[1])