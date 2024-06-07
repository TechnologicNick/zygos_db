import sys

from structures import Database
from zygos_db import DatabaseQueryClient

def main(file: str):
    db = Database.parse_file(filename=file)
    client = DatabaseQueryClient(file)
    index = client.read_table_index(db.datasets[0].tables[0].offset)
    print(index.get_all())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python cli.py <filename>")
        sys.exit(1)
    main(sys.argv[1])