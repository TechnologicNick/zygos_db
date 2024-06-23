from os.path import join, dirname, abspath
import platform

from zygos_db import DatabaseQueryClient

_dir = dirname(abspath(__file__))

class Config:
    zygos_db_dataset = "alzheimer"
    all_columns = [ "POS", "CHR", "P", "POS_HG38", "RSID", "MAF", "REF", "ALT"]
    query_columns = { "POS": int, "P": float, "RSID": str }

    def __init__(self) -> None:
        if any(platform.win32_ver()):
            self.zygos_db_file = join(_dir, "../../../snpXplorer/Data/databases/snpXplorer.zygosdb")
        else:
            self.zygos_db_file = abspath("/home/nick/snpXplorer.zygosdb")
        pass

    def get_input_file(self, chromosome: int) -> str:
        if any(platform.win32_ver()):
            return join(_dir, f"../../../snpXplorer/Data/databases/Alzheimer_million/chr{chromosome}_Alzheimer_million.txt.gz")
        else:
            return abspath(f"/home/nick/Alzheimer_million/chr{chromosome}_Alzheimer_million.txt.gz")

    def get_all_positions(self, chromosome: int) -> list[int]:
        client = DatabaseQueryClient(self.zygos_db_file)
        table_index = client.read_table_index(self.zygos_db_dataset, chromosome)
        row_reader = table_index.create_query()

        rows = row_reader.query_range(table_index.min_position, table_index.max_position)
        return [row[0] for row in rows]

    def get_all_chromosomes(self) -> list[int]:
        client = DatabaseQueryClient(self.zygos_db_file)
        dataset = [dataset for dataset in client.header.datasets if dataset.name == self.zygos_db_dataset][0]
        return [table.chromosome for table in dataset.tables]
    
    def get_compression_algorithm(self) -> str:
        client = DatabaseQueryClient(self.zygos_db_file)
        dataset = [dataset for dataset in client.header.datasets if dataset.name == self.zygos_db_dataset][0]
        return dataset.compression_algorithm

