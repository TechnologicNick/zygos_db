from os.path import join, dirname, abspath

query_chromosome = 2
input_file = join(dirname(abspath(__file__)), f"../../../snpXplorer/Data/databases/Alzheimer_million/chr{query_chromosome}_Alzheimer_million.txt.gz")
zygos_db_file = join(dirname(abspath(__file__)), f"../../../snpXplorer/Data/databases/snpXplorer.zygosdb")
zygos_db_dataset = "alzheimer"
all_columns = [ "POS", "CHR", "P", "POS_HG38", "RSID", "MAF", "REF", "ALT"]
query_columns = { "POS": int, "P": float, "RSID": str }
query_start = 0
query_end = 249172500