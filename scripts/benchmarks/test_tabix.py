from io import StringIO
import subprocess
import time
import pandas as pd

from config import Config
from test_base import Test

class TestTabix(Test):
    input_files: dict[int, str] = {}

    def __init__(self, config: Config):
        super().__init__(config, "Tabix")

    def setup(self, chromosomes: list[int]):
        for chromosome in chromosomes:
            self.input_files[chromosome] = self.config.get_input_file(chromosome)
        

    def run(self, queries, duration):
        total_rows = 0
        completed_queries = 0

        total_time_querying = 0
        total_time_decoding_string = 0
        total_time_parsing = 0
        total_time_waiting = 0

        start_time = time.time()

        for query in queries:
            time_query_start = time.time()

            if time_query_start - start_time > duration:
                break

            cmd = ["tabix", self.input_files[query.chromosome], f"{query.chromosome}:{query.start}-{query.end}"]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE)

            # print("Running tabix...", cmd)

            contents = process.communicate()[0]

            time_contents_read = time.time()

            string = StringIO(contents.decode("utf-8"))

            time_string_decoded = time.time()

            # C engine does not support the 'sep' parameter:
            # https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas-read-csv
            # df = pd.read_csv(
            #     # process.stdout,
            #     StringIO(process.communicate()[0].decode("utf-8")),
            #     sep="\t",
            #     engine="python",
            #     names=self.config.all_columns,
            #     usecols=self.config.query_columns,
            #     converters=self.config.query_columns,
            # )

            # They lied, it does support it:
            df = pd.read_csv(
                string,
                sep="\t",
                names=self.config.all_columns,
                usecols=self.config.query_columns.keys(),
                converters=self.config.query_columns,
            )

            time_parsed = time.time()

            process.wait()

            time_waited = time.time()

            total_time_querying += time_contents_read - time_query_start
            total_time_decoding_string += time_string_decoded - time_contents_read
            total_time_parsing += time_parsed - time_string_decoded
            total_time_waiting += time_waited - time_parsed
            

            total_rows += df.shape[0]

            completed_queries += 1

        end_time = time.time()

        print(f"[{self.name}] Querying took {total_time_querying} seconds")
        print(f"[{self.name}] Decoding string took {total_time_decoding_string} seconds")
        print(f"[{self.name}] Parsing took {total_time_parsing} seconds")
        print(f"[{self.name}] Waiting took {total_time_waiting} seconds")

        print(f"[{self.name}] Querying {total_rows} rows took {end_time - start_time} seconds")
        print(f"[{self.name}] Average time per query: {(end_time - start_time) / completed_queries}")
        print(f"[{self.name}] Average rows per second: {total_rows / (end_time - start_time)}")

        if completed_queries >= len(queries):
            raise RuntimeError(f"[{self.name}] Completed all queries, increase num_samples or decrease duration.")

        return total_rows

