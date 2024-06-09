from io import StringIO
import subprocess
import time
import pandas as pd

import config as config
from util import measure_time

def run(config=config):
    cmd = ["tabix", config.input_file, f"{config.query_chromosome}:{config.query_start}-{config.query_end}"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    print("Running tabix...", cmd)

    # C engine does not support the 'sep' parameter:
    # https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas-read-csv
    # df = pd.read_csv(
    #     # process.stdout,
    #     StringIO(process.communicate()[0].decode("utf-8")),
    #     sep="\t",
    #     engine="python",
    #     names=config.all_columns,
    #     usecols=config.query_columns,
    #     converters={"POS": int, "P": float, "RSID": str},
    # )

    # They lied, it does support it:
    contents = measure_time(lambda: process.communicate()[0], "Reading tabix output")
    string = measure_time(lambda: contents.decode("utf-8"), "Decoding tabix output")
    df = measure_time(lambda: pd.read_csv(
            StringIO(string),
            sep="\t",
            names=config.all_columns,
            usecols=config.query_columns.keys(),
            converters=config.query_columns,
        ),
        "Parsing tabix output"
    )

    process.wait()

    return df

if __name__ == "__main__":
    start = time.time()
    df = run()
    print("Total time taken:", time.time() - start)
    print(df)
    print(df.dtypes)
    print(df.shape)
