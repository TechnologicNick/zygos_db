from dataclasses import dataclass
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np

@dataclass
class TestResult():
    name: str
    window_size: int
    throughput: float

results = [
    (1, {'ZygosDB': 971.8673838762625, 'ZygosDB_Gzip': 1837.2594974643926, 'ZygosDB_LZ4': 1531.6277705995944, 'Tabix': 522.2711947909263}),
    (10, {'ZygosDB': 15122.882776926532, 'ZygosDB_Gzip': 17438.599237757557, 'ZygosDB_LZ4': 11154.817022375008, 'Tabix': 4971.148295847912}),
    (100, {'ZygosDB': 90383.84515744388, 'ZygosDB_Gzip': 90139.26435550248, 'ZygosDB_LZ4': 173143.71705665457, 'Tabix': 47894.09358607953}),
    (1000, {'ZygosDB': 1044153.8625563037, 'ZygosDB_Gzip': 1124853.6651894534, 'ZygosDB_LZ4': 1158702.4105315635, 'Tabix': 368838.07562526676}),
    (10000, {'ZygosDB': 4561121.180095218, 'ZygosDB_Gzip': 4079134.1051165815, 'ZygosDB_LZ4': 4832665.575597453, 'Tabix': 1047019.887533764}),
    (100000, {'ZygosDB': 6909575.550247099, 'ZygosDB_Gzip': 5298046.945017996, 'ZygosDB_LZ4': 6466138.087878921, 'Tabix': 1346063.5682206533}),
]
results = [TestResult("ZygosDB", window_size, throughput["ZygosDB"]) for window_size, throughput in results] \
        + [TestResult("Tabix", window_size, throughput["Tabix"]) for window_size, throughput in results] \
        + [TestResult("ZygosDB_Gzip", window_size, throughput["ZygosDB_Gzip"]) for window_size, throughput in results] \
        + [TestResult("ZygosDB_LZ4", window_size, throughput["ZygosDB_LZ4"]) for window_size, throughput in results]

def plot_throughput():
    # Draw the graph
    fig, ax = plt.subplots()

    # Set the labels
    ax.set_xlabel("Window size")
    ax.set_ylabel("Throughput (returned rows per second)")

    # Set the title
    ax.set_title("Throughput of Tabix and ZygosDB")

    # Set the x-axis to be logarithmic
    ax.set_xscale("log")

    # Set the x-ticks
    ax.set_xticks([1, 10, 100, 1000, 10000, 100000])
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())

    # Set the y-ticks
    # ax.set_yticks(np.arange(0, 10000000, 1000000))

    # Set the y-axis formatter
    # formatter = FuncFormatter(lambda x, pos: f'{x*1e-6:.0f} 000 000' if x != 0 else '0')
    # ax.yaxis.set_major_formatter(formatter)

    # Make the y-axis logarithmic
    ax.set_yscale("log")

    # Plot the data
    tabix_results = [result for result in results if result.name == "Tabix"]
    zygosdb_results = [result for result in results if result.name == "ZygosDB"]
    zygosdb_gzip_results = [result for result in results if result.name == "ZygosDB_Gzip"]
    zygosdb_lz4_results = [result for result in results if result.name == "ZygosDB_LZ4"]

    ax.plot([result.window_size for result in tabix_results], [result.throughput for result in tabix_results], label="Tabix")
    ax.plot([result.window_size for result in zygosdb_results], [result.throughput for result in zygosdb_results], label="ZygosDB (ours), not compressed")
    ax.plot([result.window_size for result in zygosdb_gzip_results], [result.throughput for result in zygosdb_gzip_results], label="ZygosDB (ours), Gzip compressed")
    ax.plot([result.window_size for result in zygosdb_lz4_results], [result.throughput for result in zygosdb_lz4_results], label="ZygosDB (ours), LZ4 compressed")

    # Change the left margin
    # plt.subplots_adjust(left=0.205)

    # Add a legend
    ax.legend()

    # Add a grid
    ax.grid()

    # Save the plot
    fig.savefig(os.path.dirname(os.path.abspath(__file__)) + "/results/figures/throughput.png")

    # Show the plot
    plt.show()

def plot_speedup():
    # Draw the graph
    fig, ax = plt.subplots()

    # Set the labels
    ax.set_xlabel("Window size")
    ax.set_ylabel("Speedup (number of times faster)")

    # Set the title
    ax.set_title("Speedup of ZygosDB over Tabix")

    # Set the x-axis to be logarithmic
    ax.set_xscale("log")

    # Set the x-ticks
    ax.set_xticks([1, 10, 100, 1000, 10000, 100000])
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())

    # Make the y-axis logarithmic
    # ax.set_yscale("log")

    # Set the y-axis formatter
    formatter = FuncFormatter(lambda x, pos: f'{x:.0f}×')
    ax.yaxis.set_major_formatter(formatter)

    # Plot the data
    tabix_results = [result for result in results if result.name == "Tabix"]
    zygosdb_results = [result for result in results if result.name == "ZygosDB"]
    zygosdb_gzip_results = [result for result in results if result.name == "ZygosDB_Gzip"]
    zygosdb_lz4_results = [result for result in results if result.name == "ZygosDB_LZ4"]

    # Plot baseline
    ax.plot(
        [result.window_size for result in tabix_results],
        [1 for _ in tabix_results],
        label="Tabix, baseline"
    )

    ax.plot(
        [result.window_size for result in tabix_results],
        [zybosdb / tabix for zybosdb, tabix in zip([result.throughput for result in zygosdb_results], [result.throughput for result in tabix_results])],
        label="ZygosDB (ours), not compressed"
    )

    ax.plot(
        [result.window_size for result in tabix_results],
        [zybosdb / tabix for zybosdb, tabix in zip([result.throughput for result in zygosdb_gzip_results], [result.throughput for result in tabix_results])],
        label="ZygosDB (ours), Gzip compressed"
    )

    ax.plot(
        [result.window_size for result in tabix_results],
        [zybosdb / tabix for zybosdb, tabix in zip([result.throughput for result in zygosdb_lz4_results], [result.throughput for result in tabix_results])],
        label="ZygosDB (ours), LZ4 compressed"
    )

    # Add a legend
    ax.legend()

    # Add a grid
    ax.grid()

    # Save the plot
    fig.savefig(os.path.dirname(os.path.abspath(__file__)) + "/results/figures/speedup.png")

    # Show the plot
    plt.show()

if __name__ == "__main__":
    plot_throughput()
    plot_speedup()