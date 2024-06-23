import glob
import json
import os

from matplotlib.ticker import FuncFormatter

# Get the names of all files in ./results/parallel
files = glob.glob(os.path.dirname(os.path.abspath(__file__)) + "/results/parallel/*.json")
print(files)

results = dict()
window_size = None
num_samples = None
duration = None
warmup = None

for file in files:
    with open(file, "r") as f:
        res = json.loads(f.read())

        # Verify that all results have the same window_size, num_samples, and duration
        if window_size is None:
            window_size = res["window_size"]
            num_samples = res["num_samples"]
            duration = res["duration"]
            warmup = res["warmup"]
        else:
            assert window_size == res["window_size"]
            assert num_samples == res["num_samples"]
            assert duration == res["duration"]
            assert warmup == res["warmup"]
        
        num_threads_to_throughput = dict()
        for (key, value) in res["results"].items():
            num_threads = int(key.split("threads=")[1].split(")")[0])
            num_threads_to_throughput[num_threads] = value

        compression_algorithm = os.path.basename(file).removesuffix(".json")

        if compression_algorithm == "None":
            results[f"ZygosDB (ours), no compression"] = num_threads_to_throughput
        else:
            results[f"ZygosDB (ours), {compression_algorithm} compressed"] = num_threads_to_throughput



print(results)


# Draw the graph
import matplotlib.pyplot as plt

fig, ax = plt.subplots()

# Set the labels
ax.set_xlabel("Number of threads")
ax.set_ylabel("Throughput (returned rows per second)")

# Set the title
ax.set_title("Throughput with different number of threads")

# Set the x-ticks
ax.set_xticks([1, *list(range(2, 33, 2))])

# Set the y-ticks
ax.set_yticks(range(0, 11 * 10**6, 10**6))
ax.set_ylim(0, 11 * 10**6)
# ax.ticklabel_format(style="sci", useLocale=True, useMathText=True, useOffset=True, axis="y")

formatter = FuncFormatter(lambda x, pos: f'{x*1e-6:.0f} 000 000' if x != 0 else '0')

# Set the y-axis formatter
ax.yaxis.set_major_formatter(formatter)


# Plot the data
for file, num_threads_to_throughput in results.items():
    ax.plot(list(num_threads_to_throughput.keys()), list(num_threads_to_throughput.values()), label=file)

# Add a grid
ax.grid()

# Add a legend
ax.legend()

# Change the left margin
plt.subplots_adjust(left=0.205)

# Save the plot
fig.savefig(os.path.dirname(os.path.abspath(__file__)) + "/results/figures/benchmark_parallel.png")

# Show the plot
plt.show()
