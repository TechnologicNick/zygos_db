from dataclasses import dataclass
import matplotlib.pyplot as plt
import numpy as np

@dataclass
class TestResult():
    name: str
    window_size: int
    throughput: float

results = [
    (1, {'ZygosDB': 79749.86539907669, 'Tabix': 522.2711947909263}),
    (10, {'ZygosDB': 700468.9718615286, 'Tabix': 4971.148295847912}),
    (100, {'ZygosDB': 3784844.6326252916, 'Tabix': 47894.09358607953}),
    (1000, {'ZygosDB': 6768505.3487811005, 'Tabix': 368838.07562526676}),
    (10000, {'ZygosDB': 6919190.719647226, 'Tabix': 1047019.887533764}),
    (100000, {'ZygosDB': 7000172.61615921, 'Tabix': 1346063.5682206533}),
]
results = [TestResult("ZygosDB", window_size, throughput["ZygosDB"]) for window_size, throughput in results] \
        + [TestResult("Tabix", window_size, throughput["Tabix"]) for window_size, throughput in results]

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
ax.set_yticks(np.arange(0, 10000000, 1000000))

# Plot the data
tabix_results = [result for result in results if result.name == "Tabix"]
zygosdb_results = [result for result in results if result.name == "ZygosDB"]

ax.plot([result.window_size for result in tabix_results], [result.throughput for result in tabix_results], label="Tabix")
ax.plot([result.window_size for result in zygosdb_results], [result.throughput for result in zygosdb_results], label="ZygosDB (ours)")

# Add a legend
ax.legend()

# Show the plot
plt.show()

# Save the plot
fig.savefig("benchmark.png")
