import matplotlib.pyplot as plt
import numpy as np

# Your latency data
threads = [3, 6, 10, 16]

# Data format (can easily paste new values here)
data = {
    "Linux Redis": {
        "avg": [650.8843594, 540.1770435, 623.0089702, 681.7375117],
        "min": [73, 112, 79, 103.5],
        "max": [12766.5, 3247.5, 5768, 7594.5],
    },
    "Linux Rex": {
        "avg": [569070.6403, 211580.393, 105472.261, 67640.33878],
        "min": [696.5, 775.5, 722, 845],
        "max": [1349654, 914811.5, 532344, 339060],
    },
    "Mac Redis": {
        "avg": [674.8526676, 1154.062352, 1073.661247, 1478.551863],
        "min": [73.5, 89.5, 67.5, 58],
        "max": [6633, 9714.5, 24193.5, 23096],
    },
    "Mac Rex": {
        "avg": [2952.850043, 3099.982, 2928.627237, 2898.694275],
        "min": [173.5, 191, 282, 320],
        "max": [8733.5, 11955.5, 12990.5, 14087.5],
    }
}


# Color mapping (just change colors here if needed)
colors = {
    "Linux Redis": "tab:blue",  # Blue for Linux Redis
    "Linux Rex": "tab:red",     # Red for Linux Rex
    "Mac Redis": "orange",      # Yellow for Mac Redis
    "Mac Rex": "tab:green",     # Green for Mac Rex
}

# Plotting
plt.figure(figsize=(10, 6))

for label, metrics in data.items():
    avg = np.array(metrics["avg"])
    min_ = np.array(metrics["min"])
    max_ = np.array(metrics["max"])
    color = colors[label]
    
    lower_error = avg - min_
    upper_error = max_ - avg
    asymmetric_error = [lower_error, upper_error]

    # Plot with error bars (avg with min/max as error)
    plt.errorbar(
        threads, avg,
        yerr=asymmetric_error,
        label=label,
        color=color,
        fmt='o-', capsize=5
    )

plt.xlabel('Number of Nodes')
plt.ylabel('Latency (microseconds)')
plt.title('workloadf Update Latencies')
plt.legend()
plt.grid(True)
plt.yscale('log')  # Optional: log scale for better readability
plt.show()
