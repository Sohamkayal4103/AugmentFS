import matplotlib.pyplot as plt
import json
import sys

def plot():
    try:
        with open("results.json", "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("Error: results.json not found. Run collect_data.py first.")
        sys.exit(1)

    sizes = data["sizes"]
    native_times = data["native"]
    opt_times = data["optimized"]
    bad_times = data["bad"]

    plt.figure(figsize=(12, 7))

    # 1. Native (Baseline) - Blue Dashed
    plt.plot(sizes, native_times, marker='s', linestyle='--', color='blue', label='Native FS (Baseline)', linewidth=2, alpha=0.7)
    
    # 2. Bad (Naive) - Red Solid
    plt.plot(sizes, bad_times, marker='o', linestyle='-', color='red', label='Naive (Direct DB Write)', linewidth=2)
    
    # 3. Optimized (Yours) - Green Solid
    plt.plot(sizes, opt_times, marker='^', linestyle='-', color='green', label='Optimized (In-Memory Map)', linewidth=3)

    # Styling
    plt.title('File System Performance Comparison', fontsize=16)
    plt.xlabel('File Size (MB)', fontsize=12)
    plt.ylabel('Time to Write (Seconds)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(fontsize=12)

    # Annotations
    last_idx = len(sizes) - 1
    
    # Calculate Degradation vs Native
    fuse_overhead = opt_times[last_idx] / native_times[last_idx]
    
    # Calculate Improvement vs Bad
    speedup = bad_times[last_idx] / opt_times[last_idx]

    # Annotate Bad vs Optimized
    plt.annotate(f'{speedup:.1f}x Slower', 
                 xy=(sizes[last_idx], bad_times[last_idx]), 
                 xytext=(sizes[last_idx]-5, bad_times[last_idx]),
                 arrowprops=dict(facecolor='red', shrink=0.05))

    # Annotate Optimized vs Native (The "Cost" of FUSE)
    plt.annotate(f'FUSE Overhead: {fuse_overhead:.1f}x', 
                 xy=(sizes[last_idx], opt_times[last_idx]), 
                 xytext=(sizes[last_idx]-2, opt_times[last_idx] + (bad_times[last_idx] * 0.1)),
                 arrowprops=dict(facecolor='green', shrink=0.05))

    # Save
    plt.savefig('performance_comparison.png')
    print("Graph saved as 'performance_comparison.png'")
    plt.show()

if __name__ == "__main__":
    plot()