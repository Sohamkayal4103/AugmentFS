import subprocess
import time
import os
import json
import shutil

# --- CONFIG ---
# Absolute paths are required for FUSE reliability
BACKING_DIR = os.path.abspath("./backing_dir")
MOUNT_POINT = os.path.abspath("./mount_point")
RESULTS_FILE = "results.json"
SIZES_MB = [1, 5, 10, 20] 

def cleanup():
    # Force unmount if stuck
    subprocess.run(["fusermount", "-u", "-z", MOUNT_POINT], stderr=subprocess.DEVNULL)
    
    if os.path.exists(BACKING_DIR):
        try:
            shutil.rmtree(BACKING_DIR)
        except OSError:
            pass 
            
    os.makedirs(BACKING_DIR, exist_ok=True)
    os.makedirs(MOUNT_POINT, exist_ok=True)

def run_dd_test(target_file, mb):
    count = mb * 256 # 256 blocks of 4KB = 1MB
    
    start_time = time.time()
    
    # Use dd with sync to ensure write finishes to disk
    cmd = [
        "dd", 
        "if=/dev/zero", 
        f"of={target_file}", 
        "bs=4k", 
        f"count={count}", 
        "status=none",
        "oflag=sync"
    ]
    
    result = subprocess.run(cmd)
    end_time = time.time()
    
    if result.returncode != 0:
        print(f" [Error] dd failed for {mb}MB")
        return None
        
    return end_time - start_time

def benchmark_native(label):
    print(f"--- Benchmarking: {label} ---")
    times = []
    
    # Ensure dir exists
    cleanup()
    
    for size in SIZES_MB:
        print(f"  Writing {size} MB...", end="", flush=True)
        # Write directly to backing dir (Native Ext4/NTFS)
        target = os.path.join(BACKING_DIR, "native_test.dat")
        duration = run_dd_test(target, size)
        
        if duration is not None:
            print(f" Done in {duration:.4f}s")
            times.append(duration)
            try:
                os.remove(target)
            except OSError:
                pass
        else:
            times.append(0)
            
    return times

def benchmark_fs(binary_name, label):
    print(f"--- Benchmarking: {label} ({binary_name}) ---")
    times = []
    
    cleanup()
    print(f"Mounting {binary_name}...")
    
    proc = subprocess.Popen([f"./{binary_name}", BACKING_DIR, MOUNT_POINT])
    time.sleep(2) 
    
    try:
        for size in SIZES_MB:
            print(f"  Writing {size} MB...", end="", flush=True)
            # Write to Mount Point (Through FUSE)
            target = os.path.join(MOUNT_POINT, "fuse_test.dat")
            duration = run_dd_test(target, size)
            
            if duration is not None:
                print(f" Done in {duration:.4f}s")
                times.append(duration)
                try:
                    os.remove(target)
                except OSError:
                    pass
            else:
                times.append(0)
            
    finally:
        print("Unmounting...")
        proc.terminate()
        subprocess.run(["fusermount", "-u", MOUNT_POINT], stderr=subprocess.DEVNULL)
        time.sleep(1)
        
    return times

def main():
    print("Compiling...")
    subprocess.run(["make", "clean"], stdout=subprocess.DEVNULL)
    subprocess.run(["make"], stdout=subprocess.DEVNULL)
    
    results = {
        "sizes": SIZES_MB,
        "native": [],
        "optimized": [],
        "bad": []
    }
    
    # 1. Test Native
    results["native"] = benchmark_native("Native Filesystem (Baseline)")

    # 2. Test Optimized
    results["optimized"] = benchmark_fs("metadatafs", "Optimized Map Architecture")
    
    # 3. Test Bad
    results["bad"] = benchmark_fs("metadatafs_bad", "Naive DB Architecture")
    
    # Save results
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=4)
        
    print(f"\nData saved to {RESULTS_FILE}")
    print("Now run 'python3 plot_graph.py' to visualize.")

if __name__ == "__main__":
    main()