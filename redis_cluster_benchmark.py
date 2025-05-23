#!/usr/bin/env python3
import subprocess
import time
import argparse
import os
import sys
from datetime import datetime


class RedisClusterBenchmark:
    def __init__(
        self,
        workload,
        node_count,
        base_port=7000,
        runs=2,
        threads=32,
    ):
        self.workload = workload
        self.node_count = node_count
        self.base_port = base_port
        self.runs = runs
        self.threads = threads
        self.measurement_types = ["timeseries", "histogram"]
        self.output_base_dir = "redis_output"

        # Create output directory structure
        self.output_dir = os.path.join(self.output_base_dir, workload, str(node_count))
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created output directory: {self.output_dir}")

    def run_command(self, command, check=True, shell=False):
        """Execute a command and return output."""
        print(f"Running: {command}")
        try:
            if shell:
                result = subprocess.run(
                    command, shell=True, capture_output=True, text=True, check=check
                )
            else:
                result = subprocess.run(
                    command.split(), capture_output=True, text=True, check=check
                )
            return result.stdout, result.stderr
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {e}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")
            if check:
                raise
            return e.stdout, e.stderr

    def build_redis(self):
        """Build Redis using Maven."""
        print("Building Redis with Maven...")
        self.run_command("mvn clean package -pl redis -am -DskipTests")
        print("Build completed successfully.")

    def launch_redis_cluster(self):
        """Launch Redis cluster and wait for it to be ready."""
        print(f"Launching Redis cluster with {self.node_count} nodes...")

        process = subprocess.Popen(
            f"./redis_cluster_launch.sh {self.node_count} {self.base_port}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        ok_count = 0
        timeout = 120  # 2 minutes timeout
        start_time = time.time()

        while ok_count < self.node_count and time.time() - start_time < timeout:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                print(line.strip())
                if "Cluster state changed: ok" in line:
                    ok_count += 1
                    print(f"*** Detected {ok_count}/{self.node_count} nodes ready ***")

        if process.poll() is None:
            time.sleep(2)
            if process.poll() is None:
                process.terminate()

        if ok_count == self.node_count:
            print(f"All {self.node_count} nodes are ready!")
            return True
        else:
            print(f"Timeout or error: Only {ok_count}/{self.node_count} nodes ready.")
            return False

    def clean_maven_output(self, content):
        """Clean Maven output by removing everything up to BUILD SUCCESS and the next line."""
        lines = content.split("\n")

        build_success_index = -1
        for i, line in enumerate(lines):
            if "[INFO] BUILD SUCCESS" in line:
                build_success_index = i
                break

        if build_success_index == -1:
            # No BUILD SUCCESS found, return the original content
            return content

        skip_lines = 5

        if build_success_index + skip_lines < len(lines):
            return "\n".join(lines[build_success_index + skip_lines :])
        else:
            return content

    def run_ycsb_workload(self, measurement_type, run_number):
        """Run YCSB workload with specified measurement type."""
        print(f"Loading data with {self.threads} threads...")
        load_cmd = (
            f"./bin/ycsb load redis -P redis.properties "
            f"-P workloads/{self.workload} "
            f"-threads {self.threads} -s"
        )
        self.run_command(load_cmd)

        print(f"Running workload with {self.threads} threads...")
        run_cmd = (
            f"./bin/ycsb run redis -P redis.properties "
            f"-P workloads/{self.workload} "
            f"-p measurementtype={measurement_type} "
            f"-threads {self.threads} -s"
        )

        output_file = os.path.join(
            self.output_dir, f"{measurement_type}_{run_number}.txt"
        )

        stdout, _ = self.run_command(run_cmd)
        cleaned_output = self.clean_maven_output(stdout)

        with open(output_file, "w") as f:
            f.write(cleaned_output)

        print(f"Results saved to {output_file}")

    def reset_redis_cluster(self):
        """Reset Redis cluster for next run."""
        print("Resetting Redis cluster...")
        self.run_command("./redis_cluster_reset.sh")
        # Add a delay to ensure the cluster is fully stopped
        time.sleep(3)

    def run_benchmark(self):
        """Run the complete benchmark suite."""
        self.build_redis()

        print(f"\n{'=' * 50}")
        print(f"Starting benchmarks for {self.node_count} nodes")
        print(f"{'=' * 50}")

        for run in range(self.runs):
            print(f"\n{'*' * 40}")
            print(f"Starting Run {run + 1}/{self.runs}")
            print(f"{'*' * 40}")

            # Launch a fresh Redis cluster for this run
            if not self.launch_redis_cluster():
                print(f"Failed to launch cluster for run {run + 1}. Skipping...")
                continue

            try:
                for measurement_type in self.measurement_types:
                    print(
                        f"\nRun {run + 1}/{self.runs} with {measurement_type} measurement"
                    )

                    self.run_ycsb_workload(measurement_type, run + 1)
                    time.sleep(2)
            finally:
                # Always reset the cluster after each run
                self.reset_redis_cluster()
                print(f"Completed run {run + 1}/{self.runs}")

        print("\n" + "=" * 50)
        print("All benchmark runs completed!")
        print("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Redis Cluster Benchmark Tool")
    parser.add_argument("workload", help="YCSB workload file name (e.g., workloada)")
    parser.add_argument("node_count", type=int, help="Number of nodes in the cluster")
    parser.add_argument(
        "--base-port", type=int, default=7000, help="Base port for Redis nodes"
    )
    parser.add_argument(
        "--runs", type=int, default=2, help="Number of runs per configuration"
    )
    parser.add_argument(
        "--threads", type=int, default=32, help="Number of threads for YCSB workloads"
    )

    args = parser.parse_args()

    if not os.path.exists(f"workloads/{args.workload}"):
        print(f"Error: Workload file 'workloads/{args.workload}' not found!")
        sys.exit(1)

    benchmark = RedisClusterBenchmark(
        args.workload, args.node_count, args.base_port, args.runs, args.threads
    )
    benchmark.run_benchmark()


if __name__ == "__main__":
    main()
