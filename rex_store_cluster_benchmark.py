#!/usr/bin/env python3
import subprocess
import time
import argparse
import os
import sys
import signal
from datetime import datetime


class RexStoreClusterBenchmark:
    def __init__(
        self,
        workload,
        node_count,
        replication_factor=3,
        write_quorum=2,
        read_quorum=1,
        runs=2,
        threads=32,
    ):
        self.workload = workload
        self.node_count = node_count
        self.replication_factor = replication_factor
        self.write_quorum = write_quorum
        self.read_quorum = read_quorum
        self.runs = runs
        self.threads = threads
        self.measurement_types = ["timeseries", "histogram"]
        self.output_base_dir = "rex_output"
        self.rex_store_path = os.path.expanduser("~/Desktop/Concurrency/rex_store")
        self.current_process = None

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

    def build_rex_store(self):
        """Build rex_store using cargo."""
        print("Building rex_store with cargo...")
        original_dir = os.getcwd()
        try:
            os.chdir(self.rex_store_path)
            self.run_command("cargo build --release")
            print("Build completed successfully.")
        finally:
            os.chdir(original_dir)

    def build_ycsb_client(self):
        """Build YCSB client for rex_store using Maven."""
        print("Building YCSB client for rex_store...")
        self.run_command("mvn clean package -pl rex_store -am -DskipTests")
        print("YCSB rex_store client build completed successfully.")

    def launch_rex_store_cluster(self):
        """Launch rex_store cluster and wait for it to be ready."""
        print(f"Launching rex_store cluster with {self.node_count} nodes...")

        original_dir = os.getcwd()
        try:
            os.chdir(self.rex_store_path)

            cmd = (
                f"cargo run --release -- -n {self.node_count} "
                f"-N {self.replication_factor} "
                f"-w {self.write_quorum} "
                f"-r {self.read_quorum}"
            )

            self.current_process = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                preexec_fn=os.setsid,  # Create new process group for proper signal handling
            )

            if self.current_process.poll() is None:
                print(
                    f"rex_store cluster with {self.node_count} nodes started successfully!"
                )
                return True
            else:
                exit_code = self.current_process.poll()
                print(f"rex_store failed to start. Exit code: {exit_code}")
                return False

        except Exception as e:
            print(f"Exception occurred: {e}")
            return False
        finally:
            os.chdir(original_dir)

    def stop_rex_store_cluster(self):
        """Stop rex_store cluster gracefully."""
        print("Stopping rex_store cluster...")
        if self.current_process and self.current_process.poll() is None:
            try:
                # Send SIGINT (Ctrl+C) to the process group
                os.killpg(os.getpgid(self.current_process.pid), signal.SIGINT)
                # Wait for graceful shutdown
                self.current_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if doesn't shut down gracefully
                os.killpg(os.getpgid(self.current_process.pid), signal.SIGKILL)
            except Exception as e:
                print(f"Error stopping rex_store: {e}")
            finally:
                self.current_process = None

        # Wait for cleanup
        time.sleep(2)
        print("rex_store cluster stopped.")

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
            f"./bin/ycsb load rex_store -P rex_store.properties "
            f"-P workloads/{self.workload} "
            f"-threads {self.threads} -s"
        )
        self.run_command(load_cmd)

        print(f"Running workload with {self.threads} threads...")
        run_cmd = (
            f"./bin/ycsb run rex_store -P rex_store.properties "
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

    def run_benchmark(self):
        """Run the complete benchmark suite."""
        self.build_rex_store()
        self.build_ycsb_client()

        print(f"\n{'=' * 50}")
        print(f"Starting benchmarks for {self.node_count} nodes")
        print(f"{'=' * 50}")

        for run in range(self.runs):
            for measurement_type in self.measurement_types:
                print(
                    f"\nRun {run + 1}/{self.runs} with {measurement_type} measurement"
                )

                # Launch cluster for each run and measurement type
                if not self.launch_rex_store_cluster():
                    print(
                        f"Failed to launch cluster with {self.node_count} nodes. Exiting..."
                    )
                    sys.exit(1)

                try:
                    self.run_ycsb_workload(measurement_type, run + 1)
                    time.sleep(2)
                finally:
                    # Stop cluster after each run
                    self.stop_rex_store_cluster()

        print("\n" + "=" * 50)
        print("Benchmark completed!")
        print("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Rex Store Cluster Benchmark Tool")
    parser.add_argument("workload", help="YCSB workload file name (e.g., workloada)")
    parser.add_argument("node_count", type=int, help="Number of nodes in the cluster")
    parser.add_argument(
        "--replication-factor",
        "-N",
        type=int,
        default=3,
        help="Replication factor for rex_store (default: 3)",
    )
    parser.add_argument(
        "--write-quorum",
        "-w",
        type=int,
        default=2,
        help="Write quorum for rex_store (default: 2)",
    )
    parser.add_argument(
        "--read-quorum",
        "-r",
        type=int,
        default=2,
        help="Read quorum for rex_store (default: 2)",
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

    benchmark = RexStoreClusterBenchmark(
        args.workload,
        args.node_count,
        args.replication_factor,
        args.write_quorum,
        args.read_quorum,
        args.runs,
        args.threads,
    )
    benchmark.run_benchmark()


if __name__ == "__main__":
    main()
