#!/bin/bash
# Run all steps in sequence

set -e  # Exit on error

echo "=== Starting Benchmark ==="

# Make scripts executable
chmod +x scripts/*.sh

# Run all steps in sequence
echo "Step 1: Downloading dataset..."
./scripts/download_dataset.sh

echo "Step 2: Creating minimal CMakeLists.txt..."
./scripts/create_minimal_cmake.sh

echo "Step 3: Generating lookup workloads..."
./scripts/generate_workloads.sh

echo "Step 4: Building benchmark..."
./scripts/build_benchmark.sh

echo "Step 5: Running benchmarks..."
./scripts/run_benchmarks.sh

echo "=== Benchmark completed successfully ==="
echo "Check results in the 'results' directory."
