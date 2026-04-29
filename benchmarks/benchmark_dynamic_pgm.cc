#include "benchmarks/benchmark_pgm.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/dynamic_pgm_index.h"

// =====================================================
// MINIMAL DYNAMIC PGM BENCHMARK (COLAB OPTIMIZED)
// Only for fb_100M + mixed workloads
// =====================================================

template <int record>
void benchmark_64_dynamic_pgm(tli::Benchmark<uint64_t>& benchmark,
                             const std::string& filename) {

  // Only run for fb_100M
  if (filename.find("fb_100M") == std::string::npos) {
    return;
  }

  // Only run for mixed workloads (0.9 and 0.1 insert)
  // if (filename.find("mix") == std::string::npos) {
  //   return;
  // }

  // -------------------------------------------------
  // ONLY instantiate best-performing configs
  // -------------------------------------------------

  // Primary target (best from your experiments)
  benchmark.template Run<
      DynamicPGM<uint64_t, BranchingBinarySearch<record>, 64>
  >();

  // Optional secondary (for sanity comparison)
  // benchmark.template Run<
  //     DynamicPGM<uint64_t, BranchingBinarySearch<record>, 128>
  // >();
}

// Disable pareto version entirely (avoids template explosion)
template <typename Searcher>
void benchmark_64_dynamic_pgm(tli::Benchmark<uint64_t>& benchmark,
                             bool pareto,
                             const std::vector<int>& params) {
  // Do nothing — prevent instantiation of many configs
}

// Keep template instantiation macro
INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_dynamic_pgm, uint64_t);