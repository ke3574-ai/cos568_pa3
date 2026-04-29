#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// =====================================================
// MINIMAL HYBRID PGM + LIPP BENCHMARK (COLAB OPTIMIZED)
// Only for fb_100M + mixed workloads
// =====================================================

// Disable pareto version to avoid template explosion
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                 bool pareto,
                                 const std::vector<int>& params) {
  // Intentionally empty
}

// Main benchmark (reduced)
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                 const std::string& filename) {

  // Only run for fb_100M dataset
  if (filename.find("fb_100M") == std::string::npos) {
    return;
  }

  // Only run mixed workloads (0.9 and 0.1 insert)
  // if (filename.find("mix") == std::string::npos) {
  //   return;
  // }

  // -------------------------------------------------
  // Best-performing configs from your experiments
  // -------------------------------------------------

  // Primary (best)
  benchmark.template Run<
      HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>
  >();

  // // Secondary (sanity comparison)
  // benchmark.template Run<
  //     HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>
  // >();
}

// Keep template instantiation macro
INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);