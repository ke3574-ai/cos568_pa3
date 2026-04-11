#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

using namespace tli;

// Layer 1: Searcher-based (minimal: only ONE config)
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(
    tli::Benchmark<uint64_t>& benchmark,
    bool pareto,
    const std::vector<int>& params) {

  if (!pareto) {
    util::fail("Hybrid PGM-LIPP does not support non-pareto mode");
  }

  // 🔥 Only ONE configuration
  benchmark.template Run<
      HybridPGMLIPP<uint64_t, Searcher, 16>
  >();
}


// Layer 2: record-based (ENTRY POINT)
template <int record>
void benchmark_64_hybrid_pgm_lipp(
    tli::Benchmark<uint64_t>& benchmark,
    const std::string& filename) {

  // 🔥 Minimal: ignore dataset-specific branching
  // Just run ONE search strategy

  benchmark_64_hybrid_pgm_lipp<
      LinearSearch<record>
  >(benchmark, true, {});
}


// Required macro
INSTANTIATE_TEMPLATES_MULTITHREAD(
    benchmark_64_hybrid_pgm_lipp,
    uint64_t);