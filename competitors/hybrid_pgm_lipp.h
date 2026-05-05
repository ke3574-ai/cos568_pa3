#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <atomic>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

/**
 * @brief Hybrid index:
 *
 * LEARNING:
 *   - inserts go to LIPP
 *   - lookups go to LIPP
 *   - count first LEARN_THRESHOLD ops
 *
 * LOOKUP_HEAVY:
 *   - lookups use pure LIPP fast path via lookup_heavy_final_
 *   - inserts go to LIPP
 *
 * INSERT_HEAVY:
 *   - inserts go to DynamicPGM
 *   - lookups check PGM first, then LIPP fallback
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params) {}

  ~HybridPGMLIPP() {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) {
      std::vector<std::pair<KeyType, uint64_t>> loading_data;
      loading_data.reserve(data.size());

      for (const auto& itm : data) {
          loading_data.emplace_back(itm.key, itm.value);
      }

      uint64_t build_time = util::timing(
          [&] {
              lipp_.bulk_load(loading_data.data(), loading_data.size());
          });

      return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key,
                        uint32_t thread_id) const {
      uint64_t value;

      // Critical hot path:
      // no atomic mode load, no counter, no PGM check.
      if (lookup_heavy_final_) {
          return lipp_.find(lookup_key, value) ? value : util::NOT_FOUND;
      }

      total_lookups_.fetch_add(1, std::memory_order_relaxed);

      if (!mode_decided_.load(std::memory_order_relaxed)) {
          MaybeSetModeOnce();
      }

      Mode m = mode_.load(std::memory_order_relaxed);

      if (m == Mode::INSERT_HEAVY) {
          auto it = pgm_active_.find(lookup_key);
          if (it != pgm_active_.end()) {
              return it->value();
          }

          return lipp_.find(lookup_key, value) ? value : util::NOT_FOUND;
      }

      // LEARNING path.
      return lipp_.find(lookup_key, value) ? value : util::NOT_FOUND;
  }

  void Insert(const KeyValue<KeyType>& data,
              uint32_t thread_id) {
      Mode m = mode_.load(std::memory_order_relaxed);

      if (m == Mode::LOOKUP_HEAVY) {
          lipp_.insert(data.key, data.value);
          return;
      }

      total_inserts_.fetch_add(1, std::memory_order_relaxed);

      if (!mode_decided_.load(std::memory_order_relaxed)) {
          MaybeSetModeOnce();
      }

      m = mode_.load(std::memory_order_relaxed);

      if (m == Mode::INSERT_HEAVY) {
          pgm_active_.insert(data.key, data.value);
          return;
      }

      // LEARNING path.
      lipp_.insert(data.key, data.value);
  }

  uint64_t RangeQuery(const KeyType& lower_key,
                      const KeyType& upper_key,
                      uint32_t thread_id) const {
      uint64_t result = 0;

      // Treat range query as lookup-like during learning.
      if (!lookup_heavy_final_) {
          total_lookups_.fetch_add(1, std::memory_order_relaxed);

          if (!mode_decided_.load(std::memory_order_relaxed)) {
              MaybeSetModeOnce();
          }
      }

      Mode m = mode_.load(std::memory_order_relaxed);

      // LEARNING or LOOKUP_HEAVY:
      // all inserted data so far is in LIPP.
      if (m != Mode::INSERT_HEAVY) {
          auto it = lipp_.lower_bound(lower_key);
          while (it != lipp_.end() && it->comp.data.key <= upper_key) {
              result += it->comp.data.value;
              ++it;
          }
          return result;
      }

      // INSERT_HEAVY:
      // base + learning data is in LIPP;
      // post-decision inserts are in PGM.
      auto pit = pgm_active_.lower_bound(lower_key);
      while (pit != pgm_active_.end() && pit->key() <= upper_key) {
          result += pit->value();
          ++pit;
      }

      auto lit = lipp_.lower_bound(lower_key);
      while (lit != lipp_.end() && lit->comp.data.key <= upper_key) {
          result += lit->comp.data.value;
          ++lit;
      }

      return result;
  }

  std::string name() const {
      return "HybridPGMLIPP";
  }

  std::size_t size() const {
      return pgm_active_.size_in_bytes() + lipp_.index_size();
  }

  bool applicable(bool unique,
                  bool range_query,
                  bool insert,
                  bool multithread,
                  const std::string& ops_filename) const {
      std::string name = SearchClass::name();
      return name != "LinearAVX" && !multithread;
  }

  std::vector<std::string> variants() const {
      std::vector<std::string> vec;
      vec.push_back(SearchClass::name());
      vec.push_back(std::to_string(pgm_error));
      return vec;
  }

 private:
  enum class Mode {
      LEARNING,
      INSERT_HEAVY,
      LOOKUP_HEAVY
  };

  void MaybeSetModeOnce() const {
      if (mode_decided_.load(std::memory_order_relaxed)) {
          return;
      }

      size_t ins = total_inserts_.load(std::memory_order_relaxed);
      size_t lkp = total_lookups_.load(std::memory_order_relaxed);
      size_t total = ins + lkp;

      if (total < LEARN_THRESHOLD) {
          return;
      }

      double lookup_ratio =
          static_cast<double>(lkp) / static_cast<double>(total);

      Mode decided_mode =
          (lookup_ratio > 0.7)
              ? Mode::LOOKUP_HEAVY
              : Mode::INSERT_HEAVY;

      mode_.store(decided_mode, std::memory_order_relaxed);
      mode_decided_.store(true, std::memory_order_relaxed);

      if (decided_mode == Mode::LOOKUP_HEAVY) {
          lookup_heavy_final_ = true;
      }
  }

  // Data structures.
  LIPP<KeyType, uint64_t> lipp_;

  DynamicPGMIndex<
      KeyType,
      uint64_t,
      SearchClass,
      PGMIndex<KeyType, SearchClass, pgm_error, 16>
  > pgm_active_;

  // Learning / mode control.
  mutable std::atomic<size_t> total_inserts_{0};
  mutable std::atomic<size_t> total_lookups_{0};

  mutable std::atomic<bool> mode_decided_{false};
  mutable std::atomic<Mode> mode_{Mode::LEARNING};

  // Critical cached flag for lookup-heavy hot path.
  // Do not replace this with mode_.load(...) in EqualityLookup().
  mutable bool lookup_heavy_final_ = false;

  static constexpr size_t LEARN_THRESHOLD = 1'000;
};

#endif  // TLI_HYBRID_PGM_LIPP_H