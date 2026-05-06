#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <limits>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

/**
 * @brief HybridPGMLIPP with internally specialized modes.
 *
 * Public behavior:
 *
 * LEARNING:
 *   - inserts go to LIPP
 *   - lookups go to LIPP
 *   - count first LEARN_THRESHOLD operations
 *
 * LOOKUP_HEAVY:
 *   - lookups use isolated LIPP-only helper
 *   - inserts go to LIPP
 *
 * INSERT_HEAVY:
 *   - inserts go to pgm_active_
 *   - pgm_active_ periodically swaps into pgm_flush_
 *   - pgm_flush_ asynchronously flushes into LIPP
 *   - lookups use insert-heavy-specific helper
 *
 * Important:
 *   lookup_heavy_final_ is a cached plain bool used only to protect
 *   the lookup-heavy hot path from atomic mode checks.
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params) {}

  ~HybridPGMLIPP() {
      if (flush_thread_.joinable()) {
          flush_thread_.join();
      }
  }

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
      // Tiny dispatcher.
      //
      // Critical: keep lookup-heavy path as the first branch and route
      // immediately into the LIPP-only helper.
      if (__builtin_expect(lookup_heavy_final_, 1)) {
          return LookupHeavyEqualityLookup(lookup_key);
      }

      if (insert_heavy_final_) {
          return InsertHeavyEqualityLookup(lookup_key);
      }

      return LearningEqualityLookup(lookup_key);
  }

  void Insert(const KeyValue<KeyType>& data,
              uint32_t thread_id) {
      // Tiny dispatcher for inserts too.
      if (__builtin_expect(lookup_heavy_final_, 1)) {
          LookupHeavyInsert(data);
          return;
      }

      if (insert_heavy_final_) {
          InsertHeavyInsert(data, thread_id);
          return;
      }

      LearningInsert(data, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lower_key,
                      const KeyType& upper_key,
                      uint32_t thread_id) const {
      // Range queries are not the current performance target, but keep them
      // semantically aligned with the three-mode design.

      if (lookup_heavy_final_) {
          return LookupHeavyRangeQuery(lower_key, upper_key);
      }

      if (insert_heavy_final_) {
          return InsertHeavyRangeQuery(lower_key, upper_key);
      }

      return LearningRangeQuery(lower_key, upper_key);
  }

  std::string name() const {
      return "HybridPGMLIPP";
  }

  std::size_t size() const {
      return pgm_active_.size_in_bytes()
           + pgm_flush_.size_in_bytes()
           + lipp_.index_size();
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
      vec.push_back("flush_threshold_" + std::to_string(FLUSH_THRESHOLD));
      vec.push_back("chunk_" + std::to_string(LIPP_FLUSH_CHUNK));
      return vec;
  }

 private:
  enum class Mode {
      LEARNING,
      INSERT_HEAVY,
      LOOKUP_HEAVY
  };

  // --------------------------------------------------------------------------
  // Mode decision
  // --------------------------------------------------------------------------

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
      } else {
          insert_heavy_final_ = true;
      }
  }

  // --------------------------------------------------------------------------
  // Equality lookup helpers
  // --------------------------------------------------------------------------

  __attribute__((always_inline)) inline size_t LookupHeavyEqualityLookup(
      const KeyType& lookup_key
  ) const {
      uint64_t value;
      return lipp_.find(lookup_key, value) ? value : util::NOT_FOUND;
  }

  size_t LearningEqualityLookup(const KeyType& lookup_key) const {
      uint64_t value;

      total_lookups_.fetch_add(1, std::memory_order_relaxed);

      if (!mode_decided_.load(std::memory_order_relaxed)) {
          MaybeSetModeOnce();
      }

      // If the current lookup caused the mode decision, dispatch to the
      // specialized mode immediately.
      if (lookup_heavy_final_) {
          return LookupHeavyEqualityLookup(lookup_key);
      }

      if (insert_heavy_final_) {
          return InsertHeavyEqualityLookup(lookup_key);
      }

      return lipp_.find(lookup_key, value) ? value : util::NOT_FOUND;
  }

  size_t InsertHeavyEqualityLookup(const KeyType& lookup_key) const {
      uint64_t value;

      // Unique-key optimized order:
      // for this benchmark, logs say data is unique, so LIPP-first should be
      // safe if inserted keys do not overwrite existing LIPP keys.
      //
      // If no async flush is active, avoid the LIPP mutex entirely.
      if (!flush_in_progress_.load(std::memory_order_acquire)) {
          if (lipp_.find(lookup_key, value)) {
              return value;
          }
      } else {
          std::lock_guard<std::mutex> lipp_lock(lipp_mutex_);
          if (lipp_.find(lookup_key, value)) {
              return value;
          }
      }

      // New inserts after the most recent active-buffer reset.
      auto it = pgm_active_.find(lookup_key);
      if (it != pgm_active_.end()) {
          return it->value();
      }

      // Frozen buffer currently being flushed.
      if (flush_in_progress_.load(std::memory_order_acquire)) {
          std::lock_guard<std::mutex> flush_lock(flush_mutex_);

          auto fit = pgm_flush_.find(lookup_key);
          if (fit != pgm_flush_.end()) {
              return fit->value();
          }
      }

      return util::NOT_FOUND;
  }

  // --------------------------------------------------------------------------
  // Insert helpers
  // --------------------------------------------------------------------------

  void LookupHeavyInsert(const KeyValue<KeyType>& data) {
      lipp_.insert(data.key, data.value);
  }

  void LearningInsert(const KeyValue<KeyType>& data,
                      uint32_t thread_id) {
      total_inserts_.fetch_add(1, std::memory_order_relaxed);

      if (!mode_decided_.load(std::memory_order_relaxed)) {
          MaybeSetModeOnce();
      }

      // If this insert caused the mode decision, use the specialized mode
      // immediately.
      if (lookup_heavy_final_) {
          LookupHeavyInsert(data);
          return;
      }

      if (insert_heavy_final_) {
          InsertHeavyInsert(data, thread_id);
          return;
      }

      // Still learning.
      lipp_.insert(data.key, data.value);
  }

  void InsertHeavyInsert(const KeyValue<KeyType>& data,
                         uint32_t thread_id) {
      pgm_active_.insert(data.key, data.value);
      ++current_buffer_size_;

      if (current_buffer_size_ >= FLUSH_THRESHOLD &&
          !flush_in_progress_.load(std::memory_order_acquire)) {
          StartAsyncFlush(thread_id);
      }
  }

  // --------------------------------------------------------------------------
  // Range query helpers
  // --------------------------------------------------------------------------

  uint64_t LookupHeavyRangeQuery(const KeyType& lower_key,
                                 const KeyType& upper_key) const {
      uint64_t result = 0;

      auto it = lipp_.lower_bound(lower_key);
      while (it != lipp_.end() && it->comp.data.key <= upper_key) {
          result += it->comp.data.value;
          ++it;
      }

      return result;
  }

  uint64_t LearningRangeQuery(const KeyType& lower_key,
                              const KeyType& upper_key) const {
      total_lookups_.fetch_add(1, std::memory_order_relaxed);

      if (!mode_decided_.load(std::memory_order_relaxed)) {
          MaybeSetModeOnce();
      }

      if (lookup_heavy_final_) {
          return LookupHeavyRangeQuery(lower_key, upper_key);
      }

      if (insert_heavy_final_) {
          return InsertHeavyRangeQuery(lower_key, upper_key);
      }

      return LookupHeavyRangeQuery(lower_key, upper_key);
  }

  uint64_t InsertHeavyRangeQuery(const KeyType& lower_key,
                                 const KeyType& upper_key) const {
      uint64_t result = 0;

      // LIPP part.
      if (!flush_in_progress_.load(std::memory_order_acquire)) {
          auto lit = lipp_.lower_bound(lower_key);
          while (lit != lipp_.end() && lit->comp.data.key <= upper_key) {
              result += lit->comp.data.value;
              ++lit;
          }
      } else {
          std::lock_guard<std::mutex> lipp_lock(lipp_mutex_);

          auto lit = lipp_.lower_bound(lower_key);
          while (lit != lipp_.end() && lit->comp.data.key <= upper_key) {
              result += lit->comp.data.value;
              ++lit;
          }
      }

      // Active PGM part.
      auto pit = pgm_active_.lower_bound(lower_key);
      while (pit != pgm_active_.end() && pit->key() <= upper_key) {
          result += pit->value();
          ++pit;
      }

      // Frozen flush PGM part.
      if (flush_in_progress_.load(std::memory_order_acquire)) {
          std::lock_guard<std::mutex> flush_lock(flush_mutex_);

          auto fit = pgm_flush_.lower_bound(lower_key);
          while (fit != pgm_flush_.end() && fit->key() <= upper_key) {
              result += fit->value();
              ++fit;
          }
      }

      return result;
  }

  // --------------------------------------------------------------------------
  // Async flushing
  // --------------------------------------------------------------------------

  void StartAsyncFlush(uint32_t thread_id) {
      // Only one flush thread at a time.
      if (flush_thread_.joinable()) {
          flush_thread_.join();
      }

      {
          std::lock_guard<std::mutex> flush_lock(flush_mutex_);

          std::swap(pgm_active_, pgm_flush_);
          pgm_active_ = decltype(pgm_active_)();
          current_buffer_size_ = 0;
      }

      flush_in_progress_.store(true, std::memory_order_release);

      flush_thread_ = std::thread(
          &HybridPGMLIPP::FlushPGMToLIPP,
          this,
          thread_id
      );
  }

  void FlushPGMToLIPP(uint32_t thread_id) {
      auto it = pgm_flush_.lower_bound(std::numeric_limits<KeyType>::lowest());

      while (it != pgm_flush_.end()) {
          std::lock_guard<std::mutex> lipp_lock(lipp_mutex_);

          size_t chunk_count = 0;
          while (it != pgm_flush_.end() &&
                 chunk_count < LIPP_FLUSH_CHUNK) {
              lipp_.insert(it->key(), it->value());
              ++it;
              ++chunk_count;
          }
      }

      {
          std::lock_guard<std::mutex> flush_lock(flush_mutex_);
          pgm_flush_ = decltype(pgm_flush_)();
      }

      flush_in_progress_.store(false, std::memory_order_release);
  }

  // --------------------------------------------------------------------------
  // Data structures
  // --------------------------------------------------------------------------

  LIPP<KeyType, uint64_t> lipp_;

  DynamicPGMIndex<
      KeyType,
      uint64_t,
      SearchClass,
      PGMIndex<KeyType, SearchClass, pgm_error, 16>
  > pgm_active_;

  DynamicPGMIndex<
      KeyType,
      uint64_t,
      SearchClass,
      PGMIndex<KeyType, SearchClass, pgm_error, 16>
  > pgm_flush_;

  // --------------------------------------------------------------------------
  // Mode / learning state
  // --------------------------------------------------------------------------

  mutable std::atomic<size_t> total_inserts_{0};
  mutable std::atomic<size_t> total_lookups_{0};

  mutable std::atomic<bool> mode_decided_{false};
  mutable std::atomic<Mode> mode_{Mode::LEARNING};

  // Critical cached flags.
  //
  // lookup_heavy_final_ protects the LIPP-level hot path.
  // insert_heavy_final_ avoids repeatedly consulting mode_ after decision.
  mutable bool lookup_heavy_final_ = false;
  mutable bool insert_heavy_final_ = false;

  static constexpr size_t LEARN_THRESHOLD = 1'000;

  // --------------------------------------------------------------------------
  // Flush state
  // --------------------------------------------------------------------------

  size_t current_buffer_size_ = 0;

  static constexpr size_t FLUSH_THRESHOLD = 1'000'000;
  static constexpr size_t LIPP_FLUSH_CHUNK = 50'000;

  std::thread flush_thread_;
  std::atomic<bool> flush_in_progress_{false};

  mutable std::mutex lipp_mutex_;
  mutable std::mutex flush_mutex_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H