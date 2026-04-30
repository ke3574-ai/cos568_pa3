#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <limits>
#include <thread>
#include <mutex>
#include <atomic>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

/**
 * @brief
 * 
 * @todo
 * 1. Create a class that has both LIPP and PGM
 * 2. Simple rule to interact with both of them
 * 3. Add the flushing strategy
 * 4. Test on a small dataset that can be run from this machine
*/
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params){}

  ~HybridPGMLIPP() {
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {

      if (is_built_) {
          std::cout << "[HybridPGMLIPP] WARNING: Build() invoked more than once\n";
      }

      is_built_ = true;

      // std::cout << "[HybridPGMLIPP] Build started with "
      //           << data.size() << " elements\n";

      std::vector<std::pair<KeyType, uint64_t>> loading_data;
      loading_data.reserve(data.size());

      for (const auto& itm : data) {
          loading_data.emplace_back(itm.key, itm.value);
      }

      uint64_t build_time = util::timing(
          [&] { lipp_.bulk_load(loading_data.data(), loading_data.size()); });

    //   std::cout << "[HybridPGMLIPP] Build finished in "
    //             << build_time << " ns\n";

      return build_time;
  }

  // size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {

  //   uint64_t value;
  //   bool found = false;
  //   if (lipp_.find(lookup_key, value)){
  //       found = true;
  //   }
  //   //@todo check if inserts are unique or not. Otherwise, have to check both ogm ad lipp and return newer data
  //   if (!found)
  //   {
  //     auto it = pgm_.find(lookup_key);
  //     if (it == pgm_.end()) {
  //       value = util::OVERFLOW;
  //     } else {
  //       value = it->value();
  //     }
  //   }

  //   lookup_count_++;

  //   return value;
  // }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {

      uint64_t value;
        total_lookups_++;
        window_lookups_++;
      // 1. LIPP (fast path)
      {
          std::lock_guard<std::mutex> lock(lipp_mutex_);
          if (lipp_.find(lookup_key, value)) {
              lookup_count_++;
              return value;
          }
      }

      // 2. Active buffer
      auto it = pgm_active_.find(lookup_key);
      if (it != pgm_active_.end()) {
          lookup_count_++;
          return it->value();
      }

      // 3. Flush buffer
      if (flush_in_progress_.load(std::memory_order_acquire)) {
          std::lock_guard<std::mutex> lock(flush_mutex_);
          auto it2 = pgm_flush_.find(lookup_key);
          if (it2 != pgm_flush_.end()) {
              lookup_count_++;
              return it2->value();
          }
      }

      lookup_count_++;
      return util::OVERFLOW;
  }

  // uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {

  //   auto pgm_it = pgm_.lower_bound(lower_key);
  //   uint64_t result = 0;
  //   while(pgm_it != pgm_.end() && pgm_it->key() <= upper_key){
  //     result += pgm_it->value();
  //     ++pgm_it;
  //   }

  //   auto lipp_it = lipp_.lower_bound(lower_key);
  //   while(lipp_it != lipp_.end() && lipp_it->comp.data.key <= upper_key){
  //     result += lipp_it->comp.data.value;
  //     ++lipp_it;
  //   }

  //   return result;
  // }

  uint64_t RangeQuery(const KeyType& lower_key,
                    const KeyType& upper_key,
                    uint32_t thread_id) const {

      uint64_t result = 0;
      total_lookups_++;
      window_lookups_++;
      // 1. Active buffer
      auto it = pgm_active_.lower_bound(lower_key);
      while (it != pgm_active_.end() && it->key() <= upper_key) {
          result += it->value();
          ++it;
      }

      // 2. Flush buffer
      if (flush_in_progress_.load(std::memory_order_acquire)) {
          std::lock_guard<std::mutex> lock(flush_mutex_);

          auto it2 = pgm_flush_.lower_bound(lower_key);
          while (it2 != pgm_flush_.end() && it2->key() <= upper_key) {
              result += it2->value();
              ++it2;
          }
      }

      // 3. LIPP
      {
          std::lock_guard<std::mutex> lock(lipp_mutex_);

          auto lipp_it = lipp_.lower_bound(lower_key);
          while (lipp_it != lipp_.end() && lipp_it->comp.data.key <= upper_key) {
              result += lipp_it->comp.data.value;
              ++lipp_it;
          }
      }

      return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {

      pgm_active_.insert(data.key, data.value);
      current_buffer_size_++;
      insert_count_++;
      total_inserts_++;
      window_inserts_++;

      if (current_buffer_size_ >= flush_threshold_) {

          // Wait for previous flush (minimal version)
          if (flush_thread_.joinable()) {
              flush_thread_.join();
          }

          size_t flush_size = current_buffer_size_;
          current_buffer_size_ = 0;

          {
              std::lock_guard<std::mutex> lock(flush_mutex_);
              std::swap(pgm_active_, pgm_flush_);
          }

          flush_in_progress_.store(true, std::memory_order_release);

          flush_thread_ = std::thread(
              &HybridPGMLIPP::FlushInsertToLIPP,
              this,
              flush_size,
              thread_id
          );
      }
  }

  std::string name() const { 
    std::cout << "[HybridPGMLIPP] Initialized\n";
    return "HybridPGMLIPP";  }

  std::size_t size() const { return pgm_active_.size_in_bytes() + pgm_flush_.size_in_bytes() + lipp_.index_size(); }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
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

  //Data storage engines
  // DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_;
  LIPP<KeyType, uint64_t> lipp_;
  // std::vector<std::pair<KeyType, uint64_t>> base_data_;

  DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_active_;
  DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_flush_;

  // Control Logic
  //@todo, in the future, can pass this value from params[0] in the cosnt
  size_t flush_threshold_ = 500000;
  size_t current_buffer_size_ = 0;

  mutable size_t insert_count_ = 0;
  mutable size_t lookup_count_ = 0;

  std::atomic<size_t> total_inserts_{0};
  std::atomic<size_t> total_lookups_{0};
  std::atomic<size_t> window_inserts_{0};
  std::atomic<size_t> window_lookups_{0};

  std::thread flush_thread_;
  std::atomic<bool> flush_in_progress_{false};

  mutable std::mutex flush_mutex_;  // protects pgm_flush_
  mutable std::mutex lipp_mutex_;   // protects lipp_

  bool is_built_ = false;

  bool FlushInsertToLIPP(const size_t flush_size, uint32_t thread_id) {

      std::vector<std::pair<KeyType, uint64_t>> buffer;
      buffer.reserve(flush_size);

      {
          std::lock_guard<std::mutex> lock(flush_mutex_);

          for (auto it = pgm_flush_.lower_bound(std::numeric_limits<KeyType>::min());
              it != pgm_flush_.end(); ++it) {
              buffer.emplace_back(it->key(), it->value());
          }
      }

      std::sort(buffer.begin(), buffer.end());

      {
          std::lock_guard<std::mutex> lock(lipp_mutex_);

          for (const auto& [key, value] : buffer) {
              lipp_.insert(key, value);
          }
      }

      {
          std::lock_guard<std::mutex> lock(flush_mutex_);
          pgm_flush_ = decltype(pgm_flush_)();
      }

        size_t ins = window_inserts_.exchange(0);
        size_t lkp = window_lookups_.exchange(0);

        double ratio = (double)lkp / (ins + lkp + 1);

        std::cout << "[HybridPGMLIPP] Window Stats: "
                << "Inserts=" << ins
                << " Lookups=" << lkp
                << " LookupRatio=" << ratio
                << "\n";

      flush_in_progress_.store(false, std::memory_order_release);
      return true;
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H
