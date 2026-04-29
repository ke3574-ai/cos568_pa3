#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <limits>

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
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {

    if (is_built_) {
        std::cout << "[HybridPGMLIPP] WARNING: Build() invoked more than once\n";
    }

    is_built_ = true;
    // TODO: use num_threads for parallel loading or preprocessing
    std::cout << "[HybridPGMLIPP] Build started with " << data.size() << " elements\n";
    std::vector<std::pair<KeyType, uint64_t>> loading_data;

    loading_data.reserve(data.size());
    for (const auto& itm : data) {
        loading_data.emplace_back(itm.key, itm.value);
    }

    uint64_t build_time = util::timing(
      [&] { lipp_.bulk_load(loading_data.data(), loading_data.size()); });
    
    std::cout << "[HybridPGMLIPP] Build finished in " << build_time << " ns\n";

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

      // 1. Check active buffer (newest)
      auto it = pgm_active_.find(lookup_key);
      if (it != pgm_active_.end()) {
          lookup_count_++;
          return it->value();
      }

      // 2. Check flush buffer (recent, in-flight)
      it = pgm_flush_.find(lookup_key);
      if (it != pgm_flush_.end()) {
          lookup_count_++;
          return it->value();
      }

      // 3. Check LIPP (base)
      if (lipp_.find(lookup_key, value)) {
          lookup_count_++;
          return value;
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

      // 1. Active buffer
      auto it = pgm_active_.lower_bound(lower_key);
      while (it != pgm_active_.end() && it->key() <= upper_key) {
          result += it->value();
          ++it;
      }

      // 2. Flush buffer
      it = pgm_flush_.lower_bound(lower_key);
      while (it != pgm_flush_.end() && it->key() <= upper_key) {
          result += it->value();
          ++it;
      }

      // 3. LIPP
      auto lipp_it = lipp_.lower_bound(lower_key);
      while (lipp_it != lipp_.end() && lipp_it->comp.data.key <= upper_key) {
          result += lipp_it->comp.data.value;
          ++lipp_it;
      }

      return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {


    pgm_active_.insert(data.key, data.value);
    current_buffer_size_++;
    insert_count_++;
    if (current_buffer_size_ >= flush_threshold_)
    {

      // Swap active and flush buffers
      std::swap(pgm_active_, pgm_flush_);
      size_t flush_size = current_buffer_size_;
      current_buffer_size_ = 0;

      auto res = FlushInsertToLIPP(flush_size, thread_id);

      if (!res){
        std::cerr << "Critical: Flush to LIPP failed!" << std::endl;
      }
    }
    
    // std::cout << "[HybridPGMLIPP] Insert Finished!\n";

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

  DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_active_;
  DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_flush_;

  // Control Logic
  //@todo, in the future, can pass this value from params[0] in the cosnt
  size_t flush_threshold_ = 500000;
  size_t current_buffer_size_ = 0;

  size_t insert_count_ = 0;
  size_t lookup_count_ = 0;

  bool is_built_ = false;

  bool FlushInsertToLIPP(const size_t flush_size, uint32_t thread_id) {

    std::vector<std::pair<KeyType, uint64_t>> buffer;
    buffer.reserve(flush_size);

    for (auto it = pgm_flush_.lower_bound(std::numeric_limits<KeyType>::min());
         it != pgm_flush_.end(); ++it) {
        buffer.emplace_back(it->key(), it->value());
    }

    std::sort(buffer.begin(), buffer.end());

    for (const auto& [key, value] : buffer) {
        lipp_.insert(key, value);
    }
    // 2. Reset the PGM index 
    // This calls the default constructor and replaces the old object
    pgm_flush_ = decltype(pgm_flush_)();

    // 3. Reset your tracking counter
    std::cout << "[HybridPGMLIPP] Flush size=" << buffer.size() << "\n";
    return true;
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H
