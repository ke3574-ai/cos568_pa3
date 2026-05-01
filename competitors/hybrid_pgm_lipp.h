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
    PrintHitStats();
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

  void PrintWindowStats() const {
      size_t ins = window_inserts_.exchange(0);
      size_t lkp = window_lookups_.exchange(0);

      double ratio = (double)lkp / (ins + lkp + 1);

        if (ratio > 0.7) {
            mode_.store(Mode::LOOKUP_HEAVY, std::memory_order_relaxed);
        } else {
            mode_.store(Mode::INSERT_HEAVY, std::memory_order_relaxed);
        }

      std::cout << "[HybridPGMLIPP] Window Stats: "
                << "Inserts=" << ins
                << " Lookups=" << lkp
                << " LookupRatio=" << ratio
                << "\n";
  }

    void MaybeSetModeOnce() const {
        if (mode_decided_.load(std::memory_order_relaxed)) return;

        size_t ins = total_inserts_.load(std::memory_order_relaxed);
        size_t lkp = total_lookups_.load(std::memory_order_relaxed);
        size_t total = ins + lkp;

        if (total < LEARN_THRESHOLD) return;

        double ratio = (double)lkp / (total + 1);

        if (ratio > 0.7) {
        mode_.store(Mode::LOOKUP_HEAVY, std::memory_order_relaxed);
        } else {
        mode_.store(Mode::INSERT_HEAVY, std::memory_order_relaxed);
        }

        mode_decided_.store(true, std::memory_order_relaxed);
    }


    void PrintHitStats() const {
        size_t l = lipp_hits_.load();
        size_t p = pgm_hits_.load();
        size_t m = misses_.load();
        size_t total = l + p + m;

        std::cout << "[HitStats] LIPP=" << l
                  << " PGM=" << p
                  << " MISS=" << m
                  << " LIPP_ratio=" << (double)l / (total + 1)
                  << " PGM_ratio=" << (double)p / (total + 1)
                  << "\n";
    }
    
    size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
        uint64_t value;

        // FAST PATH: after lookup-heavy mode is fully materialized,
        // behave like pure LIPP.
        if (mode_.load(std::memory_order_relaxed) == Mode::LOOKUP_HEAVY &&
            materialized_.load(std::memory_order_acquire)) {
            return lipp_.find(lookup_key, value) ? value : util::NOT_FOUND;
        }

        // Learning / insert-heavy path.
        total_lookups_.fetch_add(1, std::memory_order_relaxed);

        if (!mode_decided_.load(std::memory_order_relaxed)) {
            MaybeSetModeOnce();
        }

        Mode m = mode_.load(std::memory_order_relaxed);

        if (m == Mode::LOOKUP_HEAVY) {
            // Not materialized yet: safe fallback.
            if (lipp_.find(lookup_key, value)) return value;

            auto it = pgm_active_.find(lookup_key);
            return (it == pgm_active_.end()) ? util::NOT_FOUND : it->value();
        }

        if (m == Mode::INSERT_HEAVY) {
            auto it = pgm_active_.find(lookup_key);
            if (it != pgm_active_.end()) return it->value();

            return lipp_.find(lookup_key, value) ? value : util::NOT_FOUND;
        }

        // LEARNING: LIPP first, then PGM.
        if (lipp_.find(lookup_key, value)) return value;

        auto it = pgm_active_.find(lookup_key);
        return (it == pgm_active_.end()) ? util::NOT_FOUND : it->value();
}
  
    // size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {

    //     uint64_t value;

    //     total_lookups_.fetch_add(1, std::memory_order_relaxed);
    //     window_lookups_.fetch_add(1, std::memory_order_relaxed);

    //     size_t window = window_inserts_.load() + window_lookups_.load();

    //     if (window >= 500000 &&
    //         !stats_in_progress_.exchange(true, std::memory_order_acquire)) {

    //         PrintWindowStats();

    //         stats_in_progress_.store(false, std::memory_order_release);
    //     }

    //     Mode m = mode_.load(std::memory_order_relaxed);

    //     // 🔴 LOOKUP_HEAVY → LIPP first
    //     if (m == Mode::LOOKUP_HEAVY) {
    //         {
    //             std::lock_guard<std::mutex> lock(lipp_mutex_);
    //             if (lipp_.find(lookup_key, value)) {
    //                 lookup_count_++;
    //                 return value;
    //             }
    //         }
    //     }

    //     // 🔵 Always check PGM (fallback or primary)
    //     auto it = pgm_active_.find(lookup_key);
    //     if (it != pgm_active_.end()) {
    //         lookup_count_++;
    //         return it->value();
    //     }

    //     if (flush_in_progress_.load(std::memory_order_acquire)) {
    //         std::lock_guard<std::mutex> lock(flush_mutex_);
    //         auto it2 = pgm_flush_.find(lookup_key);
    //         if (it2 != pgm_flush_.end()) {
    //             lookup_count_++;
    //             return it2->value();
    //         }
    //     }

    //     // 🔵 INSERT_HEAVY → LIPP fallback
    //     if (m == Mode::INSERT_HEAVY) {
    //         std::lock_guard<std::mutex> lock(lipp_mutex_);
    //         if (lipp_.find(lookup_key, value)) {
    //             lookup_count_++;
    //             return value;
    //         }
    //     }

    //     lookup_count_++;
    //     return util::OVERFLOW;
    // }

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

        total_lookups_.fetch_add(1, std::memory_order_relaxed);
        window_lookups_.fetch_add(1, std::memory_order_relaxed);

        Mode m = mode_.load(std::memory_order_relaxed);

        // 🔴 LOOKUP_HEAVY → LIPP first
        if (m == Mode::LOOKUP_HEAVY) {
            {
                std::lock_guard<std::mutex> lock(lipp_mutex_);
                auto it = lipp_.lower_bound(lower_key);
                while (it != lipp_.end() && it->comp.data.key <= upper_key) {
                    result += it->comp.data.value;
                    ++it;
                }
            }
        }

        // 🔵 Always scan PGM (fallback or primary)
        auto it = pgm_active_.lower_bound(lower_key);
        while (it != pgm_active_.end() && it->key() <= upper_key) {
            result += it->value();
            ++it;
        }

        if (flush_in_progress_.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(flush_mutex_);
            auto it2 = pgm_flush_.lower_bound(lower_key);
            while (it2 != pgm_flush_.end() && it2->key() <= upper_key) {
                result += it2->value();
                ++it2;
            }
        }

        // 🔵 INSERT_HEAVY → LIPP fallback
        if (m == Mode::INSERT_HEAVY) {
            std::lock_guard<std::mutex> lock(lipp_mutex_);
            auto it = lipp_.lower_bound(lower_key);
            while (it != lipp_.end() && it->comp.data.key <= upper_key) {
                result += it->comp.data.value;
                ++it;
            }
        }

        return result;
    }

//   void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {

//       pgm_active_.insert(data.key, data.value);
//       current_buffer_size_++;
//       // insert_count_++;
//       total_inserts_.fetch_add(1, std::memory_order_relaxed);
//       window_inserts_.fetch_add(1, std::memory_order_relaxed);
      
//       size_t window = window_inserts_.load() + window_lookups_.load();

//         if (window >= 500000 &&
//             !stats_in_progress_.exchange(true, std::memory_order_acquire)) {

//             PrintWindowStats();

//             stats_in_progress_.store(false, std::memory_order_release);
//         }

//       Mode m = mode_.load(std::memory_order_relaxed);

//         if (m == Mode::LOOKUP_HEAVY) {
//             flush_threshold_ = 50'000;
//         } else {
//             flush_threshold_ = 1'000'000;
//         }

//       if (current_buffer_size_ >= flush_threshold_) {

//           // Wait for previous flush (minimal version)
//           if (flush_thread_.joinable()) {
//               flush_thread_.join();
//           }

//           size_t flush_size = current_buffer_size_;
//           current_buffer_size_ = 0;

//           {
//               std::lock_guard<std::mutex> lock(flush_mutex_);
//               std::swap(pgm_active_, pgm_flush_);
//           }

//           flush_in_progress_.store(true, std::memory_order_release);

//           flush_thread_ = std::thread(
//               &HybridPGMLIPP::FlushInsertToLIPP,
//               this,
//               flush_size,
//               thread_id
//           );
//       }
//   }

void MaybeDecideModeLight() const {
    if (mode_decided_.load(std::memory_order_relaxed)) return;

    size_t ins = total_inserts_.load(std::memory_order_relaxed);
    size_t lkp = total_lookups_.load(std::memory_order_relaxed);
    size_t total = ins + lkp;

    if (total < LEARN_THRESHOLD) return;

    double ratio = (double)lkp / (total + 1);

    mode_.store(ratio > 0.7 ? Mode::LOOKUP_HEAVY : Mode::INSERT_HEAVY,
                std::memory_order_relaxed);

    mode_decided_.store(true, std::memory_order_relaxed);
}

void EnsureMaterialized(uint32_t thread_id) {
    if (materialized_.exchange(true, std::memory_order_acq_rel)) return;

    // Move all PGM data into LIPP
    for (auto it = pgm_active_.lower_bound(std::numeric_limits<KeyType>::min());
         it != pgm_active_.end(); ++it) {
        lipp_.insert(it->key(), it->value());
    }

    // Clear PGM
    pgm_active_ = decltype(pgm_active_)();
}

// void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {

//     total_inserts_.fetch_add(1, std::memory_order_relaxed);
//     if (!mode_decided_.load(std::memory_order_relaxed)) {
//         MaybeSetModeOnce();
//     }
//     // MaybeUpdateMode();

//     Mode m = mode_.load(std::memory_order_relaxed);

//     // 🔴 PURE LIPP
//     if (m == Mode::LOOKUP_HEAVY) {
//         // std::lock_guard<std::mutex> lock(lipp_mutex_);
//         lipp_.insert(data.key, data.value);
//         return;
//     }

//     // 🔵 PURE PGM
//     if (m == Mode::INSERT_HEAVY) {
//         pgm_active_.insert(data.key, data.value);
//         return;
//     }

//     // 🟡 LEARNING phase → behave like PGM (safe + fast)
//     pgm_active_.insert(data.key, data.value);
// }

void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {

    total_inserts_.fetch_add(1, std::memory_order_relaxed);

    if (!mode_decided_.load(std::memory_order_relaxed)) {
        MaybeSetModeOnce();
    }

    Mode m = mode_.load(std::memory_order_relaxed);

    if (m == Mode::LOOKUP_HEAVY) {
        if (!materialized_.load(std::memory_order_acquire)) {
            EnsureMaterialized(thread_id);
        }

        lipp_.insert(data.key, data.value);
        return;
    }

    if (m == Mode::INSERT_HEAVY) {
        pgm_active_.insert(data.key, data.value);
        return;
    }

    // LEARNING
    pgm_active_.insert(data.key, data.value);
}

void EnsureMaterialized(uint32_t thread_id) {
    if (materialized_.exchange(true, std::memory_order_acq_rel)) return;

    for (auto it = pgm_active_.lower_bound(std::numeric_limits<KeyType>::min());
         it != pgm_active_.end(); ++it) {
        lipp_.insert(it->key(), it->value());
    }

    pgm_active_ = decltype(pgm_active_)();
}

  std::string name() const { 
    // std::cout << "[HybridPGMLIPP] Initialized\n";
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
  size_t flush_threshold_ = 50'000;
  size_t current_buffer_size_ = 0;

  mutable size_t insert_count_ = 0;
  mutable size_t lookup_count_ = 0;

  mutable std::atomic<size_t> total_inserts_{0};
  mutable std::atomic<size_t> total_lookups_{0};
  mutable std::atomic<size_t> window_inserts_{0};
  mutable std::atomic<size_t> window_lookups_{0};
  mutable std::atomic<bool> stats_in_progress_{false};

  mutable std::atomic<size_t> lipp_hits_{0};
  mutable std::atomic<size_t> pgm_hits_{0};
  mutable std::atomic<size_t> misses_{0};

  mutable std::atomic<bool> mode_decided_{false};
  static constexpr size_t LEARN_THRESHOLD = 50'000;

  mutable std::atomic<bool> materialized_{false};

  std::thread flush_thread_;
  std::atomic<bool> flush_in_progress_{false};

  mutable std::mutex flush_mutex_;  // protects pgm_flush_
  mutable std::mutex lipp_mutex_;   // protects lipp_

  bool is_built_ = false;

  enum class Mode {
    LEARNING,
    INSERT_HEAVY,
    LOOKUP_HEAVY
  };

  mutable std::atomic<Mode> mode_{Mode::LEARNING};

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

        // std::cout << "[HybridPGMLIPP] Window Stats: "
        //         << "Inserts=" << ins
        //         << " Lookups=" << lkp
        //         << " LookupRatio=" << ratio
        //         << "\n";

      flush_in_progress_.store(false, std::memory_order_release);
      return true;
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H