//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.cpp
//
// Identification: src/codegen/util/sorter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/sorter.h"

#include <algorithm>
#include <queue>

#include "common/synchronization/count_down_latch.h"
#include "common/timer.h"
#include "threadpool/mono_queue_pool.h"

namespace peloton {
namespace codegen {
namespace util {

Sorter::Sorter(::peloton::type::AbstractPool &memory, ComparisonFunction func,
               uint32_t tuple_size)
    : memory_(memory),
      cmp_func_(func),
      tuple_size_(tuple_size),
      buffer_pos_(nullptr),
      buffer_end_(nullptr),
      next_alloc_size_(kInitialBufferSize),
      tuples_start_(nullptr),
      tuples_end_(nullptr) {
  // No memory allocation
  LOG_DEBUG("Initialized Sorter for tuples of size %u bytes", tuple_size_);
}

Sorter::~Sorter() {
  uint64_t total_alloc = 0;
  for (const auto &iter : blocks_) {
    void *block = iter.first;
    total_alloc += iter.second;
    PELOTON_ASSERT(block != nullptr);
    memory_.Free(block);
  }
  buffer_pos_ = buffer_end_ = nullptr;
  tuples_start_ = tuples_end_ = nullptr;
  next_alloc_size_ = 0;

  LOG_DEBUG("Cleaned up %zu tuples from %zu blocks of memory (%.2lf KB)",
            tuples_.size(), blocks_.size(), total_alloc / 1024.0);
}

void Sorter::Init(Sorter &sorter, executor::ExecutorContext &exec_ctx,
                  ComparisonFunction func, uint32_t tuple_size) {
  new (&sorter) Sorter(*exec_ctx.GetPool(), func, tuple_size);
}

void Sorter::Destroy(Sorter &sorter) { sorter.~Sorter(); }

char *Sorter::StoreInputTuple() {
  // Make room for a new tuple
  MakeRoomForNewTuple();

  // Bump the position pointer, return location where call can write a tuple
  char *ret = buffer_pos_;
  buffer_pos_ += tuple_size_;

  // Track tuple position
  tuples_.push_back(ret);

  // Finish
  return ret;
}

void Sorter::Sort() {
  // Short-circuit
  if (tuples_.empty()) {
    return;
  }

  // Time it
  Timer<std::milli> timer;
  timer.Start();

  // Sort the sucker
  // TODO(pmenon): The standard std::sort is super slow. We should consider a
  //               switch to IPS4O which is up to 3-4x faster.
  auto cmp = [this](char *l, char *r) { return cmp_func_(l, r) < 0; };
  std::sort(tuples_.begin(), tuples_.end(), cmp);

  // Setup pointers
  tuples_start_ = tuples_.data();
  tuples_end_ = tuples_start_ + tuples_.size();

  timer.Stop();

  LOG_DEBUG("Sorted %zu tuples in %.2f ms", tuples_.size(),
            timer.GetDuration());
}

namespace {

// Structure we use to track a package of merging work.
struct MergeWork {
  using InputRange = std::pair<char **, char **>;

  std::vector<InputRange> input_ranges;
  char **destination = nullptr;

  MergeWork(std::vector<InputRange> &&inputs, char **dest)
      : input_ranges(std::move(inputs)), destination(dest) {}
};

}  // namespace

void Sorter::SortParallel(
    const executor::ExecutorContext::ThreadStates &thread_states,
    uint32_t sorter_offset) {
  // The main comparison function to compare two tuples
  const auto comp = [this](char *l, char *r) { return cmp_func_(l, r) < 0; };

  // Collect all non-empty sorter instances
  uint64_t num_tuples = 0;
  std::vector<Sorter *> sorters;
  thread_states.ForEach<Sorter>(sorter_offset,
                                [&num_tuples, &sorters](Sorter *sorter) {
                                  if (sorter->NumTuples() > 0) {
                                    sorters.push_back(sorter);
                                    num_tuples += sorter->NumTuples();
                                  }
                                });

  // If all sorters are empty, there isn't anything to sort and we're done
  if (sorters.empty()) {
    PELOTON_ASSERT(num_tuples == 0);
    return;
  }

  /*
   * If the total number of tuples across **ALL** sorter instances is less than
   * the kMinTuplesForParallelSort threshold value, then we use a simpler 
   * single-threaded sort. This will likely be faster due to less overhead in
   * spawning sort and merge jobs. This value was found empirically, but might
   * be a good candidate for learning based on tuples sizes, CPU speeds, caches,
   * algorithms, etc.
   */
#ifndef NDEBUG
  if (num_tuples < kMinTuplesForParallelSort) {
    // Resize to accommodate all tuples
    tuples_.reserve(num_tuples);

    // Move all thread-local sorter data to the main sorter instance
    for (auto *sorter : sorters) {
      tuples_.insert(tuples_.end(), sorter->tuples_.begin(),
                     sorter->tuples_.end());
      sorter->TransferMemoryBlocks(*this);
    }

    // Sort single-threaded
    Sort();

    // Finish
    return;
  }
#endif

  /*
   * At this point, we're fairly certain we'll need a full blown parallel sort.
   * Parallel sorting works as follows. We begin by sorting each thread-local 
   * sorter (collected earlier and which are non-empty) in parallel using the 
   * worker pool. We then partition each thread-local sorter into B buckets, 
   * thus also partitioning the output into B buckets. Each thread-local sorter
   * finds B-1 splitter keys that "evenly" split its contents into B buckets.
   * To handle skew in data between sorters, we take the median value of each of
   * the B-1 candidate set of splitter keys. For each splitter key, we find all
   * input ranges in all sorter instances, and compute the output position in
   * the final result. We combine these two elements into what we call a merge
   * package. Merge packages are independent pieces of work that are issued in
   * parallel across a set of worker threads. Finally, we need to transfer all
   * thread-local tuple data into this sorter instance so that the thread-local
   * instances can be discarded immediately.
   */

  // There are some tuples to sort, set up some space
  tuples_.resize(num_tuples);
  tuples_start_ = tuples_.data();
  tuples_end_ = tuples_start_ + num_tuples;

  // The worker pool we use to execute parallel work
  auto &work_pool = threadpool::MonoQueuePool::GetExecutionInstance();

  Timer<std::milli> timer;
  timer.Start();

  ////////////////////////////////////////////////////////////////////
  /// Step 1 - Sort each thread local run in parallel
  ////////////////////////////////////////////////////////////////////
  {
    common::synchronization::CountDownLatch latch(sorters.size());

    for (auto *sorter : sorters) {
      work_pool.SubmitTask([sorter, &latch]() {
        // Sort
        sorter->Sort();
        // Count down latch
        latch.CountDown();
      });
    }

    // Wait for sort jobs to complete
    latch.Await(0);
  }

  timer.Stop();
  LOG_DEBUG("Total thread-local sort time: %.2lf ms", timer.GetDuration());
  timer.Reset();
  timer.Start();

  ////////////////////////////////////////////////////////////////////
  /// Step 2 - Compute splitter keys
  ////////////////////////////////////////////////////////////////////

  /*
   * Let B be the number of buckets we wish to decompose our input into, let N
   * be the number of sorter instances we have; then, splitters is a [B-1 x N]
   * matrix. splitters[i][j] indicates the i-th splitter key found in the j-th
   * sorter instance. Thus, each row of the matrix contains a list of candidate
   * keys (from each sorter) that split partition i and i+1, and each column
   * contains all splitter keys for a single sorter.
   */
  auto num_buckets = work_pool.NumWorkers();
  std::vector<std::vector<char *>> splitters(num_buckets - 1);

  {
    for (auto *sorter : sorters) {
      if (sorter->NumTuples() < num_buckets - 1) {
        for (uint32_t i = 0; i < sorter->NumTuples(); i++) {
          splitters[i].push_back(sorter->tuples_[i]);
        }
      } else {
        auto part_size = sorter->NumTuples() / num_buckets;
        PELOTON_ASSERT(part_size > 0);
        for (uint32_t i = 0; i < num_buckets - 1; i++) {
          splitters[i].push_back(sorter->tuples_[(i + 1) * part_size]);
        }
      }
    }
  }

  timer.Stop();
  LOG_DEBUG("Splitter construction time: %.2lf ms", timer.GetDuration());
  timer.Reset();
  timer.Start();

  // Where the merging work units are collected
  std::vector<MergeWork> merge_work;

  ////////////////////////////////////////////////////////////////////
  /// Step 3 - Compute disjoint merge ranges from splitters
  ////////////////////////////////////////////////////////////////////
  {
    // This tracks the current position in the global output where the next
    // merge package will begin writing results into. At first, it points to the
    // start of the array. As we generate merge packages, we calculate the next
    // position by computing the sizes of the merge packages. We've already
    // perfectly sized the output so this memory is allocated and ready to be
    // written to.
    char **write_pos = tuples_.data();

    // This vector tracks, for each sorter, the position of the start of the
    // input range. As we move through the splitters, we bump this pointer so
    // that we don't need to perform two binary searches to find the lower and
    // upper range around the splitter key.
    std::vector<char **> next_start(sorters.size());

    for (uint32_t idx = 0; idx < splitters.size(); idx++) {
      if (splitters[idx].empty()) continue;

      // Sort the local separators and choose the median
      std::sort(splitters[idx].begin(), splitters[idx].end(), comp);

      // Find the median-of-medians splitter key
      char *splitter = splitters[idx][sorters.size() / 2];

      // The vector where we collect all input ranges that feed the merge work
      std::vector<MergeWork::InputRange> input_ranges;

      uint64_t part_size = 0;
      for (uint32_t sorter_idx = 0; sorter_idx < sorters.size(); sorter_idx++) {
        // Get the [start,end) range in the current sorter such that
        // start <= splitter < end
        Sorter *sorter = sorters[sorter_idx];
        char **start =
            (idx == 0 ? sorter->tuples_.data() : next_start[sorter_idx]);
        char **end = sorter->tuples_.data() + sorter->tuples_.size();
        if (idx < splitters.size() - 1) {
          end = std::upper_bound(start, end, splitter, comp);
        }

        // If the the range [start, end) is non-empty, push it in as work
        if (start != end) {
          input_ranges.emplace_back(start, end);
        }

        part_size += (end - start);
        next_start[sorter_idx] = end;
      }

      // Add work
      merge_work.emplace_back(std::move(input_ranges), write_pos);

      // Bump new write position
      write_pos += part_size;
    }
  }

  timer.Stop();
  LOG_DEBUG("Work generation time: %.2lf ms", timer.GetDuration());
  timer.Reset();
  timer.Start();

  //////////////////////////////////////////////////////////////////
  /// Step 4 - Distribute work packages to workers in parallel
  //////////////////////////////////////////////////////////////////
  {
    common::synchronization::CountDownLatch latch{merge_work.size()};
    auto heap_cmp =
        [this](const MergeWork::InputRange &l, const MergeWork::InputRange &r) {
          return !(cmp_func_(*l.first, *r.first) < 0);
        };
    for (auto &work : merge_work) {
      work_pool.SubmitTask([&work, &latch, &heap_cmp] {
        std::priority_queue<MergeWork::InputRange,
                            std::vector<MergeWork::InputRange>,
                            decltype(heap_cmp)> heap(heap_cmp,
                                                     work.input_ranges);
        char **dest = work.destination;
        while (!heap.empty()) {
          auto top = heap.top();
          heap.pop();
          *dest++ = *top.first;
          if (top.first + 1 != top.second) {
            heap.emplace(top.first + 1, top.second);
          }
        }

        latch.CountDown();
      });
    }

    // Wait
    latch.Await(0);
  }

  //////////////////////////////////////////////////////////////////
  /// Step 5 - Transfer ownership of thread-local memory
  //////////////////////////////////////////////////////////////////
  {
    for (auto *sorter : sorters) {
      sorter->TransferMemoryBlocks(*this);
    }
  }

  timer.Stop();
  LOG_DEBUG("Merging sorted runs time: %.2lf ms", timer.GetDuration());
}

void Sorter::MakeRoomForNewTuple() {
  bool has_room =
      (buffer_pos_ != nullptr && buffer_pos_ + tuple_size_ < buffer_end_);
  if (has_room) {
    return;
  }

  PELOTON_ASSERT(next_alloc_size_ >= tuple_size_);

  LOG_TRACE("Allocating block of size %.2lf KB ...", next_alloc_size_ / 1024.0);

  // We need to allocate another block
  void *block = memory_.Allocate(next_alloc_size_);
  blocks_.emplace_back(block, next_alloc_size_);

  // Setup new buffer boundaries
  buffer_pos_ = reinterpret_cast<char *>(block);
  buffer_end_ = buffer_pos_ + next_alloc_size_;

  next_alloc_size_ *= 2;
}

void Sorter::TransferMemoryBlocks(Sorter &target) {
  // Move all blocks we've allocated into the target's block list
  auto &target_blocks = target.blocks_;
  target_blocks.insert(target_blocks.end(), blocks_.begin(), blocks_.end());

  // Clear out
  tuples_.clear();
  blocks_.clear();
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
