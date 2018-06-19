//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_test.cpp
//
// Identification: test/codegen/sorter_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <random>

#include "common/harness.h"
#include "common/timer.h"
#include "codegen/util/sorter.h"

namespace peloton {
namespace test {

// What we will be sorting
struct TestTuple {
  uint32_t col_a;
  uint32_t col_b;
  uint32_t col_c;
  uint32_t col_d;
};

// A comparison function that sorts in ascending order on attribute 'col_b' in
// the TestTuples structure.
static int CompareColBAsc(const char *a, const char *b) {
  const auto *at = reinterpret_cast<const TestTuple *>(a);
  const auto *bt = reinterpret_cast<const TestTuple *>(b);
  return at->col_b - bt->col_b;
}

class SorterTest : public PelotonTest {
 public:
  static void LoadSorter(codegen::util::Sorter &sorter, uint64_t num_inserts,
                         uint32_t min_b = 0) {
    std::random_device r;
    std::default_random_engine e(r());
    std::uniform_int_distribution<uint32_t> gen;

    for (uint32_t i = 0; i < num_inserts; i++) {
      auto *tuple = reinterpret_cast<TestTuple *>(sorter.StoreInputTuple());
      tuple->col_a = gen(e) % 100;
      tuple->col_b = min_b + (gen(e) % 1000);
      tuple->col_c = gen(e) % 10000;
      tuple->col_d = gen(e) % 100000;
    }
  }

  static void CheckSorted(const codegen::util::Sorter &sorter, bool ascending) {
    uint32_t last_col_b = std::numeric_limits<uint32_t>::max();
    for (auto iter : sorter) {
      const auto *tt = reinterpret_cast<const TestTuple *>(iter);
      if (last_col_b != std::numeric_limits<uint32_t>::max()) {
        if (ascending) {
          EXPECT_LE(last_col_b, tt->col_b);
        } else {
          EXPECT_GE(last_col_b, tt->col_b);
        }
      }
      last_col_b = tt->col_b;
    }
  }

  void TestSort(uint64_t num_tuples_to_insert) {
    ::peloton::type::EphemeralPool pool;

    {
      codegen::util::Sorter sorter(pool, CompareColBAsc, sizeof(TestTuple));

      // Time this stuff
      Timer<std::ratio<1, 1000>> timer;
      timer.Start();

      // Load the sorter
      LoadSorter(sorter, num_tuples_to_insert);

      timer.Stop();
      LOG_INFO("Loading %" PRId64 " tuples into sort took %.2f ms",
               num_tuples_to_insert, timer.GetDuration());
      timer.Reset();
      timer.Start();

      // Sort
      sorter.Sort();

      timer.Stop();
      LOG_INFO("Sorting %" PRId64 " tuples took %.2f ms", num_tuples_to_insert,
               timer.GetDuration());

      // Check sorted results
      CheckSorted(sorter, true);

      EXPECT_EQ(num_tuples_to_insert, sorter.NumTuples());
    }
  }
};

namespace {

/**
 * Helper class for all parallel sorting tests. To use, just instantiate one of
 * these classes, providing the number of sorter instances you want to create,
 * the sorting function and the tuple size.
 *
 * You can populate all thread-local sorters before calling SortParallel() to
 * perform a parallel sort. Users can then inspect the final result by calling
 * GetMainSorter() to get a handle on the primary/final sorter instance.
 */
class ParallelSortHelper {
 public:
  ParallelSortHelper(uint32_t num_sorters,
                     codegen::util::Sorter::ComparisonFunction func,
                     uint32_t tuple_size)
      : ctx_(nullptr), main_(Pool(), func, tuple_size) {
    // The main sorter is setup (above). Now, set up each thread-local sorter.
    auto &thread_states = GetThreadStates();
    thread_states.Reset(sizeof(codegen::util::Sorter));
    thread_states.Allocate(num_sorters);
    for (auto *sorter : GetTLSorters()) {
      codegen::util::Sorter::Init(*sorter, ctx_, func, tuple_size);
    }
  }

  ~ParallelSortHelper() {
    for (auto *sorter : GetTLSorters()) {
      codegen::util::Sorter::Destroy(*sorter);
    }
  }

  const codegen::util::Sorter &GetMainSorter() const { return main_; }

  std::vector<codegen::util::Sorter *> GetTLSorters() {
    std::vector<codegen::util::Sorter *> tl_sorters;
    GetThreadStates().ForEach<codegen::util::Sorter>(
        0, [&tl_sorters](codegen::util::Sorter *sorter) {
          tl_sorters.push_back(sorter);
        });
    return tl_sorters;
  }

  void SortParallel() {
    Timer<std::milli> timer;
    timer.Start();

    // Sort parallel
    main_.SortParallel(GetThreadStates(), 0);

    timer.Stop();
    LOG_INFO("Parallel sort took: %.2lf ms", timer.GetDuration());
  }

 private:
  executor::ExecutorContext::ThreadStates &GetThreadStates() {
    return ctx_.GetThreadStates();
  }

  ::peloton::type::AbstractPool &Pool() { return *ctx_.GetPool(); }

 private:
  executor::ExecutorContext ctx_;
  codegen::util::Sorter main_;
};

}  // namespace

TEST_F(SorterTest, SimpleSortTest) { TestSort(100); }

TEST_F(SorterTest, BenchmarkSortTest) { TestSort(5000000); }

TEST_F(SorterTest, DISABLED_ParallelSortEmptyTest) {
  constexpr uint32_t num_threads = 4;

  ParallelSortHelper helper(num_threads, CompareColBAsc, sizeof(TestTuple));

  //////////////////////////////////////////////////////////
  /// Don't insert any tuples! Just sort in parallel.
  //////////////////////////////////////////////////////////

  helper.SortParallel();

  //////////////////////////////////////////////////////////
  /// We should still have a valid sorter, but with no tuples
  //////////////////////////////////////////////////////////

  /// Should be sorted (since we have no tuples)
  CheckSorted(helper.GetMainSorter(), true);

  /// Check result size
  EXPECT_EQ(0, helper.GetMainSorter().NumTuples());
}

TEST_F(SorterTest, DISABLED_ParallelSortSmallTest) {
  constexpr uint32_t num_threads = 4;

  //////////////////////////////////////////////////////////
  /// A simple test where sorters have 2 elements each
  //////////////////////////////////////////////////////////
  ParallelSortHelper helper(num_threads, CompareColBAsc, sizeof(TestTuple));

  uint32_t total_num_inserts = 0;

  bool no_insert = false;
  for (auto *sorter : helper.GetTLSorters()) {
    uint32_t to_insert = no_insert ? 0 : 2;
    LoadSorter(*sorter, to_insert);
    no_insert = !no_insert;
    total_num_inserts += to_insert;
  }

  /// Sort
  helper.SortParallel();

  /// Should be sorted
  CheckSorted(helper.GetMainSorter(), true);

  /// Check result size
  EXPECT_EQ(total_num_inserts, helper.GetMainSorter().NumTuples());
}

TEST_F(SorterTest, SimpleParallelSortTest) {
  constexpr uint32_t num_threads = 4;

  ParallelSortHelper helper(num_threads, CompareColBAsc, sizeof(TestTuple));

  //////////////////////////////////////////////////////////
  /// Load each thread-local sorter with ntuples_per_sorter
  /// random tuples each
  //////////////////////////////////////////////////////////

  constexpr uint32_t num_tuples = 5000000;
  const uint32_t ntuples_per_sorter = num_tuples / num_threads;

  for (auto *sorter : helper.GetTLSorters()) {
    LoadSorter(*sorter, ntuples_per_sorter);
  }

  /// Sort
  helper.SortParallel();

  /// Should be sorted
  CheckSorted(helper.GetMainSorter(), true);

  /// Check result size
  EXPECT_EQ(num_tuples, helper.GetMainSorter().NumTuples());
}

TEST_F(SorterTest, ParallelSortNonOverlappingTest) {
  constexpr uint32_t num_threads = 4;

  //////////////////////////////////////////////////////////
  /// TEST #1:
  /// --------
  /// We create 'num_threads' sorter instances, but we vary
  /// the range of values for the column we sort on (column B)
  /// so that they don't entirely overlap. This tests skewed
  /// parallel sorting. For example, the sorters will have
  /// the below ranges of values:
  ///   Sorter_1 : [0, 1000)
  ///   Sorter_2 : [500, 1500)
  ///   Sorter_3 : [1000, 2000)
  ///   Sorter_4 : [2000, 3000)
  //////////////////////////////////////////////////////////
  {
    ParallelSortHelper helper(num_threads, CompareColBAsc, sizeof(TestTuple));

    constexpr uint32_t num_inserts = 1000;
    auto tl_sorters = helper.GetTLSorters();
    for (uint32_t i = 0; i < tl_sorters.size(); i++) {
      LoadSorter(*tl_sorters[i], num_inserts, i * 500);
    }

    /// Sort
    helper.SortParallel();

    /// Should be sorted
    CheckSorted(helper.GetMainSorter(), true);

    /// Check result size
    EXPECT_EQ(num_inserts * num_threads, helper.GetMainSorter().NumTuples());
  }

  //////////////////////////////////////////////////////////
  /// TEST #2:
  /// --------
  /// In this test, we heavily skew only **ONE** sorter's
  /// values for column B to a very high range. The default
  /// range is [0,1000), for the last sorter we change the
  /// range to [4000,5000).
  //////////////////////////////////////////////////////////
  {
    ParallelSortHelper helper(num_threads, CompareColBAsc, sizeof(TestTuple));

    constexpr uint32_t num_inserts = 1000;
    auto tl_sorters = helper.GetTLSorters();
    for (uint32_t i = 0; i < tl_sorters.size(); i++) {
      if (i < tl_sorters.size() - 1) {
        LoadSorter(*tl_sorters[i], num_inserts);
      } else {
        LoadSorter(*tl_sorters[i], num_inserts, 4000);
      }
    }

    /// Sort
    helper.SortParallel();

    /// Should be sorted
    CheckSorted(helper.GetMainSorter(), true);

    /// Check result size
    EXPECT_EQ(num_inserts * num_threads, helper.GetMainSorter().NumTuples());
  }
}

TEST_F(SorterTest, ParallelSortUnbalancedTest) {
  constexpr uint32_t num_threads = 4;

  //////////////////////////////////////////////////////////
  /// A simple test where sorters have different, unbalanced sizes
  //////////////////////////////////////////////////////////
  ParallelSortHelper helper(num_threads, CompareColBAsc, sizeof(TestTuple));

  uint32_t num_inserts = 0;
  auto tl_sorters = helper.GetTLSorters();
  for (uint32_t i = 0; i < tl_sorters.size(); i++) {
    auto *sorter = tl_sorters[i];

    uint32_t to_insert = static_cast<uint32_t>(rand() % 8000) + 200;
    LoadSorter(*sorter, to_insert);

    LOG_DEBUG("TL sorter [%u] has %lu tuples", i, sorter->NumTuples());

    num_inserts += to_insert;
  }

  /// Sort
  helper.SortParallel();

  /// Should be sorted
  CheckSorted(helper.GetMainSorter(), true);

  /// Check result size
  EXPECT_EQ(num_inserts, helper.GetMainSorter().NumTuples());
}

}  // namespace test
}  // namespace peloton