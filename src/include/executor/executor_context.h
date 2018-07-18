//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.h
//
// Identification: src/include/executor/executor_context.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/query_parameters.h"
#include "type/ephemeral_pool.h"
#include "type/value.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace storage {
class StorageManager;
}  // namespace storage

namespace executor {

/**
 * @brief Stores information for one execution of a plan.
 */
class ExecutorContext {
 public:
  /// Constructor
  ExecutorContext(concurrency::TransactionContext *transaction,
                  codegen::QueryParameters parameters = {});

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(ExecutorContext);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Return the transaction for this particular query execution
  concurrency::TransactionContext *GetTransaction() const;

  /// Return the explicit set of parameters for this particular query execution
  const std::vector<type::Value> &GetParamValues() const;

  /// Return the storage manager for the database
  storage::StorageManager &GetStorageManager() const;

  /// Return the query parameters
  codegen::QueryParameters &GetParams();

  /// Return the memory pool for this particular query execution
  type::EphemeralPool *GetPool();

  class ThreadStates {
   public:
    explicit ThreadStates(type::EphemeralPool &pool);

    /// Reset the state space
    void Reset(uint32_t state_size);

    /// Allocate enough state for the given number of threads
    void Allocate(uint32_t num_threads);

    /// Access the state for the thread with the given id
    char *AccessThreadState(uint32_t thread_id) const;

    template <typename T>
    T *AccessThreadStateAs(uint32_t thread_id) const;

    /// Return the number of threads registered in this state
    uint32_t NumThreads() const { return num_threads_; }

    /// Iterate over each thread's state, operating on the element at the given
    /// offset only.
    template <typename T, typename F>
    void ForEach(uint32_t element_offset, const F &func) const;

   private:
    type::EphemeralPool &pool_;
    uint32_t num_threads_;
    uint32_t state_size_;
    char *states_;
  };

  ThreadStates &GetThreadStates();

  /// Number of processed tuples during execution
  uint32_t num_processed = 0;

 private:
  // The transaction context
  concurrency::TransactionContext *transaction_;
  // All query parameters
  codegen::QueryParameters parameters_;
  // The storage manager instance
  storage::StorageManager *storage_manager_;
  // Temporary memory pool for allocations done during execution
  type::EphemeralPool pool_;
  // Container for all states of all thread participating in this execution
  ThreadStates thread_states_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

template <typename T, typename F>
inline void ExecutorContext::ThreadStates::ForEach(uint32_t element_offset,
                                                   const F &func) const {
  PELOTON_ASSERT(element_offset < state_size_ &&
                 "Accessing an element beyond valid offset in thread state");
  for (uint32_t tid = 0; tid < NumThreads(); tid++) {
    auto *thread_state = AccessThreadState(tid);
    auto *elem_state = reinterpret_cast<T *>(thread_state + element_offset);
    func(elem_state);
  }
}

template <typename T>
T *ExecutorContext::ThreadStates::AccessThreadStateAs(
    uint32_t thread_id) const {
  return reinterpret_cast<T *>(AccessThreadState(thread_id));
}

}  // namespace executor
}  // namespace peloton
