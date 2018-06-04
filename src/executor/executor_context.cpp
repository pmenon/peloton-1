//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.cpp
//
// Identification: src/executor/executor_context.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/executor_context.h"

#include "storage/storage_manager.h"

namespace peloton {
namespace executor {

////////////////////////////////////////////////////////////////////////////////
///
/// ExecutorContext
///
////////////////////////////////////////////////////////////////////////////////

ExecutorContext::ExecutorContext(concurrency::TransactionContext *transaction,
                                 codegen::QueryParameters parameters)
    : transaction_(transaction),
      parameters_(std::move(parameters)),
      storage_manager_(storage::StorageManager::GetInstance()),
      thread_states_(pool_) {}

concurrency::TransactionContext *ExecutorContext::GetTransaction() const {
  return transaction_;
}

const std::vector<type::Value> &ExecutorContext::GetParamValues() const {
  return parameters_.GetParameterValues();
}

storage::StorageManager &ExecutorContext::GetStorageManager() const {
  return *storage_manager_;
}

codegen::QueryParameters &ExecutorContext::GetParams() { return parameters_; }

type::EphemeralPool *ExecutorContext::GetPool() { return &pool_; }

ExecutorContext::ThreadStates &ExecutorContext::GetThreadStates() {
  return thread_states_;
}

////////////////////////////////////////////////////////////////////////////////
///
/// ThreadStates
///
////////////////////////////////////////////////////////////////////////////////

ExecutorContext::ThreadStates::ThreadStates(type::EphemeralPool &pool)
    : pool_(pool), num_threads_(0), state_size_(0), states_(nullptr) {}

void ExecutorContext::ThreadStates::Reset(const uint32_t state_size) {
  // If we've previously allocated some state, free it up now
  if (states_ != nullptr) {
    pool_.Free(states_);
    states_ = nullptr;
  }

  // Clear # threads
  num_threads_ = 0;

  // Always fill out the thread-local state space to nearest cache-line to
  // prevent false sharing of states between different threads. This is because
  // all thread states are allocated in contiguous space.

  uint32_t pad = state_size & static_cast<uint32_t>((CACHELINE_SIZE - 1));
  state_size_ = state_size + (pad != 0 ? CACHELINE_SIZE - pad : pad);
}

void ExecutorContext::ThreadStates::Allocate(const uint32_t num_threads) {
  // There better not be any pre-existing state that we're overwriting
  PELOTON_ASSERT(states_ == nullptr && "State was not reset after use");

  // Set threads
  num_threads_ = num_threads;

  // Allocate contiguous state space for all threads
  uint32_t alloc_size = num_threads_ * state_size_;
  states_ = reinterpret_cast<char *>(pool_.Allocate(alloc_size));

  // Clear it
  PELOTON_MEMSET(states_, 0, alloc_size);
}

char *ExecutorContext::ThreadStates::AccessThreadState(
    const uint32_t thread_id) const {
  PELOTON_ASSERT(
      states_ != nullptr &&
      "No thread state configured yet! Did you forget to call Reset()?");

  PELOTON_ASSERT(thread_id < num_threads_ &&
                 "Accessing state for non-existent thread");

  // Done
  return states_ + (thread_id * state_size_);
}

}  // namespace executor
}  // namespace peloton
