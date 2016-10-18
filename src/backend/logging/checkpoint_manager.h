//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint_manager.h
//
// Identification: src/backend/logging/checkpoint/checkpoint_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>

namespace peloton {
namespace logging {

class CheckpointManager {
  // Deleted functions
  CheckpointManager(const CheckpointManager &) = delete;
  CheckpointManager &operator=(const CheckpointManager &) = delete;
  CheckpointManager(CheckpointManager &&) = delete;
  CheckpointManager &operator=(const CheckpointManager &&) = delete;


public:
  CheckpointManager() {
    recovery_thread_count_ = 1;
    max_checkpointer_count_ = 1; //std::thread::hardware_concurrency() / 2;
  }
  virtual ~CheckpointManager() {}

  virtual void SetDirectories(const std::vector<std::string> &checkpoint_dirs) = 0;

  void SetRecoveryThreadCount(const size_t &recovery_thread_count) {
    recovery_thread_count_ = recovery_thread_count;
    if (recovery_thread_count_ > max_checkpointer_count_) {
      LOG_ERROR("# recovery thread cannot be larger than # max checkpointer");
      exit(EXIT_FAILURE);
    }
  }

  virtual void SetCheckpointInterval(const int &checkpoint_interval) = 0;

  virtual void StartCheckpointing() = 0;
  
  virtual void StopCheckpointing() = 0;

  virtual void DoRecovery() {}


protected:

  size_t recovery_thread_count_;

  size_t max_checkpointer_count_;

};

}
}
