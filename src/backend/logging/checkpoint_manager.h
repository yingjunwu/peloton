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

namespace peloton {
namespace logging {

class CheckpointManager {
  // Deleted functions
  CheckpointManager(const CheckpointManager &) = delete;
  CheckpointManager &operator=(const CheckpointManager &) = delete;
  CheckpointManager(CheckpointManager &&) = delete;
  CheckpointManager &operator=(const CheckpointManager &&) = delete;


public:
  CheckpointManager() {}
  virtual ~CheckpointManager() {}

  virtual void SetDirectories(const std::vector<std::string> &checkpoint_dirs) = 0;

  virtual void SetCheckpointInterval(const int &checkpoint_interval) = 0;

  virtual void StartCheckpointing() = 0;
  
  virtual void StopCheckpointing() = 0;

};

}
}
