//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dummy_log_manager.h
//
// Identification: src/backend/logging/loggers/dummy_log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "backend/logging/checkpoint_manager.h"
#include "backend/common/macros.h"

namespace peloton {
namespace logging {

class DummyCheckpointManager : public CheckpointManager {
  DummyCheckpointManager() {}

public:
  static DummyCheckpointManager &GetInstance() {
    static DummyCheckpointManager checkpoint_manager;
    return checkpoint_manager;
  }
  virtual ~DummyCheckpointManager() {}


  virtual void SetDirectories(const std::vector<std::string> &logging_dirs UNUSED_ATTRIBUTE) final {}

  virtual void SetCheckpointInterval(const int &checkpoint_interval UNUSED_ATTRIBUTE) final {}

  virtual void StartCheckpointing() final {};
  virtual void StopCheckpointing() final {};

};

}
}