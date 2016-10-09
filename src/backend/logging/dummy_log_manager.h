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

#include "backend/logging/log_manager.h"
#include "backend/common/macros.h"

namespace peloton {
namespace logging {

class DummyLogManager : public LogManager {
  DummyLogManager() {}

public:
  static DummyLogManager &GetInstance() {
    static DummyLogManager log_manager;
    return log_manager;
  }
  virtual ~DummyLogManager() {}


  virtual void SetDirectories(const std::vector<std::string> &logging_dirs UNUSED_ATTRIBUTE) final {}

  // Worker side logic
  virtual void RegisterWorker() final {};
  virtual void DeregisterWorker() final {};

  virtual void DoRecovery() final {};

  virtual void StartLoggers() final {};
  virtual void StopLoggers() final {};

};

}
}