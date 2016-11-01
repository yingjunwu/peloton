//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// command_log_manager.h
//
// Identification: src/backend/logging/command_log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/command_log_manager.h"
#include "backend/benchmark/tpcc/tpcc_command_logger.h"


namespace peloton {
namespace benchmark {
namespace tpcc {

class TpccCommandLogManager : public logging::CommandLogManager {
  TpccCommandLogManager(const TpccCommandLogManager &) = delete;
  TpccCommandLogManager &operator=(const TpccCommandLogManager &) = delete;
  TpccCommandLogManager(TpccCommandLogManager &&) = delete;
  TpccCommandLogManager &operator=(TpccCommandLogManager &&) = delete;

protected:

  TpccCommandLogManager() {}

public:
  static TpccCommandLogManager &GetInstance() {
    static TpccCommandLogManager log_manager;
    return log_manager;
  }
  virtual ~TpccCommandLogManager() {}


  virtual void SetDirectories(const std::vector<std::string> &logging_dirs) override {
    if (logging_dirs.size() > 0) {
      pepoch_dir_ = logging_dirs.at(0);
    }
    // check the existence of logging directories.
    // if not exists, then create the directory.
    for (auto logging_dir : logging_dirs) {
      if (logging::LoggingUtil::CheckDirectoryExistence(logging_dir.c_str()) == false) {
        LOG_INFO("Logging directory %s is not accessible or does not exist", logging_dir.c_str());
        bool res = logging::LoggingUtil::CreateDirectory(logging_dir.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", logging_dir.c_str());
        }
      }
    }

    logger_count_ = logging_dirs.size();
    for (size_t i = 0; i < logger_count_; ++i) {
      loggers_.emplace_back(new TpccCommandLogger(i, logging_dirs.at(i)));
    }
  }


  virtual void DoCommandReplay(std::vector<logging::ParamWrapper>& param_wrappers UNUSED_ATTRIBUTE) override {

  }

};

}
}
}