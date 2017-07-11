//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libevent_thread.h
//
// Identification: src/include/wire/libevent_thread.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <vector>

#include <sys/file.h>
#include <fstream>

#include "common/exception.h"
#include "common/logger.h"
#include "configuration/configuration.h"
#include "container/lock_free_queue.h"
#include "wire/packet_manager.h"


#define QUEUE_SIZE 100

namespace peloton {
namespace wire {

// Forward Declarations
struct NewConnQueueItem;

class LibeventThread {
 protected:
  // The connection thread id
  const int thread_id_;
  struct event_base *libevent_base_;

 private:
  bool is_started = false;
  bool is_closed = false;
  int sock_fd = -1;

 public:
  LibeventThread(const int thread_id, struct event_base *libevent_base)
      : thread_id_(thread_id), libevent_base_(libevent_base) {
    if (libevent_base_ == nullptr) {
      LOG_ERROR("Can't allocate event base\n");
      exit(1);
    }
  };

  inline struct event_base *GetEventBase() { return libevent_base_; }

  inline int GetThreadID() const { return thread_id_; }

  // TODO implement destructor
  inline ~LibeventThread() {}

  // Getter and setter for flags
  bool GetThreadIsStarted() { return is_started; }

  void SetThreadIsStarted(bool is_started) { this->is_started = is_started; }

  bool GetThreadIsClosed() { return is_closed; }

  void SetThreadIsClosed(bool is_closed) { this->is_closed = is_closed; }

  int GetThreadSockFd() { return sock_fd; }

  void SetThreadSockFd(int fd) { this->sock_fd = fd; }
};

}  // namespace wire
}  // namespace peloton
