//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libevent_server.h
//
// Identification: src/include/wire/libevent_server.h
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

#include "wire/libevent_master_thread.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

namespace peloton {
namespace wire {

class LibeventServer {

 public:
  LibeventServer();

  void StartServer();

  void CloseServer();

  /**
   * Change port to new_port
   */
  void SetPort(int new_port) { port_ = new_port; }

  // Getter and setter for flags
  bool GetIsStarted() { return is_started_; }

  void SetIsStarted(bool is_started) { this->is_started_ = is_started; }

  bool GetIsClosed() { return is_closed_; }

  void SetIsClosed(bool is_closed) { this->is_closed_ = is_closed; }

  event_base *GetEventBase() { return base_; }

  static LibeventSocket *GetConn(const int &connfd);

  static void CreateNewConn(const int &connfd, short ev_flags,
                            LibeventThread *thread, ConnState init_state);

 private:
  /* Maintain a global list of connections.
   * Helps reuse connection objects when possible
   */
  static std::unordered_map<int, std::unique_ptr<LibeventSocket>> &
  GetGlobalSocketList() {
    // mapping from socket id to socket object.
    static std::unordered_map<int, std::unique_ptr<LibeventSocket>>
        global_socket_list;

    return global_socket_list;
  }

 public:
  static int recent_connfd;
  static SSL_CTX *ssl_context;

 private:
  // When starting the server, the server will create a master thread.
  // This master thread will then create multiple worker threads.
  std::shared_ptr<LibeventMasterThread> master_thread_;

  uint64_t port_;             // port number
  size_t max_connections_;    // maximum number of connections

  std::string private_key_file_;
  std::string certificate_file_;

  struct event *ev_stop_;     // libevent stop event
  struct event *ev_timeout_;  // libevent timeout event
  
  struct event_base *base_;  // libevent event_base

  // Flags for controlling server start/close status
  bool is_started_ = false;
  bool is_closed_ = false;

};

}
}
