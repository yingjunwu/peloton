


namespace peloton {
namespace wire {

class LibeventWorkerThread : public LibeventThread {

 public:
  LibeventWorkerThread(const int thread_id);

  // Getters and setters
  event *GetNewConnEvent() { return this->new_conn_event_; }

  event *GetTimeoutEvent() { return this->ev_timeout_; }

  int GetNewConnSendFd() { return this->new_conn_send_fd_; }

  int GetNewConnReceiveFd() { return this->new_conn_receive_fd_; }

 private:
  // New connection event
  struct event *new_conn_event_;

  // Timeout event
  struct event *ev_timeout_;

  // Notify new connection pipe(send end)
  int new_conn_send_fd_;

  // Notify new connection pipe(receive end)
  int new_conn_receive_fd_;

 public:
  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;
};

}  // namespace wire
}  // namespace peloton
