


#define MASTER_THREAD_ID -1

namespace peloton {
namespace wire {

// a master thread contains multiple worker threads.
class LibeventMasterThread : public LibeventThread {
 public:
  LibeventMasterThread(const int num_threads, struct event_base *libevent_base);

  void DispatchConnection(int new_conn_fd, short event_flags);

  void CloseConnection();

  static void StartWorker(peloton::wire::LibeventWorkerThread *worker_thread);

  // Get the vector of libevent worker threads
  std::vector<std::shared_ptr<LibeventWorkerThread>> &GetWorkerThreads() {
    static std::vector<std::shared_ptr<LibeventWorkerThread>> worker_threads;
    return worker_threads;
  }

 private:
  const int num_threads_;
  std::atomic<int> next_thread_id_;  // next thread we dispatched to

};


}  // namespace wire
}  // namespace peloton
