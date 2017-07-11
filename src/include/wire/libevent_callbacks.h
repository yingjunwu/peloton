


namespace peloton {
namespace wire {
/*
 * ControlCallback - Some callback helper functions
 */
class ControlCallback {
 public:
  /* Used to handle signals */
  static void Signal_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                              UNUSED_ATTRIBUTE short what, void *arg);

  /* Used to control server start and close */
  static void ServerControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                     UNUSED_ATTRIBUTE short what, void *arg);

  /* Used to control thread event loop's begin and exit */
  static void ThreadControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                     UNUSED_ATTRIBUTE short what, void *arg);
};

}
}
