// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

// Define this class to manage locks. Used in the map
class lock_entry {
private:
  bool acquired;
  pthread_mutex_t _m;	// mutex variable
  pthread_cond_t  _cv;	// cond variable

public:
  lock_entry();
  ~lock_entry();

  void acquire();
  void release();
};


class lock_server {

 protected:
  int nacquire;
  // Use this data structure to manage locks
  std::map<lock_protocol::lockid_t, lock_entry> locks;
 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  // These were added by me to implement lock server
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







