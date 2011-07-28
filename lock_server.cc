// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

// the following implementation is prone to cause distributed deadlocks
// for that RPC handlers at the server side is blocked by a conditional
// variable

lock_entry::lock_entry()
    : acquired(false)
{
  pthread_mutex_init(&_m, NULL);
  pthread_cond_init(&_cv, NULL);
}

lock_entry::~lock_entry()
{
  pthread_mutex_destroy(&_m);
  pthread_cond_destroy(&_cv);
}

void
lock_entry::acquire()
{
  pthread_mutex_lock(&_m);
  while (acquired) {
    pthread_cond_wait(&_cv, &_m);
  }
  acquired = true;
  pthread_mutex_unlock(&_m);
}

void
lock_entry::release()
{
  pthread_mutex_lock(&_m);
  if (acquired) {
    acquired = false;
    pthread_cond_broadcast(&_cv);
  } else {

  }
  pthread_mutex_unlock(&_m);
}

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &unused)
{
  // check the availability of the specified lock.
  // if it is available or brand new, then grant it. if it is not, we
  //  add this acquisition request to the retrier queue and return RETRY.
  lock_protocol::status ret = lock_protocol::OK;
  locks[lid].acquire();
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &unused)
{
  lock_protocol::status ret = lock_protocol::OK;
  locks[lid].release();
  return ret;
}

