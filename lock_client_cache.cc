// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include "jsl_log.h"
#include <sstream>
#include <iostream>
#include <stdio.h>

cached_lock::cached_lock()
  : owner(0), seq(0), used(false), waiting_clients(0), can_retry(false),
    _status(NONE)
{
  pthread_cond_init(&status_cv, NULL);
  pthread_cond_init(&used_cv, NULL);
  pthread_cond_init(&retry_cv, NULL);
  pthread_cond_init(&got_acq_reply_cv, NULL);
}

cached_lock::~cached_lock()
{
  pthread_cond_destroy(&status_cv);
  pthread_cond_destroy(&used_cv);
  pthread_cond_destroy(&retry_cv);
  pthread_cond_destroy(&got_acq_reply_cv);
}

void
cached_lock::set_status(lock_status sts)
{
  // assume the thread holds the mutex m
  if (_status != sts) {
    if (sts == LOCKED) {
      owner = pthread_self();
      used = true;
      pthread_cond_signal(&used_cv);
    }
    if (sts == NONE) {
      // clear all fields
      used = false;
      can_retry = false;
    }
    _status = sts;
    pthread_cond_broadcast(&status_cv);
  }
}

cached_lock::lock_status
cached_lock::status() const
{
  return _status;
}

static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu), last_seq(0)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;

  pthread_mutex_init(&m, NULL);
  pthread_mutex_init(&revoke_m, NULL);
  pthread_cond_init(&revoke_cv, NULL);

  rlsrpc = new rpcs(rlock_port);
  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);
}

lock_client_cache::~lock_client_cache()
{
  int unused;
  pthread_mutex_lock(&m);
  std::map<lock_protocol::lockid_t, cached_lock>::iterator itr;
  for (itr = cached_locks.begin(); itr != cached_locks.end(); ++itr) {
    if (itr->second.status() == cached_lock::FREE) {
      cl->call(lock_protocol::release, cl->id(), itr->second.seq, itr->first,
          unused);
    } else if (itr->second.status() == cached_lock::LOCKED
           && pthread_self() == itr->second.owner) {
      release(itr->first);
      cl->call(lock_protocol::release, cl->id(), itr->second.seq, itr->first,
          unused);
    }
    // TODO what about other states?
  }
  pthread_mutex_unlock(&m);
  pthread_cond_destroy(&revoke_cv);
  pthread_mutex_destroy(&m);
  delete rlsrpc;
}

void
lock_client_cache::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  running = true;
  while (running) {
    int unused;
    pthread_mutex_lock(&m);
    while (revoke_map.empty()) {
      pthread_cond_wait(&revoke_cv, &m);
    }
    std::map<lock_protocol::lock_protocol::lockid_t, int>::iterator itr = revoke_map.begin();
    lock_protocol::lockid_t lid = itr->first;
    int seq = itr->second;
    jsl_log(JSL_DBG_4, "[%d] releasing lock %llu at seq %d\n", cl->id(),
        lid, seq);
    cached_lock &l = cached_locks[lid];
    if (l.status() == cached_lock::NONE) {
      jsl_log(JSL_DBG_3, "[%d] false revoke alarm: %llu\n", cl->id(), lid);
      revoke_map.erase(lid);
      pthread_mutex_unlock(&m);
      continue;
    }
    while (l.seq < seq) {
      pthread_cond_wait(&l.got_acq_reply_cv, &m);
    }
    while (!l.used) {
      // wait until this lock is used at least once
      pthread_cond_wait(&l.used_cv, &m);
    }
    while (l.status() != cached_lock::FREE) {
      // wait until the lock is released 
      pthread_cond_wait(&l.status_cv, &m);
    }
    jsl_log(JSL_DBG_4, "[%d] calling release RPC for lock %llu\n", cl->id(),
        lid);
    if (cl->call(lock_protocol::release, cl->id(), l.seq, lid, unused) ==
        lock_protocol::OK) {
      // we set the lock's status to NONE instead of erasing it
      l.set_status(cached_lock::NONE);
      revoke_map.erase(lid);
    }
    // if remote release fails, we leave this lock in the revoke_map, which
    // will be released in a later attempt
    pthread_mutex_unlock(&m);
    usleep(500);
  }
}


// this function blocks until the specified lock is successfully acquired
// or if an unexpected error occurs.
// note that acquire() is NOT an atomic operation because it may temporarily
// release the mutex while waiting on certain condition varaibles.
// for this reason, we need an ACQUIRING status to tell other threads that
// an acquisition is in progress.
lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  lock_protocol::status r;

  if (last_seq == 0) {
    // this is my first contact with the server, so i have to tell him
    // my rpc address to subscribe for async rpc response
    int unused;
    if ((r = cl->call(lock_protocol::subscribe, cl->id(), id, unused)) !=
        lock_protocol::OK) {
      jsl_log(JSL_DBG_2, "failed to subscribe client: %u\n", cl->id());
      return r;
    }
  }

  pthread_mutex_lock(&m);
  cached_lock &l = cached_locks[lid];
  switch (l.status()) {
    case cached_lock::FREE:
      // great! no one is using the cached lock
      jsl_log(JSL_DBG_4, "[%d] lock %llu free locally: grant to %lu\n",
          cl->id(), lid, (unsigned long)pthread_self());
      r = lock_protocol::OK;
      l.set_status(cached_lock::LOCKED);
      break;
    case cached_lock::ACQUIRING:
      // there is on-going lock acquisition; we just sit here and wait the
      // its completion, in which case we can safely fall through to the
      // next case
      jsl_log(JSL_DBG_4, "[%d] lck-%llu: another thread in acquisition\n",
          cl->id(), lid);
      while (l.status() == cached_lock::ACQUIRING) {
        pthread_cond_wait(&l.status_cv, &m);
      }
      if (l.status() == cached_lock::FREE) {
        // somehow this lock becomes mine!
        r = lock_protocol::OK;
        l.set_status(cached_lock::LOCKED);
        break;
      }
      // the lock is LOCKED or NONE, so we continue to the next case
    case cached_lock::LOCKED:
      if (l.owner == pthread_self()) {
        // the current thread has already obtained the lock
        jsl_log(JSL_DBG_4, "[%d] current thread already got lck %llu\n",
            cl->id(), lid);
        r = lock_protocol::OK;
        break;
      } else {
        // in the predicate of the while loop, we don't check if the lock is
        // revoked by the server. this allows competition between the local
        // threads and the revoke thread.
        while (l.status() != cached_lock::FREE && l.status() !=
            cached_lock::NONE) {
          // TODO also check if there are many clients waiting
          pthread_cond_wait(&l.status_cv, &m);
        }
        if (l.status() == cached_lock::FREE) {
          jsl_log(JSL_DBG_4, "[%d] lck %llu obatained locally by th %lu\n",
              cl->id(), lid, (unsigned long)pthread_self());
          r = lock_protocol::OK;
          l.set_status(cached_lock::LOCKED);
          break;
        }
        // if we reach here, it means the lock has been returned to the
        // server, i.e., l.status() == cached_lock::NONE. we just fall through
      }
    case cached_lock::NONE:
      jsl_log(JSL_DBG_4, "[%d] lock %llu not available; acquiring now\n",
          cl->id(), lid);
      l.set_status(cached_lock::ACQUIRING);
      while ((r = do_acquire(lid)) == lock_protocol::RETRY) {
        while (!l.can_retry) {
          pthread_cond_wait(&l.retry_cv, &m);
        }
      }
      if (r == lock_protocol::OK) {
        jsl_log(JSL_DBG_4, "[%d] thread %lu got lock %llu at seq %d\n",
            cl->id(), pthread_self(), lid, l.seq);
        l.set_status(cached_lock::LOCKED);
      }
      break;
    default:
      break;
  }
  pthread_mutex_unlock(&m);
  return r;
}

// release() is an atomic operation
lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  lock_protocol::status r = lock_protocol::OK;
  pthread_mutex_lock(&m);
  cached_lock &l = cached_locks[lid];
  if (l.status() == cached_lock::LOCKED && l.owner == pthread_self()) {
    assert(l.used);
    if (l.waiting_clients >= 5) {
      // too many contending clients - we have to relinquish the lock
      // right now
      jsl_log(JSL_DBG_3,
          "[%d] more than 5 clients waiting on lck %llu; release now\n",
          cl->id(), lid);
      revoke_map.erase(lid);
      int unused;
      cl->call(lock_protocol::release, cl->id(), l.seq, lid, unused);
      // mark this lock as NONE anyway
      l.set_status(cached_lock::NONE);
    } else {
      l.set_status(cached_lock::FREE);
    }
  } else { 
    jsl_log(JSL_DBG_4, "[%d] thread %lu is not owner of lck %llu\n",
        cl->id(), (unsigned long)pthread_self(), lid);
    r = lock_protocol::NOENT;
  }
  pthread_mutex_unlock(&m);
  return r;
}

rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int seq, int &unused)
{
  rlock_protocol::status r = rlock_protocol::OK;

  jsl_log(JSL_DBG_4,
      "[%d] server request to revoke lck %llu at seq %d\n", cl->id(), lid,
      seq);
  // we do nothing but pushing back the lock id to the revoke queue
  pthread_mutex_lock(&revoke_m);
  revoke_map[lid] = seq;
  pthread_cond_signal(&revoke_cv);
  pthread_mutex_unlock(&revoke_m);
  return r;
}

rlock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, int seq,
    int &current_seq)
{
  rlock_protocol::status r = rlock_protocol::OK;
  pthread_mutex_lock(&m);
  assert(cached_locks.find(lid) != cached_locks.end());
  cached_lock &l = cached_locks[lid];
  if (seq >= l.seq) {
    // it doesn't matter whether this retry message arrives before or
    // after the response to the corresponding acquire arrives, as long
    // as the sequence number of the retry matches that of the acquire
    assert(l.status() == cached_lock::ACQUIRING);
    jsl_log(JSL_DBG_4, "[%d] retry message for lid %llu seq %d\n",
        cl->id(), lid, seq);
    l.can_retry = true;
    pthread_cond_signal(&l.retry_cv);
  } else {
    jsl_log(JSL_DBG_3,
        "[%d] outdated retry %d, current seq for lid %llu is %d\n", 
        cl->id(), seq, lid, l.seq);
  }

  pthread_mutex_unlock(&m);
  return r;
}

// assumes the current thread holds the mutex
int
lock_client_cache::do_acquire(lock_protocol::lockid_t lid)
{
  int r, queue_len;
  cached_lock &l = cached_locks[lid];
  jsl_log(JSL_DBG_4, "[%d] calling acquire rpc for lck %llu id=%d seq=%d\n",
      cl->id(), lid, cl->id(), last_seq+1);
  r = cl->call(lock_protocol::acquire, cl->id(), ++last_seq, lid, queue_len);
  l.seq = last_seq;
  if (r == lock_protocol::OK) {
    l.waiting_clients = queue_len;
  } else if (r == lock_protocol::RETRY) {
    l.can_retry = false;
  }
  pthread_cond_signal(&l.got_acq_reply_cv);
  return r;
}

