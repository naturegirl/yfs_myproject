// the caching lock server implementation

#include "lock_server_cache.h"
#include "jsl_log.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

client_record::client_record()
  : clt(-1), seq(-1)
{

}

client_record::client_record(int clt_, int seq_)
  : clt(clt_), seq(seq_)
{

}

lock_t::lock_t()
  : expected_clt(-1), retry_responded(false), revoke_sent(false)
{
  pthread_cond_init(&retry_responded_cv, NULL);
}

lock_t::~lock_t()
{
  pthread_cond_destroy(&retry_responded_cv);
}

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&revoke_cv, NULL);
  pthread_cond_init(&release_cv, NULL);

  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
}

lock_server_cache::~lock_server_cache()
{
  pthread_mutex_lock(&m);
  std::map<int, rpcc *>::iterator itr;
  for (itr = clients.begin(); itr != clients.end(); ++itr) {
    delete itr->second;
  }
  pthread_mutex_unlock(&m);
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&revoke_cv);
  pthread_cond_destroy(&release_cv);
}

lock_protocol::status
lock_server_cache::acquire(int clt, int seq, lock_protocol::lockid_t lid,
    int &queue_len)
{
  lock_protocol::status r;
  jsl_log(JSL_DBG_4, "clt %d seq %d acquiring lock %llu\n", clt, seq, lid);
  pthread_mutex_lock(&m);
  lock_t &l = locks[lid];
  queue_len = l.waiting_list.size();
  jsl_log(JSL_DBG_4, "queue len for lock %llu: %d\n", lid, queue_len);
  if (l.owner.clt == -1 && ((queue_len > 0 && l.expected_clt == clt) ||
        queue_len == 0)) {
    jsl_log(JSL_DBG_4, "lock %llu is free; granting to clt %d\n", lid, clt);
    l.owner.clt = clt;
    l.owner.seq = seq;
    r = lock_protocol::OK;
    if (queue_len != 0) {
      jsl_log(JSL_DBG_4, "expected clt %d replied to retry request\n", clt);
      l.expected_clt = -1;
      // since there are waiting clients, we have to unfortunately add this
      // lock to the revoke set to get it back
      if (queue_len < 5) {
        revoke_set.insert(lid);
        l.revoke_sent = true;
        pthread_cond_signal(&revoke_cv);
      } else {
        // if the queue has more than 5 clients waiting, we don't need to
        // to send a revoke because we know the client will soon release
        // the lock. we just pretend that we have sent a revoke to the owner
        // of the lock
        l.revoke_sent = true;
      }
      //l.retry_responded = true;
      //pthread_cond_signal(&l.retry_responded_cv);
    } else {
      // a brand new lock
      l.revoke_sent = false;
    }
  } else {
    if (queue_len > 0) {
      // Note that we don't need to add lid to revoke_set here, because we
      // already did so for the head of the queue
      jsl_log(JSL_DBG_4, "clt %d not expected for lock %llu; queued\n", clt,
          lid);
    } else {
      jsl_log(JSL_DBG_4, "queuing clt %d seq %d for lock %llu\n", clt, seq,
          lid);
      // i will be the head of the waiting list
      if (!l.revoke_sent) {
        revoke_set.insert(lid);
        l.revoke_sent = true;
        pthread_cond_signal(&revoke_cv);
      }
    }
    l.waiting_list.push_back(client_record(clt, seq));
    r = lock_protocol::RETRY;
  }
  pthread_mutex_unlock(&m);
  return r;
}

lock_protocol::status
lock_server_cache::release(int clt, int seq, lock_protocol::lockid_t lid,
    int &unused)
{
  lock_protocol::status r = lock_protocol::OK;
  pthread_mutex_lock(&m);
  if (locks.find(lid) != locks.end() && locks[lid].owner.clt == clt) {
    assert(locks[lid].owner.seq = seq);
    jsl_log(JSL_DBG_4, "clt %d released lck %llu at seq %d\n", clt, lid,
        seq);
    locks[lid].owner.clt = -1;
    locks[lid].owner.seq = -1;
    //locks[lid].revoke_sent = false;
    released_locks.push_back(lid);
    pthread_cond_signal(&release_cv);
  }
  pthread_mutex_unlock(&m);
  return r;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  r = 0;
  return ret;
}

lock_protocol::status
lock_server_cache::subscribe(int clt, std::string id, int &unused)
{
  lock_protocol::status r = lock_protocol::OK;
  pthread_mutex_lock(&m);
  sockaddr_in dstsock;
  make_sockaddr(id.c_str(), &dstsock);
  rpcc *cl = new rpcc(dstsock);
  if (cl->bind() == 0) {
    clients[clt] = cl;
  } else {
    printf("failed to bind to clt %d\n", clt);
  }
  pthread_mutex_unlock(&m);
  return r;
}

void
lock_server_cache::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock

  while (true) {
    pthread_mutex_lock(&m);
    while (revoke_set.empty()) {
      pthread_cond_wait(&revoke_cv, &m);
    }
    std::set<lock_protocol::lockid_t>::iterator itr = revoke_set.begin();
    lock_protocol::lockid_t lid = *itr;
    revoke_set.erase(lid);
    int unused;
    lock_t &l = locks[lid];
    rpcc *cl = clients[l.owner.clt];
    if (cl) {
      if (cl->call(rlock_protocol::revoke, lid, l.owner.seq, unused)
          != rlock_protocol::OK) {
        jsl_log(JSL_DBG_2, "failed to send revoke\n");
      }
    } else {
      jsl_log(JSL_DBG_2, "client %d didn't subscribe\n", l.owner.clt);
    }
    pthread_mutex_unlock(&m);
    usleep(500);
  }
}


void
lock_server_cache::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.

  for (;;) {
    pthread_mutex_lock(&m);
    while (released_locks.empty()) {
      pthread_cond_wait(&release_cv, &m);
    }
    lock_protocol::lockid_t lid = released_locks.front();
    // XXX warning: this is not fault-tolerant
    released_locks.pop_front();
    lock_t &l = locks[lid];
    std::deque<client_record> &wq = l.waiting_list;
    client_record *cr = NULL;
    if (!wq.empty()) {
      cr = &wq.front();
      l.expected_clt = cr->clt;
      wq.pop_front();
    }
    pthread_mutex_unlock(&m);

    if (cr) {
      int cur_seq;
      // TODO place a time limit on the retry for this client
      if (clients[cr->clt]->call(rlock_protocol::retry, lid, cr->seq,
            cur_seq) == rlock_protocol::OK) {
        jsl_log(JSL_DBG_4,
            "successfully sent a retry to clt %d seq %d for lck %llu\n",
            cr->clt, cr->seq, lid); 
      } else {
        jsl_log(JSL_DBG_2,
            "failed to tell client %d to retry lock %llu\n", cr->clt, lid);
      }
    }
    //usleep(500);
  }
}

