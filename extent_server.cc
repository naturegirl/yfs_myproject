// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
	// stupid, forgot to init root server=__=
	 int i;
	 put(1, "", i);
	 pthread_mutex_init(&m, NULL);
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  // You fill this in for Lab 2.
	  int r = extent_protocol::OK;
	  pthread_mutex_lock(&m);
	  extent_entry &entry = extent_store[id];
	  entry.buf = buf;						// set content
	  time((time_t *)&entry.attr.mtime);	// set modification and touch time
	  time((time_t *)&entry.attr.ctime);
	  //entry.attr.size = buf.size();
	  pthread_mutex_unlock(&m);
	  return r;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{

  // You fill this in for Lab 2.
	int r = extent_protocol::NOENT;	// possible that we don't find it
	pthread_mutex_lock(&m);
	if (extent_store.find(id) != extent_store.end()) {
		extent_entry &entry = extent_store[id];
		buf = entry.buf;
		time((time_t *)&entry.attr.atime);	// set access time
		r = extent_protocol::OK;
	}
	pthread_mutex_unlock(&m);
	return r;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{

  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
	int r = extent_protocol::NOENT;
	pthread_mutex_lock(&m);
	if (extent_store.find(id) != extent_store.end()) {
		extent_entry &entry = extent_store[id];
		a.size = entry.buf.size();	// has been set in put according to buf.size()
		a.atime = entry.attr.atime;
		a.ctime = entry.attr.ctime;
		a.mtime = entry.attr.mtime;
		r = extent_protocol::OK;
	}
	pthread_mutex_unlock(&m);
	return r;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
	int r = extent_protocol::NOENT;
	pthread_mutex_lock(&m);
	if (extent_store.find(id) != extent_store.end()) {
		extent_store.erase(id);
		r = extent_protocol::OK;
	}
	pthread_mutex_unlock(&m);
	return r;
}

