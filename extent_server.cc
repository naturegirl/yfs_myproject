// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
  // init the root dir
  _put(1, "");
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  int r = extent_protocol::OK;
  _put(id, buf);
  return r;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  int r = extent_protocol::NOENT;
  if (extent_store.find(id) != extent_store.end()) {
    extent_entry &entry = extent_store[id];
    buf = entry.buf;
    time((time_t *)&entry.attr.atime);
    r = extent_protocol::OK;
  }
  return r;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  int r = extent_protocol::NOENT;

  if (extent_store.find(id) != extent_store.end()) {
    extent_entry &entry = extent_store[id];
    a.size = entry.buf.size();
    a.atime = entry.attr.atime;
    a.mtime = entry.attr.mtime;
    a.ctime = entry.attr.ctime;
    r = extent_protocol::OK;
  }
  return r;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  int ret = extent_store.erase(id); 
  return ret ? extent_protocol::OK : extent_protocol::NOENT;
}

int extent_server::pget(extent_protocol::extentid_t id, off_t offset,
          size_t nbytes, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::NOENT;
  if (extent_store.find(id) != extent_store.end()) {
    extent_entry &entry = extent_store[id];
    size_t len = entry.buf.size();
    if (offset < len) {
      size_t can_read = len - offset;
      size_t actual_read = can_read > nbytes ? nbytes : can_read;
      buf = entry.buf.substr(offset, actual_read);
      time((time_t *)&entry.attr.atime);
      ret = extent_protocol::OK;
    } else {
      ret = extent_protocol::IOERR; 
    }
  }
  return ret;
}

int extent_server::update(extent_protocol::extentid_t id, std::string data,
        off_t offset, size_t &bytes_written)
{
  extent_protocol::status ret = extent_protocol::NOENT;
  if (extent_store.find(id) != extent_store.end()) {
    extent_entry &entry = extent_store[id]; 
    size_t len = entry.buf.size();
    size_t nbytes = data.size();
    size_t end = offset + nbytes;
    if (end > len) {
      // we need to resize the string 
      entry.buf.resize(end);
    }
    entry.buf.replace(offset, nbytes, data);
    time((time_t *)&entry.attr.mtime);
    bytes_written = nbytes;
    ret = extent_protocol::OK;
  }
  return ret;
}

int extent_server::resize(extent_protocol::extentid_t id, off_t new_size,
    int &r)
{
  extent_protocol::status ret = extent_protocol::NOENT;
  if (extent_store.find(id) != extent_store.end()) {
    extent_entry &entry = extent_store[id];
    entry.buf.resize(new_size);
    entry.attr.mtime = time(NULL);
    ret = extent_protocol::OK;
    r = new_size;
  }
  return ret;
}

int extent_server::poke(extent_protocol::extentid_t id, int &unused)
{
  extent_protocol::status ret = extent_protocol::NOENT;
  if (extent_store.find(id) != extent_store.end()) {
    ret = extent_protocol::OK;
  }
  return ret;
}

void extent_server::_put(extent_protocol::extentid_t id,
    std::string &buf)
{
  bool updating = extent_store.find(id) != extent_store.end();
  extent_entry &entry = extent_store[id];
  entry.buf = buf;
  if (updating) {
    time((time_t *)&entry.attr.atime);
  } else {
    memset(&entry.attr, 0, sizeof(extent_protocol::attr));
  }
  time((time_t *)&entry.attr.mtime);
  time((time_t *)&entry.attr.ctime);
}

void extent_server::_put(extent_protocol::extentid_t id,
    const char *buf)
{
  std::string temp(buf);
  _put(id, temp);
}

