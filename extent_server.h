// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

// i believe extent_server should not contain any file system
// logic such as inode linking etc. it only deals with data blocks.

class extent_server {

 public:
   struct extent_entry {
     // buf is a \n-separated string, in which each line is of form:
     // entry_name:num
     // Note that this restricts usage of the colon symbol (:) in file
     // names.
     std::string buf;
     extent_protocol::attr attr;
   };
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
  int pget(extent_protocol::extentid_t id, off_t offset,
          size_t nbytes, std::string &buf);
  int update(extent_protocol::extentid_t id, std::string data,
          off_t offset, size_t &bytes_written);
  int resize(extent_protocol::extentid_t, off_t new_size, int &);
  int poke(extent_protocol::extentid_t, int &);

 private:
  std::map<extent_protocol::extentid_t, extent_entry> extent_store;

  void _put(extent_protocol::extentid_t id, std::string &);
  void _put(extent_protocol::extentid_t id, const char *);
};

#endif 

