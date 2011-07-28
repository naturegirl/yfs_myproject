// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent_server {

 public:
	// added by me
	// save entry in this structure
	   struct extent_entry {
	     std::string buf;	// "\n" separated. Each line uses format entry_name:num
	     extent_protocol::attr attr;
	   };

  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);

 private:
  std::map<extent_protocol::extentid_t, extent_entry> extent_store;
  pthread_mutex_t m;
};

#endif 







