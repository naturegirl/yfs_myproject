// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"

class extent_client {
 private:
  rpcc *cl;

 public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid, 
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  extent_protocol::status pget(extent_protocol::extentid_t id, off_t offset,
          size_t nbytes, std::string &buf);
  extent_protocol::status update(extent_protocol::extentid_t id,
      std::string &data, off_t offset, size_t &bytes_written);
  extent_protocol::status resize(extent_protocol::extentid_t eid,
      off_t new_size);
  extent_protocol::status poke(extent_protocol::extentid_t eid);
};

#endif 

