// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get, eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::put, eid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
}

extent_protocol::status
extent_client::pget(extent_protocol::extentid_t id, off_t offset,
          size_t nbytes, std::string &buf)
{
  return cl->call(extent_protocol::pget, id, offset, nbytes, buf); 
}


extent_protocol::status
extent_client::update(extent_protocol::extentid_t id, std::string &data,
    off_t offset, size_t &bytes_written)
{
  return cl->call(extent_protocol::update, id, data, offset, bytes_written);
}

extent_protocol::status
extent_client::resize(extent_protocol::extentid_t eid, off_t new_size)
{
  int r;
  return cl->call(extent_protocol::resize, eid, new_size, r);
}

extent_protocol::status
extent_client::poke(extent_protocol::extentid_t eid)
{
  int unused;
  return cl->call(extent_protocol::poke, eid, unused);
}

