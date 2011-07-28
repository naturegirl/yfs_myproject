// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);

}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)	// file has MSB to one, dir to zero
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

// added by me
// lookup a file in a directory
yfs_client::inum
yfs_client::lookup(inum di, std::string name)
{
	  std::string buf;
	  if (ec->get(di, buf) == extent_protocol::OK) {
	    std::istringstream is(buf);
	    std::string line;
	    size_t len = name.length();
	    while (getline(is, line)) {			// since each file is newline separated
	      if (line != "") {
	        if (line.find(name) == 0 && line.length() > len + 2		// at least ":" and 2 digit inum
	            && line[len] == ':') {		// found it
	          inum entry;
	          const char *inum_str = line.substr(len+1).c_str();
	          sscanf(inum_str, "%llu", &entry);
	          return entry;
	        }
	      }
	    }
	  }
	  return 0;	// not found
}


int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

// added by me
// save content of directory with inum number in entries
// get buf for directory with inum. Possible error: inum not found -> IO ERROR
// read buf. Possible error: buf format error -> RPCERROR
// on success: read them all into &entries and return ok
int
yfs_client::readdir(inum inum, std::vector<dirent> &entries)
{
  int r = OK;
  std::string buf;
  if (ec->get(inum, buf) == extent_protocol::OK) {
    std::istringstream is(buf);
    std::string line;
    size_t len = line.length();
    while (getline(is, line)) {
      if (line != "") {
        std::string::size_type colon_pos;
        colon_pos = line.find(':');
        if (colon_pos != std::string::npos && colon_pos != 0 &&	// ':' at right position
            colon_pos != len - 1) {
          dirent entry;
          entry.name = line.substr(0, colon_pos);
          sscanf(line.substr(len+1).c_str(), "%llu", &entry.inum);
          entries.push_back(entry);	// add this entry
        } else {	// format error, free all entries, return RPCERROR
          entries.clear();
          printf("malformed line in directory %llu meta: %s\n", inum,
              line.c_str());
          r = RPCERR;
        }
      }
    }
  } else {	// could not find inum
    r = IOERR;
  }
  return r;
}

// added by me
// create file with name in parent directory and return its inum
/*
 * retrieve dir into a buf and an inputstream is
 * the file is generated with random inum
 * read through is and find right place to insert new file according to inum order
 * put is and new file into outputstream os
 * write os back into dir buffer
 */
yfs_client::inum
yfs_client::create(inum parent, std::string name)
{
  std::string buf;
  inum new_inum = -1;
  extent_protocol::status ret;
  ret = ec->get(parent, buf);
  if (ret == extent_protocol::OK) {
routine:
    new_inum = (inum)(random() | 0x80000000);	// random number for inum
    std::istringstream is(buf);
    std::ostringstream os;
    std::string line;

    bool inserted = false;
    int i = 0;
    while (getline(is, line)) {
      if (line != "") {
        size_t len = line.length();
        std::string::size_type colon_pos;
        colon_pos = line.find(':');
        if (colon_pos != std::string::npos && colon_pos != 0 &&
            colon_pos != len - 1) {
          inum cur = atol(line.substr(colon_pos+1).c_str());
          if (cur == new_inum) {
            // the very slight chance of collition occured. New number...
            goto routine;
          }
          if (cur > new_inum && !inserted) {	// it's sorted in order of inum
            // insert a line in this place
            os << name << ":" << new_inum << std::endl;
            ec->put(new_inum, "");
            inserted = true;
          }
          os << line << std::endl;
          i++;
        } else {	// bad format of ":"
          printf("malformed line in directory %llx meta: %s\n", parent,
              line.c_str());
        }
      }
    }
    if (!inserted) {	// insert at end
      os << name << ":" << new_inum << std::endl;
      ec->put(new_inum, "");
    }

    // update content of os into parent directory
    buf = os.str();
    ec->put(parent, buf);
  }
  return new_inum;
}


// added by me
int
yfs_client::setattr(inum inum, off_t size, struct stat& st)
{
	std::string buf;
	if(ec->get(inum,buf) == extent_protocol::NOENT)
		return NOENT;
	extent_protocol::attr a;
	ec->getattr(inum, a);

	buf.resize(size);
	ec->put(inum,buf);

	return OK;
}

// added by me
int
yfs_client::read(inum inum, size_t size, off_t off, char* ret, size_t &n)
{
	std::string buf;
	if (ec->get(inum, buf) == extent_protocol::NOENT)
		return NOENT;
	printf("read at: %d %d ", size, off);
	std::string forRet = buf.substr(off,size);
	for (int i = 0; i < size; ++i)
		ret[i] = forRet[i];
	n = forRet.length();
	printf("total size: %d\n", n);
	return OK;
}

// added by me
int
yfs_client::write(inum inum, const char* buf, size_t size, off_t off, size_t &n)
{
	std::string content;
	std::string temp(buf);
	printf("yfs_client write: %d %d\n", size, off);
	for (int i = 0; i < size; ++i)
		printf("%c", buf[i]);
	printf("\n");
	if (ec->get(inum,content) == extent_protocol::NOENT)
		return NOENT;
	if (content.length() < off+size) {
		printf("resize: before: %d\n", content.length());
		size_t i = content.length();
		content.resize(off+size, '\0');
		for(; i <= off+size; ++i)
			content[i] = '\0';
		printf("resize: after: %d\n", content.length());
	}
	content.replace(off, size, buf, size);
	ec->put(inum, content);
	n = size;
	printf("%d, %d, %s\n", n, content.length(), content.c_str());
	return OK;
}
