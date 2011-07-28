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
  lc = new lock_client(lock_dst);

  srandom(getpid());
}

yfs_client::~yfs_client()
{
  delete ec;
  delete lc;
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
yfs_client::dir_add_entry(inum parent, const char *name, bool is_file,
    inum &entry_inum)
{
  inum i = 0;

  std::string buf;
  bool fexists = false;
  if (lc->acquire(parent) == lock_protocol::OK) {
    if (ec->get(parent, buf) == extent_protocol::OK) {
      std::ostringstream os;
      std::istringstream is(buf);

      std::string line;
      while (getline(is, line)) {
        size_t len = line.length();
        if (len > 0) {
          std::string::size_type colon_pos;
          colon_pos = line.find(':');
          if (colon_pos != std::string::npos && colon_pos != 0 &&
              colon_pos != len - 1) {
            const char *cur_name = line.substr(0, colon_pos).c_str();
            if (i == 0) {
              // new entry not inserted yet
              int cmp_result = strcmp(cur_name, name);
              if (cmp_result == 0) {
                // file/dir already exists
                fexists = true;
                std::istringstream inum_parser(line.substr(colon_pos+1));
                inum_parser >> i;
                break;
              } else if (cmp_result > 0) {
                i = this->new_inum(parent, is_file);
                assert(i > 0);
                os << name << ":" << i << std::endl;
              }
            }
          }
          os << line << std::endl;
        }
      }
      if (!fexists && i == 0) {
        // append to the end of the dir buf
        i = this->new_inum(parent, is_file);
        assert(i > 0);
        os << name << ":" << i << std::endl;
      }

      if (!fexists) {
        // changes made, so update the dir buf
        ec->put(parent, os.str());
      }
      if (i > 0) {
        entry_inum = i;
      }
    }
    lc->release(parent);
  }
  return !fexists;
}

yfs_client::inum
yfs_client::new_inum(inum parent, bool is_file)
{
  inum i = 0;

assign:
  if (is_file) {
    i = random() | 0x80000000;
  } else {
    i = random() & 0x7fffffff;
  }

  if (i <=1 || i == parent) {
    // do it again
    goto assign;
  }
  // acquire a lock on i to keep other clients from using this inum
  if (lc->acquire(i) == lock_protocol::OK) {
    if (ec->poke(i) != extent_protocol::NOENT) {
      // inum already exists
      lc->release(i);
      goto assign;
    } else {
      ec->put(i, "");
      lc->release(i);
    }
  }
  return i;
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
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


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (lc->acquire(inum) == lock_protocol::OK) {
    if (ec->getattr(inum, a) != extent_protocol::OK) {
      r = IOERR;
    } else {
      fin.atime = a.atime;
      fin.mtime = a.mtime;
      fin.ctime = a.ctime;
      fin.size = a.size;
      printf("getfile %016llx -> sz %llu\n", inum, fin.size);
    }

    lc->release(inum);
  } else {
    r = IOERR;
  } 

  return r;
}

yfs_client::inum
yfs_client::ilookup(inum di, std::string name)
{
  inum entry = 0;
  std::string buf;
  if (lc->acquire(di) == lock_protocol::OK) {
    if (ec->get(di, buf) == extent_protocol::OK) {
      std::istringstream is(buf); 
      std::string line;
      size_t len = name.length();
      while (getline(is, line)) {
        if (line != "") {
          if (line.find(name) == 0 && line.length() > len + 2
              && line[len] == ':') {
            const char *inum_str = line.substr(len+1).c_str();
            sscanf(inum_str, "%llu", &entry);
            break;
          }
        }
      }
    }
    lc->release(di);
  }
  return entry;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (lc->acquire(inum) == lock_protocol::OK) {
    if (ec->getattr(inum, a) != extent_protocol::OK) {
      r = IOERR;
    } else {
      din.atime = a.atime;
      din.mtime = a.mtime;
      din.ctime = a.ctime;
    }
    lc->release(inum);
  } else {
    r = IOERR;
  }

  return r;
}

int
yfs_client::listdir(inum inum, std::vector<dirent> &entries)
{
  int r = OK;
  std::string buf;
  if (lc->acquire(inum) == lock_protocol::OK) {
    if (ec->get(inum, buf) == extent_protocol::OK) {
      std::istringstream is(buf); 
      std::string line;
      size_t len = line.length();
      while (getline(is, line)) {
        if (line != "") {
          std::string::size_type colon_pos;
          colon_pos = line.find(':');
          if (colon_pos != std::string::npos && colon_pos != 0 &&
              colon_pos != len - 1) {
            dirent entry;
            entry.name = line.substr(0, colon_pos);
            sscanf(line.substr(len+1).c_str(), "%llu", &entry.inum);
            entries.push_back(entry);
          } else {
            entries.clear();
            printf("malformed line in directory %llu meta: %s\n", inum,
                line.c_str());
            r = IOERR;
            break;
          }
        }
      }
    } else {
      r = IOERR;
    }
    lc->release(inum);
  } else {
    r = IOERR;
  }
  return r;
}

yfs_client::status
yfs_client::create(inum parent, std::string name, inum &new_inum)
{
  if (dir_add_entry(parent, name.c_str(), true, new_inum)) {
    return OK;
  } else {
    // XXX error handling...  return OK;
    return FEXIST;
  }
}

yfs_client::status
yfs_client::mkdir(inum parent, const char * dname, inum &new_inum)
{
  if (dir_add_entry(parent, dname, false, new_inum)) {
    if (new_inum == 0) {
      return NOENT;
    }
    return OK;
  } else {
    return FEXIST;
  }
}

yfs_client::status
yfs_client::resize(inum inum, off_t new_size)
{
  status ret = OK;
  if (lc->acquire(inum) == lock_protocol::OK) {
    extent_protocol::status r = ec->resize(inum, new_size);
    if (r == extent_protocol::OK)
      ret = OK;
    else if (r == extent_protocol::NOENT)
      ret = NOENT;
    else
      ret = IOERR;

    lc->release(inum);
    return ret;
  } else {
    return IOERR;
  }
}

yfs_client::status
yfs_client::read(inum inum, char *buf, size_t nbytes, off_t offset,
        size_t &bytes_read)
{
  std::string temp;
  status ret;
  if (lc->acquire(inum) == lock_protocol::OK) {
    extent_protocol::status r = ec->pget(inum, offset, nbytes, temp);
    if (r == extent_protocol::OK) {
      bytes_read = temp.size();
      memcpy(buf, temp.c_str(), bytes_read);
      ret = OK;
    } else if (r == extent_protocol::NOENT) {
      ret = NOENT;
    } else {
      ret = IOERR;
    }
    lc->release(inum);
    return ret;
  } else {
    return IOERR;
  }
}

yfs_client::status
yfs_client::write(inum inum, const char *buf, size_t nbytes, off_t offset,
        size_t &bytes_written)
{
  status ret;
  std::string data(buf, nbytes);
  if (lc->acquire(inum) == lock_protocol::OK) {
    extent_protocol::status r = ec->update(inum, data, offset, bytes_written);
    if (r == extent_protocol::OK) {
      ret = OK;
    } else if (r == extent_protocol::NOENT) {
      ret = NOENT;
    } else {
      ret = IOERR;
    }

    lc->release(inum);
    return ret;
  } else {
    return IOERR;
  }
}

yfs_client::status
yfs_client::remove(inum parent, const char *name)
{
  status ret;
  std::string buf;
  if (lc->acquire(parent) == lock_protocol::OK) {
    if (ec->get(parent, buf) == extent_protocol::OK) {
      std::istringstream is(buf);
      std::ostringstream os;
      std::string line;

      inum to_remove = 0;
      while (getline(is, line)) {
        if (line != "") {
          size_t len = line.length();
          std::string::size_type colon_pos;
          colon_pos = line.find(':');
          if (colon_pos != std::string::npos && colon_pos != 0 &&
              colon_pos != len - 1) {
            std::string cur_name = line.substr(0, colon_pos);
            if (to_remove == 0) {
              // we haven't yet found the entry to remove
              if (strcmp(cur_name.c_str(), name) == 0) {
                // okay, we find it now
                std::istringstream inum_parser(line.substr(colon_pos+1));
                inum_parser >> to_remove;
              } else {
                os << line << std::endl;
              }
            } else {
              // since the entry to remove is found already, we don't need
              // to perform string comparison any more
              os << line << std::endl;
            }
          }
        }
      }
      if (to_remove) {
        if (ec->remove(to_remove) == extent_protocol::OK &&
            ec->put(parent, os.str()) == extent_protocol::OK) {
          ret = OK;
        } else {
          ret = IOERR;
        }
      } else {
        ret = NOENT;
      }
    } else {
      ret = NOENT;
    }
    lc->release(parent);
    return ret;
  } else {
    return IOERR;
  }
}

