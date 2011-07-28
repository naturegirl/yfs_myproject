#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include "lock_protocol.h"
#include "lock_client.h"

  class yfs_client {
  extent_client *ec;
  lock_client   *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG, FEXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    unsigned long long inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

  inum new_inum(inum, bool);

  // helper function used by create() and mkdir(), which assumes the lock
  // on "parent" has been obtained.
  // add a new entry to a dir by inserting a line to the dir buf.
  // this function also assigns an unused inum for the new entry according
  // to a boolean flag indicating if we are adding a plain file or a subdir.
  // 
  // return false if a file/dir with the given name exists
  bool dir_add_entry(inum parent, const char *, bool, inum &);

 public:

  yfs_client(std::string, std::string);
  ~yfs_client();

  bool isfile(inum);
  bool isdir(inum);
  inum ilookup(inum di, std::string name);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int create(inum parent, std::string name, inum &);
  int listdir(inum, std::vector<dirent> &);
  status resize(inum, off_t);
  status read(inum, char *, size_t, off_t, size_t &);
  status write(inum, const char *, size_t, off_t, size_t &);
  status mkdir(inum parent, const char *dirname, inum &);
  status remove(inum parent, const char *dirname);
};

#endif 
