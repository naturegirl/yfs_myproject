#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;	// 64 bit identifier
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG, EXIST };
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
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);


  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  inum creat(inum parent, std::string name);	// added by me
  int listdir(inum, std::vector<dirent> &);
  inum ilookup(inum di, std::string name);	// added by me
  int setattr(inum, off_t, struct stat&);
};

#endif 
