#ifndef __RADOS_FS_FINDER_HH__
#define __RADOS_FS_FINDER_HH__

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>

#include "Dir.hh"
#include "radosfsdefines.h"

RADOS_FS_BEGIN_NAMESPACE

struct FinderArg
{
  enum Options {
    FINDER_OPT_NONE = 0,
    FINDER_OPT_CMP_NUM = 1 << 0,
    FINDER_OPT_ICASE = 1 << 1
  };

  FinderArg(void)
    : valueNum(.0),
      options(0)
  {}

  std::string key;
  float valueNum;
  std::string valueStr;
  int options;
};

typedef struct _FinderData FinderData;

class Finder
{
public:
  Finder(Filesystem *radosFs);
  ~Finder(void);

  enum FindOptions
  {
    FIND_EQ = 1 << 0,
    FIND_NE = 1 << 2,
    FIND_LT = 1 << 3,
    FIND_GT = 1 << 4,
    FIND_NAME = 1 << 5,
    FIND_MTD = 1 << 6,
    FIND_XATTR = 1 << 7,
    FIND_SIZE = 1 << 8,
    FIND_GE = FIND_GT | FIND_EQ,
    FIND_LE = FIND_LT | FIND_EQ,

    FIND_NAME_EQ = FIND_NAME | FIND_EQ,
    FIND_NAME_NE = FIND_NAME | FIND_NE,
    FIND_XATTR_EQ = FIND_XATTR | FIND_EQ,
    FIND_XATTR_NE = FIND_XATTR | FIND_NE,
    FIND_XATTR_GT = FIND_XATTR | FIND_GT,
    FIND_XATTR_GE = FIND_XATTR | FIND_GE,
    FIND_XATTR_LT = FIND_XATTR | FIND_LT,
    FIND_XATTR_LE = FIND_XATTR | FIND_LE,
    FIND_SIZE_EQ = FIND_SIZE | FIND_EQ,
    FIND_SIZE_NE = FIND_SIZE | FIND_NE,
    FIND_SIZE_GT = FIND_SIZE | FIND_GT,
    FIND_SIZE_GE = FIND_SIZE | FIND_GE,
    FIND_SIZE_LT = FIND_SIZE | FIND_LT,
    FIND_SIZE_LE = FIND_SIZE | FIND_LE,
    FIND_MTD_EQ = FIND_MTD | FIND_EQ,
    FIND_MTD_NE = FIND_MTD | FIND_NE,
    FIND_MTD_GT = FIND_MTD | FIND_GT,
    FIND_MTD_GE = FIND_MTD | FIND_GE,
    FIND_MTD_LT = FIND_MTD | FIND_LT,
    FIND_MTD_LE = FIND_MTD | FIND_LE
  };

  int find(FinderData *data);

  int checkEntrySize(FinderArg &arg, FindOptions option,
                     const std::string &entry, const Dir &dir,
                     struct stat &buff);

  int checkEntryMtd(FinderArg &arg, FindOptions option,
                    const std::string &entry, Dir &dir);

  int checkEntryXAttrs(FinderArg &arg, FindOptions option,
                       const std::string &entry, Dir &dir);

  int checkEntryName(FinderArg &arg, FindOptions option,
                     const std::string &entry);

  int compareEntryStrValue(FinderArg &arg, const std::string &entry,
                           FindOptions option, const std::string &value,
                           Dir &dir);

  int compareEntryNumValue(FinderArg &arg, FindOptions option, float value);


  int checkXAttrKeyPresence(FinderArg &arg, FindOptions option,
                            const std::map<std::string, std::string> &xattrs);

  Filesystem *radosFs;
};

struct _FinderData {
  std::string dir;
  boost::mutex *mutex;
  boost::condition_variable *cond;
  std::string term;
  const std::map<Finder::FindOptions, FinderArg> *args;
  std::set<std::string> dirEntries;
  std::set<std::string> results;
  int *numberRelatedJobs;
  int *retCode;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_FINDER_HH__
