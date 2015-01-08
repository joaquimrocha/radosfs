#ifndef __RADOS_FS_FINDER_HH__
#define __RADOS_FS_FINDER_HH__

#include <pthread.h>
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

typedef struct
{
  std::string key;
  int valueInt;
  std::string valueStr;
} FinderArg;

typedef struct _FinderData FinderData;

class Finder
{
public:
  Finder(Filesystem *radosFs);
  ~Finder(void);

  enum FindOptions
  {
    FIND_NAME_EQ,
    FIND_NAME_NE,
    FIND_SIZE_EQ,
    FIND_SIZE_NE,
    FIND_SIZE_GT,
    FIND_SIZE_GE,
    FIND_SIZE_LT,
    FIND_SIZE_LE
  };

  int find(FinderData *data);

  int checkEntrySize(FinderData *data,
                     const std::string &entry,
                     const Dir &dir,
                     struct stat &buff);

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
