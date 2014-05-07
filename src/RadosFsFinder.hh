#ifndef __RADOS_FS_FINDER_HH__
#define __RADOS_FS_FINDER_HH__

#include <pthread.h>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>

#include "RadosFsDir.hh"
#include "radosfsdefines.h"

RADOS_FS_BEGIN_NAMESPACE

typedef struct
{
  std::string key;
  int valueInt;
  std::string valueStr;
} FinderArg;

typedef struct _FinderData FinderData;

class RadosFsFinder
{
public:
  RadosFsFinder(RadosFs *radosFs);
  ~RadosFsFinder(void);

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

  int realFind(FinderData *data);

  void find(FinderData *data);

  void updateNumThreads(int diff);

  void updateAvailThreads(int diff);

  int checkEntrySize(FinderData *data,
                     const std::string &entry,
                     const RadosFsDir &dir,
                     struct stat &buff);

  RadosFs *radosFs;
  size_t numThreads;
  size_t availableThreads;
  size_t maxNumThreads;
  pthread_mutex_t dirsMutex;
  std::queue<FinderData *> findQueue;
  pthread_mutex_t threadCountMutex;
};

struct _FinderData {
  std::string dir;
  pthread_mutex_t *mutex;
  pthread_cond_t *cond;
  std::string term;
  const std::map<RadosFsFinder::FindOptions, FinderArg> *args;
  std::set<std::string> dirEntries;
  std::set<std::string> results;
  int *numberRelatedJobs;
  int *retCode;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_FINDER_HH__
