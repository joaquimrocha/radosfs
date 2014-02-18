#ifndef __BENCHMARK_MGR_HH__
#define __BENCHMARK_MGR_HH__

#include <pthread.h>
#include <rados/librados.h>

#include "libradosfs.hh"

#define TEST_POOL "test-pool"

class BenchmarkMgr
{
public:
  BenchmarkMgr(const char *conf);
  ~BenchmarkMgr(void);

  int numFiles(void);
  void setNumFiles(int numFiles);
  void incFiles(void);


  radosfs::RadosFs radosFs;

private:
  rados_t mCluster;
  const char *mConf;
  int mNumFiles;
  pthread_mutex_t mNumFilesMutex;
};

#endif // __BENCHMARK_MGR_HH__
