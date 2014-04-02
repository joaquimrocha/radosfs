#ifndef __BENCHMARK_MGR_HH__
#define __BENCHMARK_MGR_HH__

#include <pthread.h>
#include <rados/librados.h>

#include "RadosFs.hh"
#include "RadosFsDir.hh"
#include "RadosFsFile.hh"

#define TEST_POOL "test-pool"

class BenchmarkMgr
{
public:
  BenchmarkMgr(const char *conf);
  ~BenchmarkMgr(void);

  int numFiles(void);
  void setNumFiles(int numFiles);
  void incFiles(void);
  void setCreateInDir(bool create) { mCreateInDir = create; }
  bool createInDir(void) const { return mCreateInDir; }

  radosfs::RadosFs radosFs;

private:
  rados_t mCluster;
  const char *mConf;
  int mNumFiles;
  bool mCreateInDir;
  pthread_mutex_t mNumFilesMutex;
};

#endif // __BENCHMARK_MGR_HH__
