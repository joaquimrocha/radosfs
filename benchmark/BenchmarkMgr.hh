#ifndef __BENCHMARK_MGR_HH__
#define __BENCHMARK_MGR_HH__

#include <pthread.h>
#include <rados/librados.hpp>

#include "RadosFs.hh"
#include "RadosFsDir.hh"
#include "RadosFsFile.hh"

#define TEST_POOL_DATA "radosfs-benchmark-data-pool"
#define TEST_POOL_MTD "radosfs-benchmark-data-pool"

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

  radosfs::Fs radosFs;

private:
  librados::Rados mCluster;
  const char *mConf;
  int mNumFiles;
  bool mCreateInDir;
  pthread_mutex_t mNumFilesMutex;
};

#endif // __BENCHMARK_MGR_HH__
