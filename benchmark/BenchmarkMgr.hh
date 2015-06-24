#ifndef __BENCHMARK_MGR_HH__
#define __BENCHMARK_MGR_HH__

#include <boost/thread.hpp>
#include <rados/librados.hpp>

#include "Filesystem.hh"
#include "Dir.hh"
#include "File.hh"

#define TEST_POOL_DATA "radosfs-benchmark-data-pool"
#define TEST_POOL_MTD "radosfs-benchmark-data-pool"

class BenchmarkMgr
{
public:
  BenchmarkMgr(const char *conf, const std::string &user,
               const std::string &mtdPool, const std::string &dataPool,
               bool createPools);
  ~BenchmarkMgr(void);

  int numFiles(void);
  void setNumFiles(int numFiles);
  void incFiles(void);
  void setCreateInDir(bool create) { mCreateInDir = create; }
  bool createInDir(void) const { return mCreateInDir; }
  int setupPools(void);

  radosfs::Filesystem radosFs;

private:
  const char *mConf;
  const std::string mUser;
  std::string mMtdPool;
  std::string mDataPool;
  int mNumFiles;
  bool mCreateInDir;
  boost::mutex mNumFilesMutex;
  bool mCreatedPools;
};

#endif // __BENCHMARK_MGR_HH__
