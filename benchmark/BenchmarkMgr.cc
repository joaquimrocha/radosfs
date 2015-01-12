#include <stdexcept>

#include "BenchmarkMgr.hh"

BenchmarkMgr::BenchmarkMgr(const char *conf)
  : mConf(conf),
    mNumFiles(0),
    mCreateInDir(false)
{
  mCluster.init(0);

  if (mCluster.conf_read_file(mConf) != 0)
    throw std::invalid_argument("Problem reading configuration file.");

  mCluster.connect();

  mCluster.pool_create(TEST_POOL_DATA);
  mCluster.pool_create(TEST_POOL_MTD);

  mCluster.shutdown();

  radosFs.init("", mConf);
}

BenchmarkMgr::~BenchmarkMgr()
{
  mCluster.init(0);

  mCluster.conf_read_file(mConf);
  mCluster.connect();

  mCluster.pool_delete(TEST_POOL_DATA);
  mCluster.pool_delete(TEST_POOL_MTD);

  mCluster.shutdown();
}

int
BenchmarkMgr::numFiles()
{
  int nfiles;
  boost::unique_lock<boost::mutex> lock(mNumFilesMutex);

  nfiles = mNumFiles;

  return nfiles;
}

void
BenchmarkMgr::setNumFiles(int numFiles)
{
  boost::unique_lock<boost::mutex> lock(mNumFilesMutex);
  mNumFiles = numFiles;
}

void
BenchmarkMgr::incFiles()
{
  boost::unique_lock<boost::mutex> lock(mNumFilesMutex);
  mNumFiles++;
}
