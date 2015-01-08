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

  pthread_mutex_init(&mNumFilesMutex, 0);
}

BenchmarkMgr::~BenchmarkMgr()
{
  mCluster.init(0);

  mCluster.conf_read_file(mConf);
  mCluster.connect();

  mCluster.pool_delete(TEST_POOL_DATA);
  mCluster.pool_delete(TEST_POOL_MTD);

  mCluster.shutdown();

  pthread_mutex_destroy(&mNumFilesMutex);
}

int
BenchmarkMgr::numFiles()
{
  int nfiles;

  pthread_mutex_lock(&mNumFilesMutex);

  nfiles = mNumFiles;

  pthread_mutex_unlock(&mNumFilesMutex);

  return nfiles;
}

void
BenchmarkMgr::setNumFiles(int numFiles)
{
  pthread_mutex_lock(&mNumFilesMutex);

  mNumFiles = numFiles;

  pthread_mutex_unlock(&mNumFilesMutex);
}

void
BenchmarkMgr::incFiles()
{
  pthread_mutex_lock(&mNumFilesMutex);

  mNumFiles++;

  pthread_mutex_unlock(&mNumFilesMutex);
}
