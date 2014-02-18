#include <stdexcept>

#include "BenchmarkMgr.hh"

BenchmarkMgr::BenchmarkMgr(const char *conf)
  : mConf(conf),
    mNumFiles(0)
{
  rados_create(&mCluster, 0);

  if (rados_conf_read_file(mCluster, mConf) != 0)
    throw std::invalid_argument("Problem reading configuration file.");

  rados_connect(mCluster);

  rados_pool_create(mCluster, TEST_POOL);

  rados_shutdown(mCluster);

  radosFs.init("", mConf);

  pthread_mutex_init(&mNumFilesMutex, 0);
}

BenchmarkMgr::~BenchmarkMgr()
{
  rados_create(&mCluster, 0);

  rados_conf_read_file(mCluster, mConf);
  rados_connect(mCluster);

  rados_pool_delete(mCluster, TEST_POOL);

  rados_shutdown(mCluster);

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
