#include <stdexcept>

#include "BenchmarkMgr.hh"

BenchmarkMgr::BenchmarkMgr(const char *conf, const std::string &user,
                           const std::string &mtdPool,
                           const std::string &dataPool, bool createPools,
                           size_t maxFileSize)
  : mConf(conf),
    mUser(user),
    mMtdPool(mtdPool),
    mDataPool(dataPool),
    mMaxFileSize(maxFileSize),
    mNumFiles(0),
    mCreateInDir(false),
    mCreatedPools(false),
    mDeleteObjects(false)
{
  librados::Rados cluster;
  cluster.init(mUser.empty() ? 0 : mUser.c_str());

  if (cluster.conf_read_file(mConf) != 0)
    throw std::invalid_argument("Problem reading configuration file.");

  cluster.connect();

  if (createPools)
  {
    cluster.pool_create(mMtdPool.c_str());
    cluster.pool_create(mDataPool.c_str());
    mCreatedPools = true;
  }

  cluster.shutdown();
}

BenchmarkMgr::~BenchmarkMgr()
{
  librados::Rados cluster;
  cluster.init(mUser.empty() ? 0 : mUser.c_str());

  cluster.conf_read_file(mConf);
  cluster.connect();

  if (mCreatedPools)
  {
    cluster.pool_delete(mMtdPool.c_str());
    cluster.pool_delete(mDataPool.c_str());
  }
  else if (mDeleteObjects)
  {
    std::list<std::string> pools;
    pools.push_back(mMtdPool);
    pools.push_back(mDataPool);

    std::list<std::string>::const_iterator it;
    for (it = pools.begin(); it != pools.end(); it++)
    {
      librados::IoCtx ioctx;
      if (cluster.ioctx_create((*it).c_str(), ioctx) == 0)
      {
        librados::NObjectIterator oit;
        for (oit = ioctx.nobjects_begin(); oit != ioctx.nobjects_end(); oit++)
        {
          ioctx.remove((*oit).get_oid());
        }
      }
    }
  }

  cluster.shutdown();
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

int
BenchmarkMgr::setupPools()
{
  int ret = radosFs.init(mUser, mConf);

  if (ret != 0)
    return ret;

  ret = radosFs.addDataPool(mDataPool, "/", mMaxFileSize);

  if (ret == 0)
    ret = radosFs.addMetadataPool(mMtdPool, "/");

  return ret;
}
