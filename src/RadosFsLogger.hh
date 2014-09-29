#ifndef __RADOS_FS_LOGGER_HH__
#define __RADOS_FS_LOGGER_HH__

#include <string>

#include "radosfsdefines.h"
#include "RadosFs.hh"

#define radosfs_debug(...) radosfs::RadosFsLogger::log(__FILE__, \
                                            __LINE__, \
                                            radosfs::RadosFs::LOG_LEVEL_DEBUG, \
                                            __VA_ARGS__)

RADOS_FS_BEGIN_NAMESPACE

class RadosFsLogger
{
public:
  RadosFsLogger();
  ~RadosFsLogger();

  static RadosFs::LogLevel level;

  static void log(const char *file,
                  const int line,
                  const RadosFs::LogLevel l,
                  const char *msg,
                  ...);

  pthread_mutex_t * levelMutex(void) { return &mLevelMutex; }

  void setLogLevel(const RadosFs::LogLevel level);
  RadosFs::LogLevel logLevel(void);

private:
  static const int mBufferMaxSize = 1024;
  pthread_t thread;
  pthread_mutex_t mLevelMutex;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_LOGGER_HH__
