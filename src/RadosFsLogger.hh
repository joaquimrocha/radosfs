#ifndef __RADOS_FS_LOGGER_HH__
#define __RADOS_FS_LOGGER_HH__

#include <string>

#include "radosfsdefines.h"
#include "RadosFs.hh"

RADOS_FS_BEGIN_NAMESPACE

#define radosfs_debug(...) RadosFsLogger::log(__FILE__, \
                                              __LINE__, \
                                              RadosFs::LOG_LEVEL_DEBUG, \
                                              __VA_ARGS__)

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

private:
  static const int mBufferMaxSize = 1024;
  pthread_t thread;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_LOGGER_HH__
