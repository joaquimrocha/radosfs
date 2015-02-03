#ifndef __RADOS_FS_LOGGER_HH__
#define __RADOS_FS_LOGGER_HH__

#include <boost/thread.hpp>
#include <string>

#include "radosfsdefines.h"
#include "Filesystem.hh"

#define radosfs_debug(...) radosfs::Logger::log(__FILE__, \
                                        __LINE__, \
                                        radosfs::Filesystem::LOG_LEVEL_DEBUG, \
                                        __VA_ARGS__)

RADOS_FS_BEGIN_NAMESPACE

class Logger
{
public:
  Logger();
  ~Logger();

  static Filesystem::LogLevel level;

  static void log(const char *file,
                  const int line,
                  const Filesystem::LogLevel l,
                  const char *msg,
                  ...);

  void setLogLevel(const Filesystem::LogLevel level);
  Filesystem::LogLevel logLevel(void);

private:
  static const int mBufferMaxSize = 1024;
  boost::mutex mLevelMutex;
  boost::thread thread;
};

RADOS_FS_END_NAMESPACE

#endif // __RADOS_FS_LOGGER_HH__
