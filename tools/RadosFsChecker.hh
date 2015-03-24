/*
 * Rados Filesystem - A filesystem library based in librados
 *
 * Copyright (C) 2014 CERN, Switzerland
 *
 * Author: Joaquim Rocha <joaquim.rocha@cern.ch>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License at http://www.gnu.org/licenses/lgpl-3.0.txt
 * for more details.
 */

#include "Filesystem.hh"
#include "Dir.hh"
#include "File.hh"
#include "radosfscommon.h"

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <map>
#include <set>
#include <string>

#ifndef __RADOS_FS_CHECKER_HH__
#define __RADOS_FS_CHECKER_HH__

enum ErrorCode
{
  NO_ENT = -ENOENT,
  NO_LINK = -ENOLINK,
  NO_MTIME = 1,
  NO_CTIME,
  NO_INODE,
  NO_MODE,
  NO_UID,
  NO_GID,
  NO_POOL,
  NO_INLINE_BUFFER_SIZE,
  NO_INLINEBUFFER,
  FILE_ENTRY_NO_LINK,
  EMPTY_FILE_ENTRY,
  NO_BACK_LINK,
  WRONG_BACK_LINK,
  BACK_LINK_NO_ENT,

  NO_ERROR
};

struct Issue
{
  Issue (const std::string &path, int error)
    : path(path),
      errorCode(error),
      fixed(false)
  {}

  std::string path;
  std::string extraInfo;
  int errorCode;
  bool fixed;

  void print(const std::map<ErrorCode, std::string> &errors);
  void setFixed(void) { fixed = true; }
};

struct Diagnostic
{
  std::vector<Issue> fileIssues;
  std::vector<Issue> dirIssues;
  std::vector<Issue> fileSolvedIssues;
  std::vector<Issue> dirSolvedIssues;
  boost::mutex fileIssuesMutex;
  boost::mutex dirIssuesMutex;
  boost::mutex fileSolvedIssuesMutex;
  boost::mutex dirSolvedIssuesMutex;

  void addFileIssue(const Issue &issue);
  void addDirIssue(const Issue &issue);
  void print(const std::map<ErrorCode, std::string> &errors, bool dry);
};

typedef boost::shared_ptr<Diagnostic> DiagnosticSP;

class RadosFsChecker
{
public:
  RadosFsChecker(radosfs::Filesystem *radosFs);

  void checkDir(StatSP parentStat, std::string path, bool recursive,
                DiagnosticSP diagnostic);

  void checkDirInThread(StatSP parentStat, std::string path, bool recursive,
                        DiagnosticSP diagnostic);

  void animate(void);

  const std::map<ErrorCode, std::string> errorsDescription;

  void finishCheck(void);

  void setVerbose(bool verbose) { mVerbose = verbose; }

  void setDry(bool dry) { mDry = dry; }

  void setFix(bool fix) { mFix = fix; }

private:
  bool checkPath(const std::string &path);
  void checkDirRecursive(const std::string &path);

  void generalWorkerThread(boost::shared_ptr<boost::asio::io_service> ioService);

  int verifyFileObject(const std::string path,
                       std::map<std::string, librados::bufferlist> &omap,
                       DiagnosticSP diagnostic);

  bool verifyDirObject(Stat &stat,
                       std::map<std::string, librados::bufferlist> &omap,
                       DiagnosticSP diagnostic);

  void log(const char *msg, ...);

  radosfs::Filesystem *mRadosFs;
  boost::shared_ptr<boost::asio::io_service> ioService;
  boost::shared_ptr<boost::asio::io_service::work> asyncWork;
  boost::thread_group generalWorkerThreads;
  boost::mutex mAnimationMutex;
  size_t mAnimationStep;
  boost::chrono::system_clock::time_point mAnimationLastUpdate;
  const std::string mAnimation;
  bool mVerbose;
  bool mFix;
  bool mDry;
};

#endif // __RADOS_FS_CHECKER_HH__
