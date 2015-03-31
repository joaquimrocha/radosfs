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
  INODE_NO_SIZE,
  LOOSE_INODE_STRIPE,

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
  std::vector<Issue> inodeIssues;
  std::vector<Issue> fileSolvedIssues;
  std::vector<Issue> dirSolvedIssues;
  std::vector<Issue> inodeSolvedIssues;
  boost::mutex fileIssuesMutex;
  boost::mutex dirIssuesMutex;
  boost::mutex inodeIssuesMutex;
  boost::mutex fileSolvedIssuesMutex;
  boost::mutex dirSolvedIssuesMutex;
  boost::mutex inodeSolvedIssuesMutex;

  void addFileIssue(const Issue &issue);
  void addDirIssue(const Issue &issue);
  void addInodeIssue(const Issue &issue);
  void addIssue(const Issue &issue, std::vector<Issue> &issues,
                boost::mutex &issuesMutex,
                std::vector<Issue> &fixedIssues,
                boost::mutex &fixedIssuesMutex);
  void print(const std::map<ErrorCode, std::string> &errors, bool dry);
};

typedef boost::shared_ptr<Diagnostic> DiagnosticSP;

class RadosFsChecker
{
public:
  RadosFsChecker(radosfs::Filesystem *radosFs, size_t numThreads);

  void checkPath(std::string path, DiagnosticSP diagnostic);

  void checkPathInThread(std::string path, DiagnosticSP diagnostic);

  void checkDir(std::string path, bool recursive, DiagnosticSP diagnostic);

  void checkDirInThread(std::string path, bool recursive,
                        DiagnosticSP diagnostic);

  void checkInodes(PoolSP pool, DiagnosticSP diagnostic);

  void checkInodesInThread(PoolSP pool, DiagnosticSP diagnostic);

  void checkInodes(DiagnosticSP diagnostic);

  void animate(void);

  const std::map<ErrorCode, std::string> errorsDescription;

  void finishCheck(void);

  void setVerbose(bool verbose) { mVerbose = verbose; }

  void setDry(bool dry) { mDry = dry; }

  void setFix(bool fix) { mFix = fix; }

  void setHasPools(bool hasPools) { mHasPools = hasPools; }

  PoolSP getPool(const std::string &name);

private:
  void checkDirRecursive(const std::string &path);

  void generalWorkerThread(boost::shared_ptr<boost::asio::io_service> ioService);

  int verifyFileObject(Stat &stat,
                       std::map<std::string, librados::bufferlist> &omap,
                       DiagnosticSP diagnostic);

  bool verifyDirObject(Stat &stat,
                       std::map<std::string, librados::bufferlist> &omap,
                       DiagnosticSP diagnostic);

  void checkInode(PoolSP pool, std::string inode, DiagnosticSP diagnostic);

  void checkInodeInThread(PoolSP pool, const std::string &inode,
                          DiagnosticSP diagnostic);

  void checkInodeBackLink(const std::string &inode, Pool &pool,
                          const std::string &backLink,
                          DiagnosticSP diagnostic);

  void checkInodeKeys(const std::string &inode, Pool &pool,
                      const std::map<std::string, librados::bufferlist> &keys,
                      DiagnosticSP diagnostic, bool isFile);

  int fixInodeBackLink(Stat &backLinkStat, const std::string &inode,
                       Pool &pool, Issue &issue);

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
  bool mHasPools;
};

#endif // __RADOS_FS_CHECKER_HH__
