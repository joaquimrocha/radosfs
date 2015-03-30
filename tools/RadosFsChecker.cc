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

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <rados/librados.hpp>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "RadosFsChecker.hh"
#include "FilesystemPriv.hh"

#define ANIMATION_STEP_TIMEOUT 150 // ms

void
RadosFsChecker::generalWorkerThread(
    boost::shared_ptr<boost::asio::io_service> ioService)
{
  ioService->run();
}

static std::map<ErrorCode, std::string>
createErrorsDescription()
{
  std::map<ErrorCode, std::string> errorDescription;
  errorDescription[NO_ENT] = "NO_ENT";
  errorDescription[NO_LINK] = "NO_LINK";
  errorDescription[NO_MTIME] = "NO_MTIME";
  errorDescription[NO_CTIME] = "NO_CTIME";
  errorDescription[NO_INODE] = "NO_INODE";
  errorDescription[NO_MODE] = "NO_MODE";
  errorDescription[NO_UID] = "NO_UID";
  errorDescription[NO_GID] = "NO_GID";
  errorDescription[NO_POOL] = "NO_POOL";
  errorDescription[NO_INLINE_BUFFER_SIZE] = "NO_INLINE_BUFFER_SIZE";
  errorDescription[NO_INLINEBUFFER] = "NO_INLINEBUFFER";
  errorDescription[FILE_ENTRY_NO_LINK] = "FILE_ENTRY_NO_LINK";
  errorDescription[EMPTY_FILE_ENTRY] = "EMPTY_FILE_ENTRY";
  errorDescription[NO_BACK_LINK] = "NO_BACK_LINK";
  errorDescription[WRONG_BACK_LINK] = "WRONG_BACK_LINK";
  errorDescription[BACK_LINK_NO_ENT] = "BACK_LINK_NO_ENT";
  errorDescription[NO_ERROR] = "NO_ERROR";
  errorDescription[INODE_NO_SIZE] = "INODE_NO_SIZE";
  errorDescription[LOOSE_INODE_STRIPE] = "LOOSE_INODE_STRIPE";
  return errorDescription;
}

RadosFsChecker::RadosFsChecker(radosfs::Filesystem *radosFs, size_t numThreads)
  : errorsDescription(createErrorsDescription()),
    mRadosFs(radosFs),
    ioService(new boost::asio::io_service),
    asyncWork(new boost::asio::io_service::work(*ioService)),
    mAnimationStep(0),
    mAnimationLastUpdate(boost::chrono::system_clock::now()),
    mAnimation("|/-\\"),
    mVerbose(false),
    mFix(false),
    mDry(false)
{
  while (numThreads-- > 0)
    generalWorkerThreads.create_thread(
          boost::bind(&RadosFsChecker::generalWorkerThread, this, ioService));
}

static bool
verifyPairsInEntry(const std::string &path,
                   const std::string &entry, const char **keysToCheck,
                   const ErrorCode *errorCodes,
                   boost::shared_ptr<Diagnostic> diagnostic)
{
  bool noIssues = true;
  std::map<std::string, std::string> entryMap;
  entryMap = stringAttrsToMap(entry);

  for (int i = 0; keysToCheck[i] != 0; i++)
  {
    std::map<std::string, std::string>::iterator it;
    it = entryMap.find(keysToCheck[i]);
    if (it == entryMap.end() || (*it).second == "")
    {
      Issue issue(path, errorCodes[i]);

      if (isDirPath(path))
        diagnostic->addDirIssue(issue);
      else
        diagnostic->addFileIssue(issue);

      noIssues = false;
    }
  }

  return noIssues;
}

bool
RadosFsChecker::verifyDirObject(Stat &stat,
                                std::map<std::string, librados::bufferlist> &omap,
                                DiagnosticSP diagnostic)
{
  bool noIssues = true;
  const char * keysToCheck[] = {XATTR_INODE_HARD_LINK, XATTR_CTIME, XATTR_MTIME,
                                0};
  const ErrorCode errorCodes[] = {NO_BACK_LINK, NO_CTIME, NO_MTIME, NO_ERROR};

  for (int i = 0; keysToCheck[i] != 0; i++)
  {
    std::map<std::string, librados::bufferlist>::const_iterator it;
    it = omap.find(keysToCheck[i]);

    librados::bufferlist buff;

    if (it != omap.end())
      buff = (*it).second;

    if (buff.length() == 0)
    {
      Issue issue(stat.path, errorCodes[i]);
      issue.extraInfo = stat.translatedPath;

      if (strcmp(keysToCheck[i], XATTR_INODE_HARD_LINK) == 0)
      {
        if (mFix)
        {
          int ret = 0;

          if (!mDry)
          {
            ret = setDirInodeBackLink(stat.pool.get(), stat.translatedPath,
                                      stat.path);
          }

          if (ret == 0)
            issue.setFixed();
        }
      }

      diagnostic->addDirIssue(issue);
      noIssues = false;
    }
  }

  std::map<std::string, librados::bufferlist>::const_iterator it;
  it = omap.find(XATTR_PERMISSIONS);

  if (it != omap.end())
  {
    const char * keysToCheck[] = {UID_KEY, GID_KEY, MODE_KEY, 0};
    const ErrorCode errorCodes[] = {NO_UID, NO_GID, NO_MODE, NO_ERROR};

    librados::bufferlist buff = (*it).second;
    const std::string permissions(buff.c_str(), buff.length());

    if (!verifyPairsInEntry(stat.path, permissions, keysToCheck, errorCodes,
                            diagnostic))
    {
      noIssues = false;
    }
  }

  return noIssues;
}

int
RadosFsChecker::verifyFileObject(const std::string path,
                                 std::map<std::string, librados::bufferlist> &omap,
                                 DiagnosticSP diagnostic)
{
  int ret = 0;
  const std::string parentPath = getParentDir(path, 0);
  const std::string baseName = path.substr(parentPath.length());
  std::map<std::string, librados::bufferlist>::iterator it;

  it = omap.find(XATTR_FILE_PREFIX + baseName);

  if (it == omap.end())
  {
    Issue issue(path, FILE_ENTRY_NO_LINK);
    diagnostic->addFileIssue(issue);
    return ret;
  }

  librados::bufferlist fileEntry = (*it).second;
  const std::string fileEntryContents(fileEntry.c_str(), fileEntry.length());

  if (fileEntryContents.empty())
  {
    Issue issue(path, EMPTY_FILE_ENTRY);
    diagnostic->addFileIssue(issue);
    return ret;
  }

  std::map<std::string, std::string> fileEntryMap;
  fileEntryMap = stringAttrsToMap(fileEntryContents);

  const char * keysToCheck[] = {LINK_KEY, UID_KEY, GID_KEY, MODE_KEY, TIME_KEY,
                                XATTR_FILE_INLINE_BUFFER_SIZE,
                                0};
  const ErrorCode errorCodes[] = {NO_LINK, NO_UID, NO_GID, NO_MODE, NO_CTIME,
                                  NO_INLINE_BUFFER_SIZE,
                                  NO_ERROR};

  if (!verifyPairsInEntry(path, fileEntryContents, keysToCheck, errorCodes,
                          diagnostic))
  {
    return ret;
  }

  Stat stat;
  ret = mRadosFs->mPriv->stat(path, &stat);

  if (ret < 0)
  {
    Issue issue(path, ret);
    diagnostic->addFileIssue(issue);
    return ret;
  }

  std::string backLink;
  ret = getFileInodeBackLink(stat.pool.get(), stat.translatedPath, &backLink);

  if (ret == -ENOENT)
  {
    // We can have files without the inode object so we return success when
    // the file has no inode
    return 0;
  }

  if (backLink != path)
  {
    int errorCode = backLink.empty() ? NO_BACK_LINK : WRONG_BACK_LINK;
    Issue issue(path, errorCode);
    issue.extraInfo = stat.translatedPath;

    if (mFix)
    {
      int ret = 0;

      if (!mDry)
        ret = setFileInodeBackLink(stat.pool.get(), stat.translatedPath, path);

      if (ret == 0)
        issue.setFixed();
    }

    diagnostic->addFileIssue(issue);
    return ret;
  }

  return 0;
}

void
RadosFsChecker::checkPath(std::string path, DiagnosticSP diagnostic)
{
  Stat stat;
  int ret = mRadosFs->mPriv->stat(path, &stat);

  animate();

  if (ret < 0)
  {
    Issue issue(path, ret);
    diagnostic->addFileIssue(issue);
    return;
  }

  if (S_ISDIR(stat.statBuff.st_mode))
  {
    std::map<std::string, librados::bufferlist> omap;
    stat.pool->ioctx.omap_get_vals(stat.translatedPath, "", "", UINT_MAX, &omap);

    verifyDirObject(stat, omap, diagnostic);
    return;
  }

  if (S_ISREG(stat.statBuff.st_mode))
  {
    Stat parentStat;
    std::string parentDir = getParentDir(stat.path, 0);
    ret = mRadosFs->mPriv->stat(parentDir, &parentStat);

    if (ret < 0)
    {
      Issue issue(parentDir, ret);
      issue.extraInfo.append("When checking path " + path);
      diagnostic->addDirIssue(issue);
      return;
    }

    std::map<std::string, librados::bufferlist> omap;
    parentStat.pool->ioctx.omap_get_vals(parentStat.translatedPath, "", "",
                                         UINT_MAX, &omap);

    verifyFileObject(stat.path, omap, diagnostic);
  }
}

void
RadosFsChecker::checkPathInThread(std::string path, DiagnosticSP diagnostic)
{
  ioService->post(boost::bind(&RadosFsChecker::checkPath, this, path,
                              diagnostic));
}

void
RadosFsChecker::checkDir(std::string path, bool recursive,
                         boost::shared_ptr<Diagnostic> diagnostic)
{
  Stat stat;
  int ret = mRadosFs->mPriv->stat(path, &stat);

  animate();

  log("Checking dir '%s'...\n", path.c_str());

  if (ret < 0)
  {
    Issue issue(path, ret);
    diagnostic->addDirIssue(issue);

    log(" Error in '%s': %d\n", path.c_str(), ret);

    return;
  }

  radosfs::Dir dir(mRadosFs, path);

  dir.update();

  std::set<std::string> entries;
  ret = dir.entryList(entries);

  if (ret < 0)
  {
    Issue issue(dir.path(), ret);
    issue.errorCode = ret;

    log(" Error in '%s': %d\n", path.c_str(), ret);

    diagnostic->addDirIssue(issue);
  }

  std::map<std::string, librados::bufferlist> omap;
  stat.pool->ioctx.omap_get_vals(stat.translatedPath, "", "", UINT_MAX, &omap);

  verifyDirObject(stat, omap, diagnostic);

  std::set<std::string>::iterator it;
  for (it = entries.begin(); it != entries.end(); it++)
  {
    Stat stat;
    const std::string entryName = *it;
    const std::string entryPath = dir.path() + entryName;
    ret = mRadosFs->mPriv->stat(entryPath, &stat);

    log(" Checking entry '%s' of '%s'\n", entryName.c_str(), dir.path().c_str());

    if (ret < 0)
    {
      Issue issue(entryPath, ret);

      if (isDirPath(entryPath))
      {
        log(" Error in '%s': %d\n", entryPath.c_str(), ret);

        diagnostic->addDirIssue(issue);
      }
      else
      {
        if (ret == -ENOENT)
          issue.errorCode = FILE_ENTRY_NO_LINK;

        log(" Error in '%s': %d\n", entryPath.c_str(), issue.errorCode);

        diagnostic->addFileIssue(issue);
      }

      continue;
    }

    if (S_ISREG (stat.statBuff.st_mode))
    {
      ret = verifyFileObject(dir.path() + entryName, omap, diagnostic);
      continue;
    }
    else if (recursive)
    {
      ioService->post(boost::bind(&RadosFsChecker::checkDir, this, entryPath,
                                  recursive, diagnostic));
    }
    else
    {
      std::map<std::string, librados::bufferlist> omap;
      stat.pool->ioctx.omap_get_vals(stat.translatedPath, "", "", UINT_MAX,
                                     &omap);

      verifyDirObject(stat, omap, diagnostic);
    }
  }
}

void
RadosFsChecker::checkDirInThread(std::string path, bool recursive,
                                 DiagnosticSP diagnostic)
{
  ioService->post(boost::bind(&RadosFsChecker::checkDir, this, path, recursive,
                              diagnostic));
}

int
RadosFsChecker::fixInodeBackLink(Stat &backLinkStat, const std::string &inode,
                                 Pool &pool, Issue &issue)
{
  int ret = 0;

  if (!mDry)
  {
    backLinkStat.translatedPath = inode;

    if (isDirPath(backLinkStat.path))
    {
      if (!backLinkStat.pool)
        backLinkStat.pool =
            mRadosFs->mPriv->getMetadataPoolFromPath(backLinkStat.path);

      ret = mRadosFs->mPriv->resetDirLogicalObj(backLinkStat);
    }
    else
    {
      ret = mRadosFs->mPriv->resetFileEntry(backLinkStat);
    }
  }

  if (ret == 0)
  {
    issue.extraInfo.append("Now points to '" + inode + "' in pool '"
                           + pool.name + "'.");
    issue.setFixed();
  }

  return ret;
}

void
RadosFsChecker::checkInodeBackLink(const std::string &inode,
                                   Pool &pool,
                                   const std::string &backLink,
                                   DiagnosticSP diagnostic)
{
  Stat backLinkStat;
  backLinkStat.path = backLink;
  PoolSP mtdPool = mRadosFs->mPriv->getMetadataPoolFromPath(backLink);
  bool isDirBackLink = isDirPath(backLink);

  int ret;

  if (isDirBackLink)
    ret = mRadosFs->mPriv->statDir(mtdPool, &backLinkStat);
  else
    ret = mRadosFs->mPriv->statFile(mtdPool, &backLinkStat);

  if (ret != 0 && ret != -ENODATA && ret != -ENOLINK)
  {
    Issue issue(inode, WRONG_BACK_LINK);
    issue.extraInfo.append("problem statting inode's backlink '" + backLink +
                           ".");
    diagnostic->addInodeIssue(issue);
  }
  else if (inode != backLinkStat.translatedPath)
  {
    if (backLinkStat.translatedPath.empty())
    {
      Issue issue(backLinkStat.path, NO_INODE);

      if (mFix)
        ret = fixInodeBackLink(backLinkStat, inode, pool, issue);

      if (isDirBackLink)
        diagnostic->addDirIssue(issue);
      else
        diagnostic->addFileIssue(issue);
    }
    else
    {
      Issue issue(inode, WRONG_BACK_LINK);
      issue.extraInfo.append(backLinkStat.path + " points to '" +
                             backLinkStat.translatedPath + "' in pool '" +
                             backLinkStat.pool->name + "' instead.");
      diagnostic->addInodeIssue(issue);
    }
  }
}

void
RadosFsChecker::checkInodeKeys(const std::string &inode, Pool &pool,
                        const std::map<std::string, librados::bufferlist> &keys,
                        DiagnosticSP diagnostic, bool isFile)
{
  std::map<std::string, librados::bufferlist>::const_iterator it;

  it = keys.find(XATTR_INODE_HARD_LINK);
  if (it == keys.end())
  {
    Issue issue(inode, NO_BACK_LINK);
    diagnostic->addInodeIssue(issue);
  }
  else
  {
    librados::bufferlist backLink = (*it).second;
    if (backLink.length() == 0)
    {
      Issue issue(inode, WRONG_BACK_LINK);
      diagnostic->addInodeIssue(issue);
    }
    else if (mHasPools)
    {
      std::string backLinkStr(backLink.c_str(), backLink.length());
      checkInodeBackLink(inode, pool, backLinkStr, diagnostic);
    }
  }

  it = keys.find(XATTR_MTIME);
  if (it == keys.end())
  {
    Issue issue(inode, NO_MTIME);
    diagnostic->addInodeIssue(issue);
  }

  if (isFile)
  {
    it = keys.find(XATTR_FILE_SIZE);
    if (it == keys.end())
    {
      Issue issue(inode, INODE_NO_SIZE);
      diagnostic->addInodeIssue(issue);
    }
  }
}

void
RadosFsChecker::checkInode(PoolSP pool, std::string inode,
                           boost::shared_ptr<Diagnostic> diagnostic)
{
  animate();

  if (nameIsStripe(inode))
  {
    std::string baseInode = getBaseInode(inode);

    if (pool->ioctx.stat(baseInode, 0, 0) != 0)
    {
      Issue issue(inode, LOOSE_INODE_STRIPE);
      issue.extraInfo.append("in pool '" + pool->name + "'");
      diagnostic->addInodeIssue(issue);
    }

    return;
  }

  std::map<std::string, librados::bufferlist> *keyValueMap;
  std::map<std::string, librados::bufferlist> xattrs;
  std::map<std::string, librados::bufferlist> omap;

  int getXattrsRet, getOmapRet;
  librados::ObjectReadOperation readOp;
  readOp.getxattrs(&xattrs, &getXattrsRet);
  readOp.omap_get_vals("", XATTR_RADOSFS_PREFIX, UINT_MAX, &omap, &getOmapRet);

  int ret = pool->ioctx.operate(inode, &readOp, 0);

  bool inodeIsFile = false;

  if (!omap.size() || xattrs.count(XATTR_FILE_SIZE) > 0)
    inodeIsFile = true;

  if (ret < 0)
  {
    Issue issue(inode, ret);
    diagnostic->addInodeIssue(issue);
    return;
  }

  if (inodeIsFile)
    keyValueMap = &xattrs;
  else
    keyValueMap = &omap;

  checkInodeKeys(inode, *pool.get(), *keyValueMap, diagnostic, inodeIsFile);
}

void
RadosFsChecker::checkInodeInThread(PoolSP pool, const std::string &inode,
                                   DiagnosticSP diagnostic)
{
  if (!nameIsInode(inode))
    return;

  ioService->post(boost::bind(&RadosFsChecker::checkInode, this, pool,
                              inode, diagnostic));
}

void
RadosFsChecker::checkInodes(PoolSP pool, DiagnosticSP diagnostic)
{
  librados::ObjectIterator it;

  for (it = pool->ioctx.objects_begin(); it != pool->ioctx.objects_end(); it++)
  {
    const std::string &inode = (*it).first;
    checkInodeInThread(pool, inode, diagnostic);
  }
}

void
RadosFsChecker::checkInodesInThread(PoolSP pool, DiagnosticSP diagnostic)
{
  if (pool)
    ioService->post(boost::bind(&RadosFsChecker::checkInodes, this, pool,
                                diagnostic));
}

void
RadosFsChecker::checkInodes(DiagnosticSP diagnostic)
{
  std::vector<PoolSP> pools = mRadosFs->mPriv->getDataPools();
  const std::vector<PoolSP> &mtdPools = mRadosFs->mPriv->getMtdPools();
  pools.insert(pools.end(), mtdPools.begin(), mtdPools.end());

  std::vector<PoolSP>::iterator it;
  for (it = pools.begin(); it != pools.end(); it++)
    checkInodesInThread((*it), diagnostic);
}

void
RadosFsChecker::finishCheck(void)
{
  asyncWork.reset();
  generalWorkerThreads.join_all();
}

void
RadosFsChecker::animate()
{
  boost::unique_lock<boost::mutex> lock(mAnimationMutex);
  boost::chrono::system_clock::time_point now = boost::chrono::system_clock::now();
  boost::chrono::duration<double> timeDiff;
  timeDiff = now - mAnimationLastUpdate;

  if (timeDiff < boost::chrono::milliseconds(ANIMATION_STEP_TIMEOUT))
    return;

  mAnimationLastUpdate = now;
  mAnimationStep %= mAnimation.length();

  fprintf(stdout, " Checking %c\r", mAnimation[mAnimationStep++]);
  fflush(stdout);
}

void
RadosFsChecker::log(const char *msg, ...)
{
  if (!mVerbose)
    return;

  va_list args;

  va_start(args, msg);

  vfprintf(stderr, msg, args);

  va_end(args);
}

PoolSP
RadosFsChecker::getPool(const std::string &name)
{
  PoolSP pool = mRadosFs->mPriv->getDataPoolFromName(name);

  if (!pool)
    pool = mRadosFs->mPriv->getMtdPoolFromName(name);

  if (!pool)
  {
    librados::IoCtx ioctx;
    if (mRadosFs->mPriv->radosCluster.ioctx_create(name.c_str(), ioctx) == 0)
      pool.reset(new Pool(name, 0, ioctx));
  }

  return pool;
}

void
Issue::print(const std::map<ErrorCode, std::string> &errors)
{
  std::map<ErrorCode, std::string>::const_iterator it =
      errors.find((ErrorCode) errorCode);

  if (it != errors.end())
  {
    const std::string &error = (*it).second;
    fprintf(stdout, "%-20s", error.c_str());
  }
  else
  {
    fprintf(stdout, "%-20d", errorCode);
  }

  fprintf(stdout, "   %s%s\n", path.c_str(),
          extraInfo.empty() ? "" : (" : " + extraInfo).c_str());
}

void
Diagnostic::addFileIssue(const Issue &issue)
{
  addIssue(issue, fileIssues, fileIssuesMutex, fileSolvedIssues,
           fileSolvedIssuesMutex);
}

void
Diagnostic::addDirIssue(const Issue &issue)
{
  addIssue(issue, dirIssues, dirIssuesMutex, dirSolvedIssues,
           dirSolvedIssuesMutex);
}

void
Diagnostic::addInodeIssue(const Issue &issue)
{
  addIssue(issue, inodeIssues, inodeIssuesMutex, inodeSolvedIssues,
           inodeSolvedIssuesMutex);
}

void
Diagnostic::addIssue(const Issue &issue, std::vector<Issue> &issues,
                     boost::mutex &issuesMutex, std::vector<Issue> &fixedIssues,
                     boost::mutex &fixedIssuesMutex)
{
  if (issue.fixed)
  {
    boost::unique_lock<boost::mutex> lock(fixedIssuesMutex);
    fixedIssues.push_back(issue);
  }
  else
  {
    boost::unique_lock<boost::mutex> lock(issuesMutex);
    issues.push_back(issue);
  }
}

void
Diagnostic::print(const std::map<ErrorCode, std::string> &errors, bool dry)
{
  fprintf(stdout, " Checking done \r");
  fflush(stdout);

  size_t totalIssues = fileIssues.size() + dirIssues.size() + inodeIssues.size();
  fprintf(stdout, "\n\nIssues found: %lu\n", totalIssues);


  std::vector<Issue>::iterator it;

  if (totalIssues > 0)
  {
    fprintf(stdout, "\nFile issues: %lu\n", fileIssues.size());
    for (it = fileIssues.begin(); it != fileIssues.end(); it++)
    {
      (*it).print(errors);
    }

    fprintf(stdout, "\nDirectory issues: %lu\n", dirIssues.size());
    for (it = dirIssues.begin(); it != dirIssues.end(); it++)
    {
      (*it).print(errors);
    }

    fprintf(stdout, "\nInode issues: %lu\n", inodeIssues.size());
    for (it = inodeIssues.begin(); it != inodeIssues.end(); it++)
    {
      (*it).print(errors);
    }
  }

  if (fileSolvedIssues.size() > 0)
  {
    if (!dry)
      fprintf(stdout, "\nFile issues solved: ");
    else
      fprintf(stdout, "\nFile issues that would be solved: ");

    fprintf(stdout, "%lu\n", fileSolvedIssues.size());

    for (it = fileSolvedIssues.begin(); it != fileSolvedIssues.end(); it++)
    {
      (*it).print(errors);
    }
  }

  if (dirSolvedIssues.size() > 0)
  {
    if (!dry)
      fprintf(stdout, "\nDir issues solved: ");
    else
      fprintf(stdout, "\nDir issues that would be solved: ");

    fprintf(stdout, "%lu\n", dirSolvedIssues.size());

    for (it = dirSolvedIssues.begin(); it != dirSolvedIssues.end(); it++)
    {
      (*it).print(errors);
    }
  }

  if (inodeSolvedIssues.size() > 0)
  {
    if (!dry)
      fprintf(stdout, "\nInode issues solved: ");
    else
      fprintf(stdout, "\nInode issues that would be solved: ");

    fprintf(stdout, "%lu\n", inodeSolvedIssues.size());

    for (it = inodeSolvedIssues.begin(); it != inodeSolvedIssues.end(); it++)
    {
      (*it).print(errors);
    }
  }
}
