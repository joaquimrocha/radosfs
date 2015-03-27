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

RadosFsChecker::RadosFsChecker(radosfs::Filesystem *radosFs)
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
  int numThreads = 10;
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
RadosFsChecker::checkDir(StatSP parentStat, std::string path,
                         bool recursive,
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
      ioService->post(boost::bind(&RadosFsChecker::checkDir, this, parentStat,
                                  entryPath, recursive, diagnostic));
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
RadosFsChecker::checkDirInThread(StatSP parentStat, std::string path,
                                 bool recursive, DiagnosticSP diagnostic)
{
  ioService->post(boost::bind(&RadosFsChecker::checkDir, this, parentStat,
                              path, recursive, diagnostic));
}

void
RadosFsChecker::checkInodeBackLink(const std::string &inode,
                                   const std::string &backLink,
                                   DiagnosticSP diagnostic)
{
  Stat fileStat;
  int ret = mRadosFs->mPriv->stat(backLink, &fileStat);

  if (ret != 0)
  {
    Issue issue(inode, WRONG_BACK_LINK);
    issue.extraInfo.append("problem statting inode's backlink '" + backLink +
                           "'.");
    diagnostic->addFileIssue(issue);
  }
  else
  {
    if (inode != fileStat.translatedPath)
    {
      Issue issue(inode, WRONG_BACK_LINK);
      issue.extraInfo.append(fileStat.path + " points to '" +
                             fileStat.translatedPath + "' instead.");
      diagnostic->addFileIssue(issue);
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
      diagnostic->addFileIssue(issue);
    }

    return;
  }

  std::map<std::string, librados::bufferlist> xattrs;

  int ret = pool->ioctx.getxattrs(inode, xattrs);

  if (ret < 0)
  {
    Issue issue(inode, ret);
    diagnostic->addFileIssue(issue);
    return;
  }

  std::map<std::string, librados::bufferlist>::const_iterator it;

  it = xattrs.find(XATTR_INODE_HARD_LINK);
  if (it != xattrs.end())
  {
    librados::bufferlist backLink = (*it).second;
    if (backLink.length() == 0)
    {
      Issue issue(inode, WRONG_BACK_LINK);
      diagnostic->addFileIssue(issue);
    }
    else
    {
      std::string backLinkStr(backLink.c_str(), backLink.length());
      checkInodeBackLink(inode, backLinkStr, diagnostic);
    }
  }

  it = xattrs.find(XATTR_MTIME);
  if (it == xattrs.end())
  {
    Issue issue(inode, NO_MTIME);
    diagnostic->addFileIssue(issue);
  }
}

void
RadosFsChecker::checkInodeInThread(PoolSP pool, const std::string &inode,
                                   DiagnosticSP diagnostic)
{
  ioService->post(boost::bind(&RadosFsChecker::checkInode, this, pool,
                              inode, diagnostic));
}

void
RadosFsChecker::checkInodes(PoolSP pool,
                            boost::shared_ptr<Diagnostic> diagnostic)
{
  librados::ObjectIterator it;

  for (it = pool->ioctx.objects_begin(); it != pool->ioctx.objects_end(); it++)
  {
    const std::string &inode = (*it).first;
    checkInodeInThread(pool, inode, diagnostic);
  }
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

  size_t totalIssues = fileIssues.size() + dirIssues.size();
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
}
