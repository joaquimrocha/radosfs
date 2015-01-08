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

#include "RadosFs.hh"
#include "RadosFsDir.hh"
#include "RadosFsFile.hh"

#include <map>
#include <set>
#include <string>

#ifndef __RADOS_FS_CHECKER_HH__
#define __RADOS_FS_CHECKER_HH__

class RadosFsChecker
{
public:
  RadosFsChecker(radosfs::Fs *radosFs);

  int check(void);
  void printIssues(void);
  int fixDirs(void);
  int fixInodes(void);
  void fix(void);
  void setVerbose(bool verbose) { mVerbose = verbose; }
  void setDry(bool dry) { mDry = dry; }

private:
  bool checkPath(const std::string &path);
  void checkDirRecursive(const std::string &path);

  radosfs::Fs *mRadosFs;
  bool mVerbose;
  bool mDry;
  std::map<std::string, std::string> mBrokenLinks;
  std::set<std::string> mBrokenFiles;
  std::map<std::string, std::string> mBrokenInodes;
  std::map<std::string, std::string> mBrokenDirs;
  std::map<std::string, std::string> mInodes;
  std::map<std::string, std::string> mBrokenStripes;
  std::map<std::string, std::string> mDirs;
};

#endif // __RADOS_FS_CHECKER_HH__
