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

#include <gtest/gtest.h>
#include <rados/librados.hpp>
#include <set>
#include <string>
#include "Filesystem.hh"
#include "FilesystemPriv.hh"
#include "File.hh"
#include "FilePriv.hh"
#include "DirPriv.hh"
#include "Dir.hh"

#ifndef RADOS_FS_TEST_HH
#define RADOS_FS_TEST_HH

#define CONF_ENV_VAR "RADOSFS_TEST_CLUSTER_CONF"
#define CONF_USR_VAR "RADOSFS_TEST_USER"

#define TEST_UID 1000
#define TEST_GID 1000
#define TEST_POOL "radosfs-unit-tests-pool-data"
#define TEST_POOL_MTD "radosfs-unit-tests-pool-mtd"

typedef enum {
  FS_ACTION_TYPE_FILE,
  FS_ACTION_TYPE_DIR
} FsActionType;

struct FsActionInfo
{
  radosfs::Filesystem *fs;
  FsActionType actionType;
  std::string path;
  std::string action;
  const char *contents;
  const size_t length;
  bool started;
  boost::mutex *mutex;
  boost::condition_variable *cond;

  FsActionInfo(radosfs::Filesystem *radosFs,
               FsActionType actionType,
               const std::string &path,
               std::string action,
               const char *contents,
               const size_t length,
               boost::mutex *mutex,
               boost::condition_variable *cond)
    : fs(radosFs),
      actionType(actionType),
      path(path),
      action(action),
      contents(contents),
      length(length),
      started(false),
      mutex(mutex),
      cond(cond)
  {}
};

class RadosFsTest : public testing::Test
{

public:
  RadosFsTest(void);
  ~RadosFsTest(void);

protected:
  virtual void SetUp();
  virtual void TearDown();

  void AddPool(int numExtraPools = 0);

  void testXAttrInFsInfo(radosfs::FsObj &info);

  void createNFiles(size_t numFiles);

  void removeNFiles(size_t numFiles);

  int createContentsRecursively(const std::string &prefix,
                                size_t numDirs,
                                size_t numFiles,
                                ssize_t levels);

  radosfs::Filesystem radosFs;

  radosfs::FilesystemPriv *radosFsPriv(void) const { return radosFs.mPriv; }
  radosfs::FilePriv *radosFsFilePriv(radosfs::File &file);
  radosfs::DirPriv *radosFsDirPriv(radosfs::Dir &dir);
  radosfs::FileInodePriv *fileInodePriv(radosfs::FileInode &inode) const;

  radosfs::File *launchFileOpsMultipleClients(const size_t stripeSize,
                                              const std::string &fileName,
                                              FsActionInfo *client1Action,
                                              FsActionInfo *client2Action);
  static void runInThread(void *contents);

  const char * conf(void) const { return mConf; }

  void testFileInodeBackLink(const std::string &path);

private:
  librados::Rados mCluster;
  const char *mConf;
  std::string mUser;
  std::set<std::string> mPoolsCreated;
};

#endif // RADOS_FS_TEST_HH
