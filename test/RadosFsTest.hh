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
#include "RadosFs.hh"
#include "RadosFsPriv.hh"
#include "RadosFsFile.hh"
#include "RadosFsFilePriv.hh"
#include "RadosFsDirPriv.hh"
#include "RadosFsDir.hh"

#ifndef RADOS_FS_TEST_HH
#define RADOS_FS_TEST_HH

#define CONF_ENV_VAR "RADOSFS_TEST_CLUSTER_CONF"

#define TEST_UID 1000
#define TEST_GID 1000
#define TEST_POOL "radosfs-unit-tests-pool-data"
#define TEST_POOL_MTD "radosfs-unit-tests-pool-mtd"

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

  radosfs::Fs radosFs;

  radosfs::FsPriv *radosFsPriv(void) const { return radosFs.mPriv; }
  radosfs::FilePriv *radosFsFilePriv(radosfs::File &file);
  radosfs::DirPriv *radosFsDirPriv(radosfs::Dir &dir);

  const char * conf(void) const { return mConf; }

private:
  librados::Rados mCluster;
  const char *mConf;
  std::set<std::string> mPoolsCreated;
};

#endif // RADOS_FS_TEST_HH
