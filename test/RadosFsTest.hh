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
#include <rados/librados.h>
#include "RadosFs.hh"
#include "RadosFsPriv.hh"
#include "RadosFsFile.hh"
#include "RadosFsFilePriv.hh"
#include "RadosFsDir.hh"

#ifndef RADOS_FS_TEST_HH
#define RADOS_FS_TEST_HH

#define CONF_ENV_VAR "RADOSFS_TEST_CLUSTER_CONF"

#define TEST_UID 1000
#define TEST_GID 1000

class RadosFsTest : public testing::Test
{

public:
  RadosFsTest(void);
  ~RadosFsTest(void);

protected:
  virtual void SetUp();
  virtual void TearDown();

  void AddPool();

  void testXAttrInFsInfo(radosfs::RadosFsInfo &info);

  void createNFiles(size_t numFiles);

  void removeNFiles(size_t numFiles);

  radosfs::RadosFs radosFs;

  radosfs::RadosFsPriv *radosFsPriv(void) const { return radosFs.mPriv; }
  radosfs::RadosFsFilePriv *radosFsFilePriv(radosfs::RadosFsFile &file);

private:
  rados_t mCluster;
  const char *mConf;
};

#endif // RADOS_FS_TEST_HH
