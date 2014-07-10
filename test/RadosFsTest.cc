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

#include <fcntl.h>
#include <cstdio>
#include <errno.h>
#include <stdexcept>

#include "RadosFsTest.hh"
#include "radosfscommon.h"

RadosFsTest::RadosFsTest()
  : mConf(getenv(CONF_ENV_VAR))
{
  if (mConf == 0)
    throw std::invalid_argument("Please specify the " CONF_ENV_VAR
                                "environment variable or use the --conf=... "
                                "argument.");

  rados_create(&mCluster, 0);

  if (rados_conf_read_file(mCluster, mConf) != 0)
    throw std::invalid_argument("Problem reading configuration file.");

  rados_connect(mCluster);

  rados_pool_create(mCluster, TEST_POOL);
  rados_pool_create(mCluster, TEST_POOL_MTD);

  mPoolsCreated.insert(TEST_POOL);
  mPoolsCreated.insert(TEST_POOL_MTD);

  rados_shutdown(mCluster);

  radosFs.init("", mConf);
}

RadosFsTest::~RadosFsTest()
{
  rados_create(&mCluster, 0);

  rados_conf_read_file(mCluster, mConf);
  rados_connect(mCluster);

  std::set<std::string>::iterator it;

  for (it = mPoolsCreated.begin(); it != mPoolsCreated.end(); it++)
  {
    rados_pool_delete(mCluster, (*it).c_str());
  }

  rados_shutdown(mCluster);
}

void
RadosFsTest::SetUp()
{
}

void
RadosFsTest::TearDown()
{
}

void
RadosFsTest::AddPool(int numExtraPools)
{
  int ret = radosFs.addDataPool(TEST_POOL, "/", 1000);

  EXPECT_EQ(0, ret);

  EXPECT_EQ(1, radosFs.dataPools("/").size());

  ret = radosFs.addMetadataPool(TEST_POOL_MTD, "/");

  EXPECT_EQ(0, ret);

  for (int i = 0; i < numExtraPools; i++)
  {
    std::stringstream stream;

    stream << TEST_POOL << (i + 1);

    const std::string &poolName = stream.str();

    rados_pool_create(radosFsPriv()->radosCluster, poolName.c_str());

    ret = radosFs.addDataPool(poolName, "/", 1000);

    EXPECT_EQ(0, ret);

    mPoolsCreated.insert(poolName);
  }
}

radosfs::RadosFsFilePriv *
RadosFsTest::radosFsFilePriv(radosfs::RadosFsFile &file)
{
  return file.mPriv.get();
}

void
RadosFsTest::createNFiles(size_t numFiles)
{
  const std::string dirPath("/");

  for (size_t i = 0; i < numFiles; i++)
  {
    std::ostringstream s;
    s << i;
    radosfs::RadosFsFile file(&radosFs, dirPath + "file" + s.str(),
                              radosfs::RadosFsFile::MODE_WRITE);
    EXPECT_EQ(0, file.create());
  }
}

void
RadosFsTest::removeNFiles(size_t numFiles)
{
  const std::string dirPath("/");

  for (size_t i = 0; i < numFiles; i++)
  {
    std::ostringstream s;
    s << i;
    radosfs::RadosFsFile file(&radosFs, dirPath + "file" + s.str(),
                              radosfs::RadosFsFile::MODE_WRITE);
    EXPECT_EQ(0, file.remove());
  }
}

int
RadosFsTest::createContentsRecursively(const std::string &prefix,
                                       size_t numDirs,
                                       size_t numFiles,
                                       ssize_t levels)
{
  if (--levels < 0)
    return 0;

  for (size_t i = 0; i < numDirs; i++)
  {
    int ret;
    std::ostringstream s;
    s << i;
    radosfs::RadosFsDir dir(&radosFs, prefix + "d" + s.str());

    if ((ret = dir.create()) == 0)
    {
      ret = createContentsRecursively(dir.path(), numDirs, numFiles, levels);

      if (ret != 0)
        return ret;
    }
    else
      return ret;
  }

  for (size_t i = 0; i < numFiles; i++)
  {
    int ret;
    std::ostringstream s;
    s << i;
    radosfs::RadosFsFile file(&radosFs, prefix + "f" + s.str(),
                              radosfs::RadosFsFile::MODE_READ_WRITE);

    if ((ret = file.create()) != 0)
      return ret;
  }

  return 0;
}

void
RadosFsTest::testXAttrInFsInfo(radosfs::RadosFsInfo &info)
{
  // Get the permissions xattr by a unauthorized user

  radosFs.setIds(TEST_UID, TEST_GID);

  std::string xAttrValue;
  EXPECT_EQ(-EACCES, info.getXAttr(XATTR_PERMISSIONS, xAttrValue,
                                  XATTR_PERMISSIONS_LENGTH));

  // Get an invalid xattr

  EXPECT_EQ(-EINVAL, info.getXAttr("invalid", xAttrValue,
                                  XATTR_PERMISSIONS_LENGTH));

  // Get an inexistent xattr

  EXPECT_LT(info.getXAttr("usr.inexistent", xAttrValue,
                         XATTR_PERMISSIONS_LENGTH), 0);

  // Set a user attribute

  const std::string attr("usr.attr");
  const std::string value("value");
  EXPECT_EQ(0, info.setXAttr(attr, value));

  // Get the attribute set above

  EXPECT_EQ(value.length(), info.getXAttr(attr, xAttrValue, value.length()));

  // Check the attribtue's value

  EXPECT_EQ(value, xAttrValue);

  // Change to another user

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  // Set an xattr by an unauthorized user

  EXPECT_EQ(-EACCES, info.setXAttr(attr, value));

  // Get an xattr by a user who can only read

  EXPECT_EQ(value.length(), info.getXAttr(attr, xAttrValue, value.length()));

  // Check the attribute's value

  EXPECT_EQ(value, xAttrValue);

  // Remove an xattr by an unauthorized user

  EXPECT_EQ(-EACCES, info.removeXAttr(attr));

  // Get the xattrs map

  std::map<std::string, std::string> map;

  EXPECT_EQ(0, info.getXAttrsMap(map));

  // Check the xattrs map's size

  EXPECT_EQ(1, map.size());

  // Switch to the root user

  radosFs.setIds(ROOT_UID, ROOT_UID);

  map.clear();

  // Set an xattr -- when being root -- in a different user's file

  EXPECT_EQ(0, info.setXAttr("sys.attribute", "check"));

  // Get the xattrs map

  EXPECT_EQ(0, info.getXAttrsMap(map));

  // Check the xattrs map's size

  EXPECT_EQ(3, map.size());

  // Check the xattrs map's value

  EXPECT_EQ(map[attr], value);

  // Check that a sys xattr is present

  EXPECT_EQ(1, map.count(XATTR_PERMISSIONS));
}
