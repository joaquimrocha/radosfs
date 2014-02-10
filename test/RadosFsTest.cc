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
#include <stdexcept>

#include "RadosFsTest.hh"

#define TEST_POOL "test-pool"

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

  rados_shutdown(mCluster);

  radosFs.init("", mConf);
}

RadosFsTest::~RadosFsTest()
{
  rados_create(&mCluster, 0);

  rados_conf_read_file(mCluster, mConf);
  rados_connect(mCluster);

  rados_pool_delete(mCluster, TEST_POOL);

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
RadosFsTest::AddPool()
{
  int ret = radosFs.addPool(TEST_POOL, "/", 1000);

  EXPECT_EQ(0, ret);

  EXPECT_EQ(1, radosFs.pools().size());
}
