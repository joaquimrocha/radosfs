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
#include <errno.h>
#include <stdexcept>

#include "RadosFsTest.hh"

#define TEST_UID 1000
#define TEST_GID 1000

TEST_F(RadosFsTest, DefaultConstructor)
{
  EXPECT_TRUE(radosFs.uid() == 0);
  EXPECT_TRUE(radosFs.gid() == 0);
}

TEST_F(RadosFsTest, UidAndGid)
{
  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_TRUE(radosFs.uid() == TEST_UID);
  EXPECT_TRUE(radosFs.gid() == TEST_GID);
}

TEST_F(RadosFsTest, Pools)
{
  // Check if we have at least one pool in our Cluster (the one from the tests)
  std::vector<std::string> allPools = radosFs.allPoolsInCluster();

  EXPECT_GT(allPools.size(), 0);

  // Create a pool

  const std::string poolName("test-pool");
  const std::string poolPrefix("/");
  const int poolSize(10);

  EXPECT_EQ(0, radosFs.addPool(poolName, poolPrefix, poolSize));

  EXPECT_EQ(1, radosFs.pools().size());

  // Check pool's name from prefix

  EXPECT_EQ(poolName, radosFs.poolFromPrefix(poolPrefix));

  // Check pool's prefix from name

  EXPECT_EQ(poolPrefix, radosFs.poolPrefix(poolName));

  // Check pool's size (it's MB) from name

  EXPECT_EQ(poolSize * 1024 * 1024, radosFs.poolSize(poolName));

  // Remove the pool

  EXPECT_EQ(0, radosFs.removePool(poolName));

  // Verify there are no pools now

  EXPECT_EQ(0, radosFs.pools().size());
}

TEST_F(RadosFsTest, CreateDir)
{
  AddPool();

  // Create dir without existing parent

  radosfs::RadosFsDir subDir(&radosFs, "/testdir/testsubdir");

  EXPECT_NE(0, subDir.create());

  EXPECT_FALSE(subDir.exists());

  // Create dir from path without ending in /

  radosfs::RadosFsDir dir(&radosFs, "/testdir");

  std::string path(dir.path());

  EXPECT_EQ('/', path[path.length() - 1]);

  EXPECT_FALSE(dir.exists());

  EXPECT_EQ(0, dir.create());

  EXPECT_TRUE(dir.exists());

  EXPECT_TRUE(dir.isDir());

  EXPECT_FALSE(dir.isFile());

  // Create dir from path without ending in / and set with setPath

  dir.setPath("/test");

  path = dir.path();

  EXPECT_EQ('/', path[path.length() - 1]);

  EXPECT_EQ(0, subDir.create());

  EXPECT_TRUE(subDir.exists());

  // Check path when empty string is given

  dir = radosfs::RadosFsDir(&radosFs, "");

  EXPECT_EQ("/", dir.path());

  // Create dir when file with same name exists

  radosfs::RadosFsFile file(&radosFs, "/test", radosfs::RadosFsFile::MODE_WRITE);
  EXPECT_EQ(0, file.create());

  dir.setPath("/test");

  EXPECT_EQ(-ENOTDIR, dir.create());

  // Create dir with mkpath

  dir.setPath("/testdir/1/2/3/4/5");

  EXPECT_EQ(0, dir.create(-1, true));
}

TEST_F(RadosFsTest, RemoveDir)
{
  AddPool();

  radosfs::RadosFsDir dir(&radosFs, "/testdir");
  EXPECT_EQ(0, dir.create());

  radosfs::RadosFsDir subDir(&radosFs, "/testdir/testsubdir");
  EXPECT_EQ(0, subDir.create());

  // Remove non-empty dir

  EXPECT_NE(0, dir.remove());

  EXPECT_TRUE(dir.exists());

  // Remove empty dirs

  EXPECT_EQ(0, subDir.remove());

  EXPECT_FALSE(subDir.exists());

  EXPECT_EQ(0, dir.remove());

  EXPECT_FALSE(dir.exists());
}

TEST_F(RadosFsTest, DirParent)

{
  AddPool();

  radosfs::RadosFsDir dir(&radosFs, "/testdir");

  std::string parent = radosfs::RadosFsDir::getParent(dir.path());

  EXPECT_EQ("/", parent);

  parent = radosfs::RadosFsDir::getParent("");

  EXPECT_EQ("", parent);
}

TEST_F(RadosFsTest, CreateFile)
{
  AddPool();

  // Create regular file

  radosfs::RadosFsFile file(&radosFs, "/testfile",
                            radosfs::RadosFsFile::MODE_WRITE);

  EXPECT_FALSE(file.exists());

  EXPECT_EQ(0, file.create());

  EXPECT_TRUE(file.exists());

  EXPECT_FALSE(file.isDir());

  EXPECT_TRUE(file.isFile());

  // Create file when dir with same name exists

  radosfs::RadosFsDir dir(&radosFs, "/test");

  EXPECT_EQ(0, dir.create());

  file.setPath("/test");

  EXPECT_EQ(-EISDIR, file.create());

  // Create file when path is a dir one

  file.setPath("/test/");

  std::string path(file.path());

  EXPECT_NE('/', path[path.length() - 1]);

  radosfs::RadosFsFile otherFile(&radosFs, "/testfile/",
                                 radosfs::RadosFsFile::MODE_WRITE);

  path = otherFile.path();

  EXPECT_NE('/', path[path.length() - 1]);

  // Create file with root as path

  EXPECT_THROW(otherFile.setPath("/"), std::invalid_argument);
}

TEST_F(RadosFsTest, RemoveFile)
{
  AddPool();

  radosfs::RadosFsFile file(&radosFs, "/testfile",
                            radosfs::RadosFsFile::MODE_WRITE);

  EXPECT_NE(0, file.remove());

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, file.remove());

  EXPECT_FALSE(file.exists());
}

TEST_F(RadosFsTest, CreateFileInDir)
{
  AddPool();

  // Create file in nonexisting dir

  radosfs::RadosFsFile file(&radosFs, "/testdir/testfile",
                            radosfs::RadosFsFile::MODE_WRITE);

  EXPECT_NE(0, file.create());

  EXPECT_FALSE(file.exists());

  // Create file in existing dir

  radosfs::RadosFsDir dir(&radosFs, radosfs::RadosFsDir::getParent(file.path()).c_str());

  EXPECT_EQ(0, dir.create());

  EXPECT_NE(0, file.create());

  file.update();

  EXPECT_EQ(0, file.create());
}

TEST_F(RadosFsTest, DirPermissions)
{
  AddPool();

  // Create dir with owner

  radosfs::RadosFsDir dir(&radosFs, "/userdir");
  EXPECT_EQ(0, dir.create((S_IRWXU | S_IRGRP | S_IROTH), false, TEST_UID, TEST_GID));

  EXPECT_TRUE(dir.isWritable());

  radosFs.setIds(TEST_UID, TEST_GID);

  dir.update();

  EXPECT_TRUE(dir.isWritable());

  // Create dir by owner in a not writable path

  radosfs::RadosFsDir subDir(&radosFs, "/testdir");

  EXPECT_EQ(-EACCES, subDir.create());

  // Create dir by owner in a writable path

  subDir.setPath(dir.path() + "testdir");

  EXPECT_EQ(0, subDir.create());

  // Remove dir by a user who is not the owner

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  EXPECT_EQ(-EACCES, subDir.remove());

  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_EQ(-EACCES, dir.remove());

  radosFs.setIds(0, 0);

  // Remove dir by root

  EXPECT_EQ(0, subDir.remove());
}

TEST_F(RadosFsTest, FilePermissions)
{
  AddPool();

  // Create file by root

  radosfs::RadosFsDir dir(&radosFs, "/userdir");

  EXPECT_EQ(0, dir.create((S_IRWXU | S_IRGRP | S_IROTH), false, TEST_UID, TEST_GID));

  radosFs.setIds(TEST_UID, TEST_GID);

  // Create file by non-root in a not writable path

  radosfs::RadosFsFile file(&radosFs, "/userfile",
                            radosfs::RadosFsFile::MODE_WRITE);
  EXPECT_EQ(-EACCES, file.create());

  // Create file by non-root in a writable path

  file.setPath(dir.path() + "userfile");

  EXPECT_EQ(0, file.create());

  // Remove file by a different owner

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  EXPECT_EQ(-EACCES, file.remove());

  // Create file in another owner's folder

  radosfs::RadosFsFile otherFile(&radosFs, dir.path() + "otheruserfile",
                                 radosfs::RadosFsFile::MODE_WRITE);
  EXPECT_EQ(-EACCES, otherFile.create());

  // Remove file by owner

  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_EQ(0, file.remove());

  // Create file by owner and readable by others

  file = radosfs::RadosFsFile(&radosFs, dir.path() + "userfile",
                              radosfs::RadosFsFile::MODE_WRITE);
  EXPECT_EQ(0, file.create());

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  // Check if file is readable by non-owner

  otherFile = radosfs::RadosFsFile(&radosFs, file.path(),
                                   radosfs::RadosFsFile::MODE_READ);

  EXPECT_TRUE(otherFile.isReadable());

  // Remove file by owner

  radosFs.setIds(TEST_UID, TEST_GID);

  file.remove();

  // Create file by owner and not readable by others

  EXPECT_EQ(0, file.create((S_IRWXU | S_IRGRP)));

  // Check if file is readable by non-owner

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  otherFile.update();

  EXPECT_FALSE(otherFile.isReadable());
}

TEST_F(RadosFsTest, DirContents)
{
  AddPool();

  // Create dir and check entries

  radosfs::RadosFsDir dir(&radosFs, "/userdir");

  EXPECT_EQ(0, dir.create());

  std::set<std::string> entries;

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(0, entries.size());

  // Create file in dir and check entries

  radosfs::RadosFsFile file(&radosFs, dir.path() + "userfile",
                            radosfs::RadosFsFile::MODE_WRITE);

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(0, entries.size());

  dir.update();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(1, entries.size());

  // Try to create file with an existing path and check entries

  radosfs::RadosFsFile sameFile(file);

  EXPECT_EQ(0, sameFile.create());

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(1, entries.size());

  // Create a nonexisting file and check entries

  const std::string &otherFileName("userfile1");

  radosfs::RadosFsFile otherFile(&radosFs, dir.path() + otherFileName,
                                 radosfs::RadosFsFile::MODE_WRITE);

  EXPECT_EQ(0, otherFile.create());

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(2, entries.size());

  // Create a subdir and check entries

  const std::string &subDirName("subdir");

  radosfs::RadosFsDir subDir(&radosFs, dir.path() + subDirName);

  EXPECT_EQ(0, subDir.create());

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(3, entries.size());

  // Try to create a subdir with an existing path and check entries

  radosfs::RadosFsDir sameSubDir(subDir);

  EXPECT_EQ(0, sameSubDir.create(-1, true));

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(3, entries.size());

  // Remove file and check entries

  EXPECT_EQ(0, file.remove());

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(2, entries.size());

  // Check entries' names

  std::set<std::string>::const_iterator it = entries.begin();

  EXPECT_EQ(*it, subDirName + "/");

  it++;
  EXPECT_EQ(*it, otherFileName);
}

TEST_F(RadosFsTest, FileTruncate)
{
  AddPool();

  const std::string fileName("/test");
  const unsigned long long size = 100;

  radosfs::RadosFsFile file(&radosFs, fileName,
                            radosfs::RadosFsFile::MODE_WRITE);

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, file.truncate(size));

  radosfs::RadosFsFile sameFile(&radosFs, fileName,
                                radosfs::RadosFsFile::MODE_READ);

  struct stat buff;

  EXPECT_EQ(0, sameFile.stat(&buff));

  EXPECT_EQ(size, buff.st_size);
}

TEST_F(RadosFsTest, FileReadWrite)
{
  AddPool();

  // Write contents in file synchronously

  const std::string fileName("/test");
  const std::string contents("this is a test");

  radosfs::RadosFsFile file(&radosFs, fileName,
                            radosfs::RadosFsFile::MODE_READ_WRITE);

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(contents.length(),
            file.writeSync(contents.c_str(), 0, contents.length()));

  // Read and verify the contents

  char *buff = new char[contents.length() + 1];

  EXPECT_EQ(contents.length(), file.read(buff, 0, contents.length()));

  EXPECT_EQ(0, strcmp(buff, contents.c_str()));

  // Verify size with stat

  radosfs::RadosFsFile sameFile(&radosFs, fileName,
                                radosfs::RadosFsFile::MODE_READ);

  struct stat statBuff;

  EXPECT_EQ(0, sameFile.stat(&statBuff));

  EXPECT_EQ(contents.length(), statBuff.st_size);

  delete[] buff;

  // Write other contents in file asynchronously

  const std::string contents2("this is another test");

  buff = new char[contents2.length() + 1];

  EXPECT_EQ(contents2.length(),
            file.write(contents2.c_str(), 0, contents2.length()));

  // Read and verify the contents

  EXPECT_EQ(contents2.length(), file.read(buff, 0, contents2.length()));

  EXPECT_EQ(0, strcmp(buff, contents2.c_str()));

  delete[] buff;
}

GTEST_API_ int
main(int argc, char **argv)
{
  const std::string &confArgKey("--conf=");
  const size_t confArgKeyLength(confArgKey.length());
  bool confIsSet(getenv(CONF_ENV_VAR) != 0);

  if (!confIsSet)
  {
    for (int i = 0; i < argc; i++)
    {
      std::string arg(argv[i]);

      if (arg.compare(0, confArgKeyLength, confArgKey) == 0)
      {
        setenv(CONF_ENV_VAR,
               arg.substr(confArgKeyLength, std::string::npos).c_str(),
               1);

        confIsSet = true;

        break;
      }
    }
  }

  if (!confIsSet)
  {
    fprintf(stderr, "Error: Please specify the " CONF_ENV_VAR " environment "
            "variable or use the --conf=... argument.\n");

    return -1;
  }

  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
