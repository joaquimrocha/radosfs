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

#include <boost/thread.hpp>
#include <algorithm>
#include <gtest/gtest.h>
#include <errno.h>
#include <cmath>
#include <sstream>
#include <stdexcept>
#include <time.h>

#include "FileIO.hh"
#include "FileInode.hh"
#include "RadosFsTest.hh"
#include "radosfscommon.h"

#define NSEC_TO_SEC(n) ((double)(n) / 1000000000.0)

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

  radosfs::File file(&radosFs,
                     "/file",
                     radosfs::File::MODE_READ_WRITE);

  EXPECT_EQ(-ENODEV, file.create());

  radosfs::Dir dir(&radosFs,
                   "/dir");

  EXPECT_EQ(-ENODEV, dir.create());

  // Create a data and a metadata pool

  const std::string dataPoolName(TEST_POOL);
  const std::string mtdPoolName(TEST_POOL_MTD);
  std::string poolPrefix("/");
  const int poolSize(10);

  EXPECT_EQ(0, radosFs.addDataPool(dataPoolName, poolPrefix, poolSize));

  EXPECT_EQ(0, radosFs.addMetadataPool(mtdPoolName, poolPrefix));

  EXPECT_EQ(-EEXIST, radosFs.addDataPool(dataPoolName, poolPrefix, 0));

  EXPECT_EQ(-EEXIST, radosFs.addMetadataPool(mtdPoolName, poolPrefix));

  EXPECT_EQ(1, radosFs.dataPools(poolPrefix).size());

  EXPECT_EQ(1, radosFs.metadataPools().size());

  // Check the pools' names from prefix

  std::vector<std::string> dataPools = radosFs.dataPools(poolPrefix);

  EXPECT_NE(dataPools.end(),
            find(dataPools.begin(), dataPools.end(), dataPoolName));

  EXPECT_EQ(mtdPoolName, radosFs.metadataPoolFromPrefix(poolPrefix));

  // Check the pools' prefix from name

  EXPECT_EQ(poolPrefix, radosFs.dataPoolPrefix(dataPoolName));

  EXPECT_EQ(poolPrefix, radosFs.metadataPoolPrefix(mtdPoolName));

  // Check pool's size (it's MB) from name

  EXPECT_EQ(poolSize * 1024 * 1024, radosFs.dataPoolSize(dataPoolName));

  // Create a dir and check if it got into the data pool

  Stat stat;
  PoolSP dataPool, mtdPool;

  mtdPool = radosFsPriv()->getMetadataPoolFromPath(dir.path());

  EXPECT_EQ(0, dir.create());

  EXPECT_EQ(0, radosFsPriv()->stat(dir.path(), &stat));

  // Create a file and check if it got into the data pool

  file.setPath(dir.path() + "file");

  dataPool = radosFsPriv()->getDataPool(file.path());

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, radosFsPriv()->stat(file.path(), &stat));

  EXPECT_EQ(dataPool->name, stat.pool->name);

  // Remove the pools

  EXPECT_EQ(0, radosFs.removeDataPool(dataPoolName));

  EXPECT_EQ(0, radosFs.removeMetadataPool(mtdPoolName));

  // Verify there are no pools now

  EXPECT_EQ(0, radosFs.dataPools(poolPrefix).size());

  EXPECT_EQ(0, radosFs.metadataPools().size());

  // Create a pool for a non root prefix
  poolPrefix = "/test";

  EXPECT_EQ(0, radosFs.addDataPool(dataPoolName, poolPrefix, poolSize));

  EXPECT_EQ(0, radosFs.addMetadataPool(mtdPoolName, poolPrefix));

  // Verify that one cannot create a dir in a path that doesn't start with
  // the pool's prefix

  dir.setPath("/new-dir");

  EXPECT_EQ(-ENODEV, dir.create(-1, true));

  // Verify that the pool's prefix dir exists

  dir.setPath(poolPrefix);

  EXPECT_TRUE(dir.exists());

  // Create a dir inside the pool's prefix dir

  dir.setPath(poolPrefix + "/dir");

  EXPECT_EQ(0, dir.create());

  // Set more than one data pool to the same prefix

  EXPECT_EQ(0, radosFs.addDataPool(mtdPoolName, poolPrefix, poolSize));

  EXPECT_EQ(2, radosFs.dataPools(poolPrefix).size());
}

TEST_F(RadosFsTest, CharacterConsistency)
{
  AddPool();

  // Create dir with a sequence of / in the path

  std::string path = "no-slash";

  radosfs::Dir otherDir(&radosFs, path);

  EXPECT_EQ("/" + path + "/", otherDir.path());

  otherDir.setPath("//d1//d2////////");

  EXPECT_EQ("/d1/d2/", otherDir.path());

  // Create dir with diacritics, whitespace and other different
  // characters in the path

  path = "\n acções \n  über \n\n   %%   #  caractères \n \"extraños\" \n%";

  otherDir.setPath(path);

  EXPECT_EQ(0, otherDir.create());

  EXPECT_EQ('/' + path + '/', otherDir.path());

  radosfs::Dir rootDir(&radosFs, "/");
  rootDir.update();

  std::set<std::string> entries;
  rootDir.entryList(entries);

  EXPECT_NE(entries.end(), entries.find(path + '/'));
}

TEST_F(RadosFsTest, PathsLength)
{
  AddPool();

  // Create a path with the maximum length allowed

  size_t length = MAXIMUM_PATH_LENGTH;
  std::string longString(length, 'x');
  longString[0] = PATH_SEP;

  // Create a file with that path

  radosfs::File file(&radosFs, longString);

  EXPECT_EQ(0, file.create());

  // Increase the path's length (1 char over the maximum allowed)

  longString += "x";

  // Set the file path with the one previously defined and verify that it is
  // not allowed and it reverted to the root path

  EXPECT_THROW(file.setPath(longString), std::invalid_argument);

  EXPECT_EQ("/", file.path());

  EXPECT_EQ(-EISDIR, file.create());

  // Set the file path again to the long path and verify it exists

  longString.resize(length);

  EXPECT_NO_THROW(file.setPath(longString));

  EXPECT_EQ(true, file.exists());

  // Get the entries in the root directory

  radosfs::Dir dir(&radosFs, "/");
  dir.update();

  std::set<std::string> entries;

  dir.entryList(entries);

  // Remove the heading '/'
  longString.erase(0, 1);

  // Verify that the long file name was indexed (is one of the entries)

  std::set<std::string>::iterator fileIt = entries.find(longString);

  EXPECT_NE(entries.end(), fileIt);

  // Remove the long path file, set the long path to a directory and verify it
  // is not allowed (because the directory always appends one / at the end,
  // making it go over the maximum length allowed)

  EXPECT_EQ(0, file.remove());

  radosfs::Dir otherDir(&radosFs, "");

  EXPECT_THROW(otherDir.setPath(longString), std::invalid_argument);

  EXPECT_EQ("/", otherDir.path());

  // Remove two chars of the long path so when it is set in the directory, it
  // will be added one / at the beginning and another at the end

  longString.resize(MAXIMUM_PATH_LENGTH - 2);

  // Set the long path to the directory and create it

  EXPECT_NO_THROW(otherDir.setPath(longString));

  EXPECT_EQ(0, otherDir.create());

  // Create a short path file

  file.setPath("/f");

  EXPECT_EQ(0, file.create());

  // Create a link for the short path file inside the long path directory and
  // verify it is not allowed

  EXPECT_EQ(-ENAMETOOLONG, file.createLink(otherDir.path() + "file-link"));
}

TEST_F(RadosFsTest, CreateDir)
{
  AddPool();

  // Create dir without existing parent

  radosfs::Dir subDir(&radosFs, "/testdir/testsubdir");

  EXPECT_NE(0, subDir.create());

  EXPECT_FALSE(subDir.exists());

  // Create dir from path without ending in /

  radosfs::Dir dir(&radosFs, "/testdir");

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

  dir = radosfs::Dir(&radosFs, "");

  EXPECT_EQ("/", dir.path());

  // Create dir when file with same name exists

  radosfs::File file(&radosFs, "/test", radosfs::File::MODE_WRITE);
  EXPECT_EQ(0, file.create());

  dir.setPath("/test");

  EXPECT_EQ(-ENOTDIR, dir.create());

  // Create dir with mkpath

  dir.setPath("/testdir/1/2/3/4/5");

  EXPECT_EQ(0, dir.create(-1, true));

  dir.setPath(file.path() + "/d1");

  EXPECT_EQ(-ENOTDIR, dir.create(-1, true));

  // Create dir with the mkdir option when the parent directory is root
  dir.setPath("/my-dir");

  EXPECT_EQ(0, dir.create(-1, true));
}

TEST_F(RadosFsTest, RemoveDir)
{
  AddPool();

  radosfs::Dir dir(&radosFs, "/testdir");
  EXPECT_EQ(0, dir.create());

  radosfs::Dir subDir(&radosFs, "/testdir/testsubdir");
  EXPECT_EQ(0, subDir.create());

  // Remove non-empty dir

  EXPECT_EQ(-ENOTEMPTY, dir.remove());

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

  radosfs::Dir dir(&radosFs, "/testdir");

  std::string parent = radosfs::Dir::getParent(dir.path());

  EXPECT_EQ("/", parent);

  parent = radosfs::Dir::getParent("");

  EXPECT_EQ("", parent);
}

TEST_F(RadosFsTest, CreateFile)
{
  // Create one extra pool apart from the ones created by default

  AddPool(1);

  // Create regular file

  radosfs::File file(&radosFs, "/testfile",
                     radosfs::File::MODE_WRITE);

  EXPECT_FALSE(file.exists());

  EXPECT_EQ(0, file.create());

  EXPECT_TRUE(file.exists());

  EXPECT_FALSE(file.isDir());

  EXPECT_TRUE(file.isFile());

  // Create file when dir with same name exists

  radosfs::Dir dir(&radosFs, "/test");

  EXPECT_EQ(0, dir.create());

  file.setPath("/test");

  EXPECT_EQ(-EISDIR, file.create());

  // Create file when path is a dir one

  file.setPath("/test/");

  std::string path(file.path());

  EXPECT_NE('/', path[path.length() - 1]);

  radosfs::File otherFile(&radosFs, "/testfile/",
                          radosfs::File::MODE_WRITE);

  path = otherFile.path();

  EXPECT_NE('/', path[path.length() - 1]);

  // Check the shared pointer use

  radosfs::FilePriv *filePriv = radosFsFilePriv(otherFile);

  EXPECT_TRUE(radosfs::FileIO::hasSingleClient(fileInodePriv(*filePriv->inode)->io));

  file.setPath(otherFile.path());

  EXPECT_FALSE(radosfs::FileIO::hasSingleClient(filePriv->getFileIO()));

  otherFile.setPath("/file-in-different-pool");

  const std::string &poolName = TEST_POOL "1";

  EXPECT_EQ(0, otherFile.create(-1, poolName));

  Stat stat;

  EXPECT_EQ(0, radosFsPriv()->stat(otherFile.path(), &stat));

  EXPECT_EQ(poolName, stat.pool->name);

  file.setPath(otherFile.path());

  EXPECT_EQ(poolName, radosFsFilePriv(file)->dataPool->name);

  // Instance one file when it doesn't exist and create it when it has been
  // already created from a different instance

  radosfs::File newFile(&radosFs, "/file");
  radosfs::File sameFile(&radosFs, newFile.path());

  EXPECT_EQ(0, newFile.create());

  EXPECT_EQ(-EEXIST, sameFile.create());

  // Check creating a file with a custom stripe size

  newFile.setPath("/file-with-custom-stripe-size");

  const size_t stripeSize(radosFs.fileStripeSize() / 2);

  ASSERT_EQ(0, newFile.create(-1, "", stripeSize));

  sameFile.setPath(newFile.path());

  ASSERT_EQ(stripeSize, radosFsFilePriv(sameFile)->getFileIO()->stripeSize());
}

TEST_F(RadosFsTest, RemoveFile)
{
  AddPool();

  radosfs::File file(&radosFs, "/testfile",
                     radosfs::File::MODE_WRITE);

  EXPECT_NE(0, file.remove());

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, file.remove());

  EXPECT_FALSE(file.exists());

  radosfs::File *file1, *file2;

  file1 = new radosfs::File(&radosFs, "/testfile1",
                            radosfs::File::MODE_WRITE);

  file2 = new radosfs::File(&radosFs, file1->path(),
                            radosfs::File::MODE_WRITE);

  EXPECT_EQ(0, file1->create());

  const size_t contentsLength = DEFAULT_FILE_INLINE_BUFFER_SIZE - 1;
  char *inlineContents = new char[contentsLength];

  memset(inlineContents, 'x', contentsLength);

  file1->writeSync(inlineContents, 0, contentsLength);

  file2->update();

  EXPECT_TRUE(file2->exists());

  EXPECT_EQ(0, file1->remove());

  char *inlineContents2 = new char[contentsLength];

  file2->read(inlineContents2, 0, contentsLength);

  EXPECT_TRUE(strncmp(inlineContents, inlineContents2, contentsLength));

  file2->update();

  EXPECT_FALSE(file2->exists());

  delete file2;
  delete file1;
  delete inlineContents;
  delete inlineContents2;

  file.setPath("/testfile1");

  EXPECT_FALSE(file.exists());

  // Make the files' stripe size small so many stripes will be generated

  const size_t stripeSize = 128;
  radosFs.setFileStripeSize(stripeSize);

  // Create a file with several stripes

  EXPECT_EQ(0, file.create());

  std::string contents;

  for (size_t i = 0; i < stripeSize * 3; i++)
  {
    contents += "test";
  }

  EXPECT_EQ(0, file.writeSync(contents.c_str(), 0, contents.length()));

  // Remove the file, create it again and check if the size if 0
  // (which means that no other stripes should exist)

  EXPECT_EQ(0, file.remove());

  EXPECT_EQ(0, file.create());

  struct stat buff;
  buff.st_size = 1;

  EXPECT_EQ(0, file.stat(&buff));

  EXPECT_EQ(0, buff.st_size);
}

TEST_F(RadosFsTest, CreateFileInDir)
{
  AddPool();

  // Create file in nonexisting dir

  radosfs::File file(&radosFs, "/testdir/testfile",
                     radosfs::File::MODE_WRITE);

  EXPECT_NE(0, file.create());

  EXPECT_FALSE(file.exists());

  // Create file in existing dir

  radosfs::Dir dir(&radosFs, radosfs::Dir::getParent(file.path()).c_str());

  EXPECT_EQ(0, dir.create());

  EXPECT_NE(0, file.create());

  file.update();

  EXPECT_EQ(0, file.create());
}

TEST_F(RadosFsTest, StatFile)
{
  AddPool();

  size_t inlineBufferSize = 16;

  // Create a file with a predefined inline buffer size

  radosfs::File file(&radosFs, "/file");

  ASSERT_EQ(0, file.create(-1, "", 0, inlineBufferSize));

  // Stat the empty file and verify its size

  struct stat stat1;

  EXPECT_EQ(0, radosFs.stat(file.path(), &stat1));

  EXPECT_EQ(0, stat1.st_size);

  // Write inline contents to the file

  std::string contents("x");

  EXPECT_EQ(0, file.writeSync(contents.c_str(), 0, contents.length()));

  // Stat the file from the file instance and from the filesystem instance, and
  // verify that they match

  EXPECT_EQ(0, file.stat(&stat1));

  struct stat stat2;

  EXPECT_EQ(0, radosFs.stat(file.path(), &stat2));

  EXPECT_EQ(stat1.st_size, stat2.st_size);

  EXPECT_EQ(contents.length(), stat2.st_size);

  // Write contents beyong the inline buffer and stat again

  contents.assign(inlineBufferSize + 1, 'y');

  EXPECT_EQ(0, file.writeSync(contents.c_str(), 0, contents.length()));

  EXPECT_EQ(0, radosFs.stat(file.path(), &stat1));

  EXPECT_EQ(contents.length(), stat1.st_size);

  // Create a new file and write to its inline buffer

  radosfs::File file1(&radosFs, "/file1");

  ASSERT_EQ(0, file1.create(-1, "", 0, inlineBufferSize));

  EXPECT_EQ(0, file1.writeSync(contents.c_str(), 0, contents.length() / 2));

  // Stat three paths in parallel and verify the stat operations' return codes
  // and sizes

  std::vector<std::pair<int, struct stat> > statResult;
  std::vector<std::string> paths;

  paths.push_back(file.path());
  paths.push_back(file1.path());
  paths.push_back("/non-existing");

  statResult = radosFs.stat(paths);

  EXPECT_EQ(paths.size(), statResult.size());

  const int retCodes[] = {0, 0, -ENOENT};
  const size_t sizes[] = {contents.length(), contents.length() / 2, 0};

  std::vector<std::pair<int, struct stat> >::iterator it;
  int i;
  for (it = statResult.begin(), i = 0; it != statResult.end(); it++, i++)
  {
    int retCode = (*it).first;
    struct stat fileStat = (*it).second;

    EXPECT_EQ(retCodes[i], retCode);

    if (retCode == 0)
      EXPECT_EQ(sizes[i], fileStat.st_size);
  }
}

TEST_F(RadosFsTest, DirPermissions)
{
  AddPool();

  // Create dir with owner

  radosfs::Dir dir(&radosFs, "/userdir");
  EXPECT_EQ(0, dir.create((S_IRWXU | S_IRGRP | S_IROTH), false, TEST_UID, TEST_GID));

  EXPECT_TRUE(dir.isWritable());

  radosFs.setIds(TEST_UID, TEST_GID);

  dir.update();

  EXPECT_TRUE(dir.isWritable());

  // Create dir by owner in a not writable path

  radosfs::Dir subDir(&radosFs, "/testdir");

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

  // Change permissions using chmod and check them

  radosFs.setIds(TEST_UID, TEST_GID);

  subDir.update();

  EXPECT_EQ(0, subDir.create(S_IRWXU));

  EXPECT_EQ(0, subDir.chmod(S_IRWXU | S_IROTH));

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  subDir.update();

  EXPECT_TRUE(subDir.isReadable());

  EXPECT_EQ(-EPERM, subDir.chmod(777));

  radosFs.setIds(TEST_UID, TEST_GID);

  subDir.update();

  EXPECT_EQ(0, subDir.chmod(0));

  subDir.update();

  EXPECT_FALSE(subDir.isReadable());

  radosFs.setIds(ROOT_UID, ROOT_UID);

  subDir.update();

  EXPECT_TRUE(subDir.isWritable());

  EXPECT_EQ(0, subDir.chmod(S_IWRITE));

  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_EQ(0, subDir.chmod(S_IREAD));

  subDir.update();

  EXPECT_TRUE(subDir.isReadable());
}

TEST_F(RadosFsTest, FilePermissions)
{
  AddPool();

  // Create file by root

  radosfs::Dir dir(&radosFs, "/userdir");

  EXPECT_EQ(0, dir.create((S_IRWXU | S_IRGRP | S_IROTH), false, TEST_UID, TEST_GID));

  radosFs.setIds(TEST_UID, TEST_GID);

  // Create file by non-root in a not writable path

  radosfs::File file(&radosFs, "/userfile",
                     radosfs::File::MODE_WRITE);
  EXPECT_EQ(-EACCES, file.create());

  // Create file by non-root in a writable path

  file.setPath(dir.path() + "userfile");

  EXPECT_EQ(0, file.create());

  // Remove file by a different owner

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  EXPECT_EQ(-EACCES, file.remove());

  // Create file in another owner's folder

  radosfs::File otherFile(&radosFs, dir.path() + "otheruserfile",
                          radosfs::File::MODE_WRITE);
  EXPECT_EQ(-EACCES, otherFile.create());

  // Remove file by owner

  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_EQ(0, file.remove());

  // Create file by owner and readable by others

  file = radosfs::File(&radosFs, dir.path() + "userfile");
  EXPECT_EQ(0, file.create());

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  // Check if file is readable by non-owner

  otherFile = radosfs::File(&radosFs, file.path(),
                            radosfs::File::MODE_READ);

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

  // Change permissions using chmod and check them

  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_EQ(0, file.chmod(S_IRWXU | S_IROTH));

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  file.update();

  EXPECT_TRUE(file.isReadable());

  EXPECT_EQ(-EPERM, file.chmod(777));

  radosFs.setIds(TEST_UID, TEST_GID);

  file.update();

  EXPECT_EQ(0, file.chmod(0));

  file.update();

  EXPECT_FALSE(file.isReadable());

  EXPECT_EQ(-EACCES, file.truncate(100));

  radosFs.setIds(ROOT_UID, ROOT_UID);

  file.update();

  EXPECT_TRUE(file.isWritable());

  EXPECT_EQ(0, file.truncate(100));

  EXPECT_EQ(0, file.chmod(S_IWRITE));

  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_EQ(0, file.chmod(S_IREAD));

  file.update();

  EXPECT_TRUE(file.isReadable());
}

TEST_F(RadosFsTest, DirContents)
{
  AddPool();

  // Create dir and check entries

  radosfs::Dir dir(&radosFs, "/userdir");

  EXPECT_EQ(0, dir.create());

  std::set<std::string> entries;

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(0, entries.size());

  // Create file in dir and check entries

  radosfs::File file(&radosFs, dir.path() + "userfile",
                     radosfs::File::MODE_WRITE);

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(0, entries.size());

  dir.update();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(1, entries.size());

  // Try to create file with an existing path and check entries

  radosfs::File sameFile(file);

  EXPECT_EQ(-EEXIST, sameFile.create());

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(1, entries.size());

  // Create a nonexisting file and check entries

  const std::string &otherFileName("userfile1");

  radosfs::File otherFile(&radosFs, dir.path() + otherFileName,
                          radosfs::File::MODE_WRITE);

  EXPECT_EQ(0, otherFile.create());

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(2, entries.size());

  // Create a subdir and check entries

  const std::string &subDirName("subdir");

  radosfs::Dir subDir(&radosFs, dir.path() + subDirName);

  EXPECT_EQ(0, subDir.create());

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(3, entries.size());

  // Try to create a subdir with an existing path and check entries

  radosfs::Dir sameSubDir(subDir);

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

  // Create file and write to it

  file = radosfs::File(&radosFs, "/my-file",
                       radosfs::File::MODE_READ_WRITE);

  EXPECT_EQ(0, file.create());

  const std::string contents = "my file contents";

  EXPECT_EQ(0, file.write(contents.c_str(), 0, contents.length()));

  // Verify it was correctly written

  char buff[contents.length() + 1];

  EXPECT_EQ(contents.length(), file.read(buff, 0, contents.length()));

  buff[contents.length()] = '\0';

  EXPECT_EQ(0, strcmp(contents.c_str(), buff));

  // Set the file path to a dir and list it

  dir.setPath(file.path());

  entries.clear();

  EXPECT_EQ(-ENOTDIR, dir.entryList(entries));

  EXPECT_EQ(0, entries.size());

  std::string entry;

  EXPECT_EQ(-ENOTDIR, dir.entry(0, entry));

  // Verify that the file contents were not touched

  EXPECT_EQ(contents.length(), file.read(buff, 0, contents.length()));

  EXPECT_EQ(0, strcmp(contents.c_str(), buff));
}

TEST_F(RadosFsTest, FileInode)
{
  AddPool();

  Stat stat;
  const std::string fileName("/test");

  radosfs::File file(&radosFs, fileName);

  EXPECT_EQ(0, file.create(-1, "", 0, 0));

  EXPECT_EQ(0, radosFsPriv()->stat(file.path(), &stat));

  EXPECT_EQ(-ENOENT, stat.pool->ioctx.stat(stat.translatedPath, 0, 0));

  EXPECT_EQ(0, file.truncate(1));

  EXPECT_EQ(0, stat.pool->ioctx.stat(stat.translatedPath, 0, 0));

  testFileInodeBackLink(file.path());

  EXPECT_EQ(0, stat.pool->ioctx.remove(stat.translatedPath));

  // Override the hasBackLink var in the FileIO instance because it is not aware
  // that the inode object has been removed

  radosfs::FilePriv *filePriv = radosFsFilePriv(file);

  fileInodePriv(*filePriv->inode)->io->setHasBackLink(false);

  EXPECT_EQ(0, file.write("X", 0, 1));

  file.sync();

  testFileInodeBackLink(file.path());

  EXPECT_EQ(0, stat.pool->ioctx.stat(stat.translatedPath, 0, 0));

  std::string inode, pool;

  ASSERT_EQ(0, radosFs.getInodeAndPool(file.path(), &inode, &pool));

  EXPECT_EQ(stat.translatedPath, inode);

  EXPECT_EQ(stat.pool->name, pool);
}

TEST_F(RadosFsTest, FileInodeDirect)
{
  AddPool();

  // Create an inode with a given name

  std::string inodeName(generateUuid());

  radosfs::FileInode inode(&radosFs, inodeName, TEST_POOL);

  EXPECT_EQ(inode.name(), inodeName);

  // Create an inode with an automatic generated name

  radosfs::FileInode otherInode(&radosFs, TEST_POOL);

  EXPECT_NE(inode.name(), otherInode.name());

  size_t contentsSize = 1024;

  char buff[contentsSize];

  // Read an inode that does not exist (doesn't have any stripes)

  EXPECT_EQ(-ENOENT, inode.read(buff, 0, contentsSize));

  // Write synchronously into an inode

  char contents[contentsSize];
  memset(contents, 'x', contentsSize);
  contents[contentsSize - 1] = '\0';

  ASSERT_EQ(0, inode.writeSync(contents, 0, contentsSize));

  // Read its contents

  ASSERT_GT(inode.read(buff, 0, contentsSize), 0);

  EXPECT_EQ(0, strcmp(contents, buff));

  memset(contents, 'y', contentsSize / 2);

  // Write asynchronously into an inode

  ASSERT_EQ(0, inode.write(contents, 0, contentsSize));

  inode.sync();

  // Read its contents

  ASSERT_GT(inode.read(buff, 0, contentsSize), 0);

  EXPECT_EQ(0, strcmp(contents, buff));

  // Truncate the inode to half and read it again

  ASSERT_EQ(0, inode.truncate(contentsSize / 2));

  EXPECT_EQ(contentsSize / 2, inode.read(buff, 0, contentsSize));

  EXPECT_EQ(contentsSize / 3, inode.read(buff, 0, contentsSize / 3));

  // Check the backlink set on the inode

  std::string backLink;

  EXPECT_EQ(-ENODATA, inode.getBackLink(&backLink));

  radosfs::File file(&radosFs, "/file");

  EXPECT_FALSE(file.exists());

  // Create a file

  radosfs::File file1(&radosFs, "/file1");

  ASSERT_EQ(0, file1.create());

  // Register the inode with an invalid file path

  ASSERT_EQ(-EISDIR, inode.registerFile("/", TEST_UID, TEST_GID, O_RDWR));

  ASSERT_EQ(-EINVAL, inode.registerFile("", TEST_UID, TEST_GID, O_RDWR));

  ASSERT_EQ(-EINVAL, inode.registerFile("no-slash-file", TEST_UID, TEST_GID,
                                        O_RDWR));

  ASSERT_EQ(-ENOENT, inode.registerFile("/nonexitent/file", TEST_UID, TEST_GID,
                                        O_RDWR));

  ASSERT_EQ(-EINVAL, inode.registerFile("/file1/file", TEST_UID, TEST_GID,
                                        O_RDWR));

  // Register the inode with an existing file path

  ASSERT_EQ(-EEXIST, inode.registerFile(file1.path(), TEST_UID, TEST_GID,
                                        O_RDWR));

  radosfs::Dir dir(&radosFs, "/");

  ASSERT_EQ(0, dir.createLink("/dir-link/"));

  // Register the inode with a link path

  ASSERT_EQ(-EINVAL, inode.registerFile("/dir-link/file", TEST_UID, TEST_GID,
                                        O_RDWR));

  // Register the inode with a new file path

  ASSERT_EQ(0, inode.registerFile(file.path(), TEST_UID, TEST_GID, O_RDWR));

  file.update();

  // Verify the registered file exists

  EXPECT_TRUE(file.exists());

  // Check the backlink set on the inode

  EXPECT_EQ(0, inode.getBackLink(&backLink));

  EXPECT_EQ(file.path(), backLink);

  // Read from the registered file

  memset(buff, 0, contentsSize / 2);

  ASSERT_EQ(contentsSize / 2, file.read(buff, 0, contentsSize / 2));

  EXPECT_EQ(0, strcmp(contents, buff));

  // Stat from the registered file and check it

  Stat fileStat;

  ASSERT_EQ(0, radosFsPriv()->stat(file.path(), &fileStat));

  EXPECT_EQ(TEST_UID, fileStat.statBuff.st_uid);

  EXPECT_EQ(TEST_GID, fileStat.statBuff.st_gid);

  EXPECT_TRUE((fileStat.statBuff.st_mode & O_RDWR) != 0);

  EXPECT_EQ(inode.name(), fileStat.translatedPath);

  // Remove the inode and try to read it

  ASSERT_EQ(0, inode.remove());

  EXPECT_EQ(-ENOENT, inode.read(buff, 0, 1));
}

TEST_F(RadosFsTest, FileTruncate)
{
  AddPool();

  // Make the files' stripe size small so many stripes will be generated

  const size_t stripeSize = 128;
  radosFs.setFileStripeSize(stripeSize);

  const std::string fileName("/test");
  char contents[stripeSize * 10];
  memset(contents, 'x', stripeSize * 10);
  unsigned long long size = 1024;

  // Create a file and truncate it to the content's size

  radosfs::File file(&radosFs, fileName,
                     radosfs::File::MODE_WRITE);

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, file.write(contents, 0, stripeSize * 10));

  EXPECT_EQ(0, file.truncate(size));

  // Setting a fake path to file so its cache is dumped and we have a "fresh"
  // new instance below with sameFile which is necessary to check if the
  // file stripe size persistency is working

  file.setPath("/fake");

  // Create a new instance of the same file and check the size

  radosfs::File sameFile(&radosFs, fileName,
                         radosfs::File::MODE_READ);

  struct stat buff;

  // Setting a different stripe size before checking sameFile's size because
  // it should have been set in the file when it was written

  radosFs.setFileStripeSize(stripeSize + 1);

  EXPECT_EQ(0, sameFile.stat(&buff));

  EXPECT_EQ(size, buff.st_size);

  // Get the right file instance back again

  file = sameFile;

  // Truncate the file to 0 and verify

  EXPECT_EQ(0, file.truncate(0));

  sameFile.update();

  EXPECT_EQ(0, sameFile.stat(&buff));

  EXPECT_EQ(0, buff.st_size);

  // Truncate the file to a non-multiple of the stripe size and verify

  size = stripeSize * 5.3;

  EXPECT_EQ(0, file.truncate(size));

  sameFile.update();

  EXPECT_EQ(0, sameFile.stat(&buff));

  EXPECT_EQ(size, buff.st_size);

  // Truncate the file to a half of the stripe size and verify

  size = stripeSize / 2;

  EXPECT_EQ(0, file.truncate(size));

  sameFile.update();

  EXPECT_EQ(0, sameFile.stat(&buff));

  EXPECT_EQ(size, buff.st_size);
}

TEST_F(RadosFsTest, FileReadWrite)
{
  AddPool();

  // Set a small file stripe size so many stripes will be created

  const size_t stripeSize = 128;
  radosFs.setFileStripeSize(stripeSize);

  // Write contents in file synchronously

  const std::string fileName("/test");
  const std::string contents("this is a test");

  radosfs::File file(&radosFs, fileName,
                     radosfs::File::MODE_READ_WRITE);

  EXPECT_EQ(0, file.create());

  // Read from an empty file

  char *buff = new char[contents.length() + 1];

  EXPECT_EQ(0, file.read(buff, 0, contents.length()));

  delete[] buff;

  EXPECT_EQ(0, file.writeSync(contents.c_str(), 0, contents.length()));

  // Read and verify the contents

  buff = new char[contents.length() + 1];

  EXPECT_EQ(contents.length(), file.read(buff, 0, contents.length()));
  buff[contents.length()] = '\0';

  EXPECT_EQ(0, strcmp(buff, contents.c_str()));

  // Verify size with stat

  radosfs::File sameFile(&radosFs, fileName,
                         radosfs::File::MODE_READ);

  struct stat statBuff;

  EXPECT_EQ(0, sameFile.stat(&statBuff));

  EXPECT_EQ(contents.length(), statBuff.st_size);

  delete[] buff;

  // Write other contents in file asynchronously

  std::string contents2("this is another test ");

  for (size_t i = 0; i < stripeSize; i++)
    contents2 += "this is another test ";

  buff = new char[contents2.length() + 1];

  EXPECT_EQ(0, file.write(contents2.c_str(), 0, contents2.length()));

  // Read and verify the contents

  EXPECT_EQ(contents2.length(), file.read(buff, 0, contents2.length()));

  buff[contents2.length()] = '\0';

  EXPECT_EQ(0, strcmp(buff, contents2.c_str()));

  // Change the contents of the file and verify them

  int charToChange = stripeSize * 1.3;
  EXPECT_EQ(0, file.writeSync("d", charToChange, 1));

  contents2[charToChange] = 'd';

  EXPECT_EQ(contents2.length(), file.read(buff, 0, contents2.length()));

  EXPECT_EQ(0, strcmp(buff, contents2.c_str()));

  charToChange = stripeSize * 1.9;
  EXPECT_EQ(0, file.write("x", charToChange, 1, true));

  contents2[charToChange] = 'x';

  EXPECT_EQ(0, file.sync());

  EXPECT_EQ(contents2.length(), file.read(buff, 0, contents2.length()));

  buff[contents2.length()] = '\0';

  EXPECT_EQ(0, strcmp(buff, contents2.c_str()));

  EXPECT_EQ(0, file.stat(&statBuff));

  EXPECT_EQ(contents2.length(), statBuff.st_size);

  // Read outside of the file's size (from the file size to 2x file size)

  ASSERT_EQ(0, file.read(buff, statBuff.st_size, statBuff.st_size * 2));

  // Increase the file size and read a region that doesn't have corresponding
  // stripes

  const size_t fileOldSize = statBuff.st_size;

  ASSERT_EQ(0, file.truncate(fileOldSize * 2));

  // Read in a region of the file without existing stripes
  // (read the second half of the file)

  ASSERT_EQ(fileOldSize, file.read(buff, fileOldSize,
                                   (fileOldSize * 2) - fileOldSize));

  char *blankContents = new char[contents2.length() + 1];
  memset(blankContents, '\0', contents2.length());

  EXPECT_EQ(0, strcmp(buff, blankContents));

  delete[] buff;
  delete[] blankContents;
}

TEST_F(RadosFsTest, FileVectorRead)
{
  AddPool();

  // Set a small file stripe size so many stripes will be created

  const size_t stripeSize = 64;
  radosFs.setFileStripeSize(stripeSize);

  // Write contents in file synchronously

  const std::string fileName("/test");
  std::stringstream sstream;
  for (size_t i = 0; i < stripeSize * 1.5; i++)
    sstream << i << ".";

  const std::string contents(sstream.str());

  radosfs::File file(&radosFs, fileName);

  const size_t inlineSize = 8;
  EXPECT_EQ(0, file.create(-1, "", 0, inlineSize));

  // Write contents to file

  EXPECT_EQ(0, file.writeSync(contents.c_str(), 0, contents.length()));

  const size_t fileSize = contents.length() + contents.length() / 2;
  EXPECT_EQ(0, file.truncate(fileSize));

  // Read and verify the contents

  char *buff = new char[fileSize + 1];
  char *buff2 = new char[fileSize + 1];

  memcpy(buff2, contents.c_str(), contents.length());
  bzero(buff2 + contents.length(), fileSize - contents.length());

  EXPECT_EQ(contents.length(), file.read(buff, 0, contents.length()));

  EXPECT_EQ(0, strncmp(buff, contents.c_str(), contents.length()));

  ssize_t retValue, retValue1, retValue2, retValue3;
  size_t readLength = inlineSize / 2;
  size_t readLength1 = inlineSize;
  size_t readLength2 = fileSize * 2;

  std::vector<radosfs::FileReadData> intervals;
  intervals.push_back(radosfs::FileReadData(buff, 0, readLength, &retValue));
  intervals.push_back(radosfs::FileReadData(buff + readLength, readLength,
                                            readLength1, &retValue1));

  intervals.push_back(radosfs::FileReadData(buff + readLength + readLength1,
                                            readLength + readLength1,
                                            readLength2, &retValue2));
  intervals.push_back(radosfs::FileReadData(buff, fileSize + 2, 1,
                                            &retValue3));
  std::string opId;

  memset(buff, 'x', fileSize + 1);

  EXPECT_EQ(0, file.read(intervals, &opId));

  int ret = file.sync(opId);

  // -ENOENT Because one of the operations goes beyond the file size
  ASSERT_EQ(-ENOENT, ret);

  EXPECT_EQ(readLength, retValue);
  EXPECT_EQ(readLength1, retValue1);
  EXPECT_EQ(fileSize - (readLength + readLength1), retValue2);
  EXPECT_EQ(0, retValue3);
  EXPECT_EQ(0, strncmp(buff, contents.c_str(), contents.length()));
  EXPECT_EQ(0, strncmp(buff, buff2, fileSize));
  EXPECT_EQ(0, strncmp(buff, contents.c_str(), readLength + readLength1));

  radosfs::File otherFile(&radosFs, "/test1");

  EXPECT_EQ(-ENOENT, otherFile.read(intervals, &opId));

  EXPECT_EQ(0, otherFile.create(-1, "", 0, 0));

  EXPECT_EQ(0, otherFile.read(intervals, &opId));

  ret = otherFile.sync(opId);

  // -ENOENT Because one of the operations goes beyond the file size
  ASSERT_EQ(-ENOENT, ret);

  EXPECT_EQ(0, retValue);
  EXPECT_EQ(0, retValue1);
  EXPECT_EQ(0, retValue2);
  EXPECT_EQ(0, retValue3);

  EXPECT_EQ(0, otherFile.truncate(readLength));

  EXPECT_EQ(0, otherFile.read(intervals, &opId));

  ret = otherFile.sync(opId);

  // -ENOENT Because one of the operations goes beyond the file size
  ASSERT_EQ(-ENOENT, ret);

  bzero(buff2, readLength);

  EXPECT_EQ(readLength, retValue);
  EXPECT_EQ(0, strncmp(buff, buff2, readLength));
  EXPECT_EQ(0, retValue1);
  EXPECT_EQ(0, retValue2);
  EXPECT_EQ(0, retValue3);


  delete buff;
  delete buff2;
}

TEST_F(RadosFsTest, FileInline)
{
  AddPool();

  radosfs::File file(&radosFs, "/file");

  // Create a file with an inline buffer size that is too big

  ASSERT_EQ(-EINVAL, file.create(-1, "", 0, MAX_FILE_INLINE_BUFFER_SIZE + 1));

  // Create a file with a custom inline buffer size

  const size_t inlineBufferSize(512);

  ASSERT_EQ(0, file.create(-1, "", 0, inlineBufferSize));

  EXPECT_EQ(inlineBufferSize, file.inlineBufferSize());

  // Write contents only in the inline buffer

  char contents[inlineBufferSize * 2];
  memset(contents, 'x', inlineBufferSize * 2);
  contents[inlineBufferSize * 2 - 1] = '\0';

  ASSERT_EQ(0, file.write(contents, 0, inlineBufferSize));

  // Verify that the inode object was not created

  std::string inodeObj = radosFsFilePriv(file)->inode->name();

  Stat stat;
  radosFsPriv()->stat(file.path(), &stat);

  EXPECT_EQ(-ENOENT, stat.pool->ioctx.stat(inodeObj, 0, 0));

  // Read the inline contetns

  char contents2[inlineBufferSize * 2];

  ASSERT_EQ(inlineBufferSize - 1, file.read(contents2, 0,
                                            inlineBufferSize - 1));

  EXPECT_TRUE(strncmp(contents2, contents, inlineBufferSize - 1) == 0);

  // Write beyond than the inline buffer size and verify it creates the inode
  // object

  ASSERT_EQ(0, file.write(contents, inlineBufferSize, inlineBufferSize));

  file.sync();

  EXPECT_EQ(0, stat.pool->ioctx.stat(inodeObj, 0, 0));

  // Read the full file length

  bzero(contents2, inlineBufferSize);

  ASSERT_EQ(inlineBufferSize * 2, file.read(contents2, 0, inlineBufferSize * 2));

  contents2[inlineBufferSize * 2 - 1] = '\0';

  EXPECT_TRUE(strcmp(contents2, contents) == 0);

  // Truncate so contents only exist in the inline buffer

  ASSERT_EQ(0, file.truncate(inlineBufferSize / 2));

  // Verify that the size of the contents read match what was truncated

  EXPECT_EQ(inlineBufferSize / 2, file.read(contents2, 0, inlineBufferSize / 2));

  // Truncate to 0

  ASSERT_EQ(0, file.truncate(0));

  // Write beyond the inline buffer when it is not full and then only up to
  // half of it

  bzero(contents2, inlineBufferSize * 2);

  ASSERT_EQ(0, file.write(contents, inlineBufferSize, inlineBufferSize));

  ASSERT_EQ(0, file.write(contents, 0, inlineBufferSize / 2));

  // Verify that all contents are read

  EXPECT_EQ(inlineBufferSize * 2, file.read(contents2, 0, inlineBufferSize * 2));

  char blankContents[inlineBufferSize / 2];
  memset(blankContents, '\0', inlineBufferSize / 2);

  EXPECT_TRUE(strncmp(contents2 + inlineBufferSize / 2, blankContents,
                      inlineBufferSize / 2) == 0);
}

TEST_F(RadosFsTest, RenameFile)
{
  AddPool();

  std::string originalPath("/my-file");
  std::string path("/moved-file");

  radosfs::File file(&radosFs, originalPath);

  // Rename file that doesn't exist

  EXPECT_EQ(-ENOENT, file.rename(path));

  EXPECT_EQ(0, file.create());

  // Move file into a directory that doesn't exist

  EXPECT_EQ(-ENOENT, file.rename("/phony/" + path));

  // Move file in the same directory

  EXPECT_EQ(0, file.rename(path));

  EXPECT_EQ(path, file.path());

  EXPECT_TRUE(file.exists());

  // Make sure that renaming didn't create the inode

  Stat stat;

  EXPECT_EQ(0, radosFsPriv()->stat(file.path(), &stat));

  EXPECT_EQ(-ENOENT, stat.pool->ioctx.stat(stat.translatedPath, 0, 0));

  file.setPath(originalPath);

  EXPECT_FALSE(file.exists());

  // Create a user directory

  radosfs::Dir userDir(&radosFs, "/user-dir");

  EXPECT_EQ(0, userDir.create(-1, false, TEST_UID, TEST_GID));

  radosFs.setIds(TEST_UID, TEST_GID);

  file.setPath(path);

  // Rename file without the required permissions

  EXPECT_EQ(-EACCES, file.rename(originalPath));

  // Rename a file as user

  path = userDir.path() + "user-file";

  file.setPath(path);

  EXPECT_EQ(0, file.create(-1, "", 0, 124));

  // Add contents to the file's inline buffer

  std::string fileContents = "abcdef";
  char *fileContsBuff = new char[fileContents.length()];

  EXPECT_EQ(0, file.writeSync(fileContents.c_str(), 0, fileContents.length()));

  EXPECT_EQ(fileContents.length(),
            file.read(fileContsBuff, 0, fileContents.length()));

  EXPECT_EQ(0, strncmp(fileContsBuff, fileContents.c_str(),
                       fileContents.length()));

  // Move the file inside the same directory

  path = userDir.path() + "file";

  EXPECT_EQ(0, file.rename(path));

  radosfs::File sameFile(&radosFs, path);

  EXPECT_TRUE(sameFile.exists());

  // Check the contents

  bzero(fileContsBuff, fileContents.length());

  EXPECT_EQ(fileContents.length(),
            sameFile.read(fileContsBuff, 0, fileContents.length()));

  EXPECT_EQ(0, strncmp(fileContsBuff, fileContents.c_str(),
                       fileContents.length()));

  // Get the user dir's entries

  std::set<std::string> entries;

  userDir.update();

  EXPECT_EQ(0, userDir.entryList(entries));

  // Rename the file (owned by the user) as root

  radosFs.setIds(ROOT_UID, ROOT_UID);

  path = "/file-moved";

  EXPECT_EQ(0, sameFile.rename(path));

  file.setPath(path);

  EXPECT_TRUE(file.exists());

  // Check the contents again

  bzero(fileContsBuff, fileContents.length());

  EXPECT_EQ(fileContents.length(),
            file.read(fileContsBuff, 0, fileContents.length()));

  EXPECT_EQ(0, strncmp(fileContsBuff, fileContents.c_str(),
                       fileContents.length()));

  // Get the user dir's contents again and compare them with the old ones

  std::set<std::string> entries1;

  userDir.update();

  EXPECT_EQ(0, userDir.entryList(entries1));

  EXPECT_LT(entries1.size(), entries.size());

  EXPECT_EQ(entries1.find("file"), entries1.end());

  // Verify that the new file's parent can list it

  entries.clear();

  radosfs::Dir rootDir(&radosFs, "/");

  rootDir.update();

  EXPECT_EQ(0, rootDir.entryList(entries));

  EXPECT_NE(entries.find("file-moved"), entries.end());

  // Move the file to the user's dir

  path = userDir.path() + path;

  EXPECT_EQ(0, file.rename(path));

  sameFile.setPath(path);

  EXPECT_TRUE(sameFile.exists());

  // Rename the file to an empty path argument

  EXPECT_EQ(-EINVAL, file.rename(""));

  // Rename the file to its own name

  EXPECT_EQ(-EPERM, file.rename(file.path()));

  // Rename the file to a directory path

  EXPECT_EQ(-EISDIR, file.rename(userDir.path()));

  // Rename the file to be in the root directory

  path = "/file";

  EXPECT_EQ(0, file.rename(path));

  EXPECT_EQ(path, file.path());

  sameFile.update();

  EXPECT_FALSE(sameFile.exists());

  // Create a file without an inline buffer so we check if the backlink in its
  // inode gets updated when the file is renamed

  file = radosfs::File(&radosFs, "/new-file");

  EXPECT_EQ(0, file.create(-1, "", 0, 0));

  EXPECT_EQ(0, file.writeSync("x", 0, 1));

  testFileInodeBackLink(file.path());

  path = "/new-file-renamed";

  EXPECT_EQ(0, file.rename(path));

  ASSERT_EQ(path, file.path());

  testFileInodeBackLink(file.path());
}

bool
checkStripesExistence(librados::IoCtx ioctx, const std::string &baseName,
                      size_t firstStripe, size_t lastStripe, bool shouldExist)
{
  bool checkResult = true;
  for (size_t i = firstStripe; i <= lastStripe; i++)
  {
    std::string stripe = makeFileStripeName(baseName, i);

    if (ioctx.stat(stripe, 0, 0) != 0)
    {
      if (shouldExist)
      {
        fprintf(stderr, "Error: Stripe %s does not exist!\n", stripe.c_str());
        checkResult = false;
      }
    }
    else if (!shouldExist)
    {
      fprintf(stderr, "Error: Stripe %s exist!\n", stripe.c_str());
      checkResult = false;
    }
  }

  return checkResult;
}

TEST_F(RadosFsTest, FileOpsMultClientsWriteTruncate)
{
    const size_t size = pow(1024, 3);
    const size_t numStripes = 30;
    const size_t stripeSize = size / numStripes;
    char *contents = new char[size];
    const std::string fileName("/file");
    FsActionInfo c1(0, FS_ACTION_TYPE_FILE, fileName, "write",
                    contents, size, 0, 0);
    FsActionInfo c2(0, FS_ACTION_TYPE_FILE, fileName, "truncate",
                    0, 0, 0, 0);

    radosfs::File *file = launchFileOpsMultipleClients(stripeSize, fileName,
                                                       &c1, &c2);

    std::string inode = radosFsFilePriv(*file)->getFileIO()->inode();
    librados::IoCtx ioctx = radosFsFilePriv(*file)->dataPool->ioctx;

    EXPECT_TRUE(checkStripesExistence(ioctx, inode, 0, 0, true));

    EXPECT_TRUE(checkStripesExistence(ioctx, inode, 1, numStripes, false));

    delete file;
}

TEST_F(RadosFsTest, FileOpsMultClientsWriteRemove)
{
    const size_t size = pow(1024, 3);
    const size_t numStripes = 30;
    const size_t stripeSize = size / numStripes;
    char *contents = new char[size];
    const std::string fileName("/file");
    FsActionInfo c1(0, FS_ACTION_TYPE_FILE, fileName, "write",
                    contents, size, 0, 0);
    FsActionInfo c2(0, FS_ACTION_TYPE_FILE, fileName, "remove",
                    0, 0, 0, 0);

    radosfs::File *file = launchFileOpsMultipleClients(stripeSize, fileName,
                                                       &c1, &c2);

    std::string inode = radosFsFilePriv(*file)->getFileIO()->inode();
    librados::IoCtx ioctx = radosFsFilePriv(*file)->dataPool->ioctx;

    EXPECT_TRUE(checkStripesExistence(ioctx, inode, 0, numStripes, false));

    delete file;
}

TEST_F(RadosFsTest, FileOpsMultClientsTruncateRemove)
{
    const size_t size = pow(1024, 3);
    const size_t numStripes = 30;
    const size_t stripeSize = size / numStripes;
    char *contents = new char[size];
    const std::string fileName("/file");
    FsActionInfo c1(0, FS_ACTION_TYPE_FILE, fileName, "truncate",
                    contents, size, 0, 0);
    FsActionInfo c2(0, FS_ACTION_TYPE_FILE, fileName, "remove",
                    0, 0, 0, 0);

    radosfs::File *file = launchFileOpsMultipleClients(stripeSize, fileName,
                                                       &c1, &c2);

    std::string inode = radosFsFilePriv(*file)->getFileIO()->inode();
    librados::IoCtx ioctx = radosFsFilePriv(*file)->dataPool->ioctx;

    EXPECT_TRUE(checkStripesExistence(ioctx, inode, 0, numStripes, false));

    delete file;
}

TEST_F(RadosFsTest, DirOpsMultipleClients)
{
  radosFs.addDataPool(TEST_POOL, "/", 50 * 1024);
  radosFs.addMetadataPool(TEST_POOL, "/");

  // Create another RadosFs instance to be used as a different client

  radosfs::Filesystem otherClient;
  otherClient.init("", conf());

  otherClient.addDataPool(TEST_POOL, "/", 50 * 1024);
  otherClient.addMetadataPool(TEST_POOL, "/");

  // Create the same directory from both clients

  radosfs::Dir cli1DirInst(&radosFs, "/dir");
  radosfs::Dir cli2DirInst(&otherClient, "/dir");

  EXPECT_EQ(0, cli1DirInst.create());
  EXPECT_EQ(-EEXIST, cli2DirInst.create());

  // Launch 10 threads for each client, creating files and dirs in the same
  // directory

  const int numOps = 10;
  boost::thread *cli1Threads[numOps], *cli2Threads[numOps];
  FsActionInfo *cli1ActionInfos[numOps];
  FsActionInfo *cli2ActionInfos[numOps];

  for (int i = 0; i < numOps; i++)
  {
    bool createDir = (i % 2) == 0;
    FsActionType actionType = FS_ACTION_TYPE_FILE;
    std::stringstream stream;
    stream << cli1DirInst.path();

    if (createDir)
    {
      stream << "client-1-dir-" << i;
      actionType = FS_ACTION_TYPE_DIR;
    }
    else
    {
      stream << "client-1-file-" << i;
    }

    cli1ActionInfos[i] = new FsActionInfo(&radosFs, actionType, stream.str(),
                                          "create", "", 0, 0, 0);
    cli1Threads[i] = new boost::thread(&RadosFsTest::runInThread,
                                       cli1ActionInfos[i]);
  }

  for (int i = 0; i < numOps; i++)
  {
    bool createDir = (i % 2) != 0;
    FsActionType actionType = FS_ACTION_TYPE_FILE;
    std::stringstream stream;
    stream << cli2DirInst.path();

    if (createDir)
    {
      stream << "client-1-dir-" << i;
      actionType = FS_ACTION_TYPE_DIR;
    }
    else
    {
      stream << "client-1-file-" << i;
    }

    cli2ActionInfos[i] = new FsActionInfo(&radosFs, actionType, stream.str(),
                                          "create", "", 0, 0, 0);

    cli2Threads[i] = new boost::thread(&RadosFsTest::runInThread,
                                       cli2ActionInfos[i]);
  }

  sleep(3);

  for (int i = 0; i < numOps; i++)
  {
    cli1Threads[i]->join();
    cli2Threads[i]->join();

    delete cli1Threads[i];
    delete cli1ActionInfos[i];
    delete cli2Threads[i];
    delete cli2ActionInfos[i];
  }

  // Verify that both dir instances have the same number of entries

  cli1DirInst.update();

  std::set<std::string> entries;

  EXPECT_EQ(0, cli1DirInst.entryList(entries));

  EXPECT_EQ(2 * numOps, entries.size());

  entries.clear();
  cli2DirInst.update();

  EXPECT_EQ(0, cli2DirInst.entryList(entries));

  EXPECT_EQ(2 * numOps, entries.size());
}

TEST_F(RadosFsTest, StatCluster)
{
  AddPool();

  uint64_t total = 0, used = 1, available = 1, numberOfObjects;

  int ret = radosFs.statCluster(&total, &used, &available, &numberOfObjects);

  EXPECT_EQ(0, ret);

  EXPECT_GT(total, used);

  EXPECT_GT(total, available);
}

TEST_F(RadosFsTest, XAttrs)
{
  AddPool();

  // Create a folder for the user

  radosfs::Dir dir(&radosFs, "/user");
  EXPECT_EQ(0, dir.create((S_IRWXU | S_IRGRP | S_IROTH), false, TEST_UID, TEST_GID));

  const std::string &fileName(dir.path() + "file");

  radosFs.setIds(TEST_UID, TEST_GID);

  // Create a file for the xattrs

  radosfs::File file(&radosFs, fileName,
                     radosfs::File::MODE_READ_WRITE);

  EXPECT_EQ(0, file.create((S_IRWXU | S_IRGRP | S_IROTH)));

  // Get an invalid xattr

  std::string xAttrValue;

  EXPECT_EQ(-EINVAL, radosFs.getXAttr(fileName, "invalid", xAttrValue));

  // Get an inexistent

  EXPECT_LT(radosFs.getXAttr(fileName, "usr.inexistent", xAttrValue), 0);

  // Set a user attribute

  const std::string attr("usr.attr");
  const std::string value("value");
  EXPECT_EQ(0, radosFs.setXAttr(fileName, attr, value));

  testFileInodeBackLink(fileName);

  // Check if the attribute got into the file inode's omap

  Stat stat;
  ASSERT_EQ(0, radosFsPriv()->stat(fileName, &stat));

  std::map<std::string, librados::bufferlist> omap;
  std::set<std::string> omapKeys;

  omapKeys.insert(attr);
  ASSERT_EQ(0, stat.pool->ioctx.omap_get_vals(stat.translatedPath, "", UINT_MAX,
                                              &omap));

  ASSERT_TRUE(omap.find(attr) != omap.end());

  // Get the attribute set above

  EXPECT_EQ(value.length(), radosFs.getXAttr(fileName, attr, xAttrValue));

  // Check the attribtue's value

  EXPECT_EQ(value, xAttrValue);

  // Change to another user

  radosFs.setIds(TEST_UID + 1, TEST_GID + 1);

  // Set an xattr by an unauthorized user

  EXPECT_EQ(-EACCES, radosFs.setXAttr(fileName, attr, value));

  // Get an xattr by a user who can only read

  EXPECT_EQ(value.length(), radosFs.getXAttr(fileName, attr, xAttrValue));

  // Check the attribute's value

  EXPECT_EQ(value, xAttrValue);

  // Remove an xattr by an unauthorized user

  EXPECT_EQ(-EACCES, radosFs.removeXAttr(fileName, attr));

  // Get the xattrs map

  std::map<std::string, std::string> map;

  EXPECT_EQ(0, radosFs.getXAttrsMap(fileName, map));

  // Check the xattrs map's size

  EXPECT_EQ(1, map.size());

  // Switch to the root user

  radosFs.setIds(ROOT_UID, ROOT_UID);

  map.clear();

  // Set an xattr -- when being root -- in a different user's file

  EXPECT_EQ(0, radosFs.setXAttr(fileName, "sys.attribute", "check"));

  // Get the xattrs map

  EXPECT_EQ(0, radosFs.getXAttrsMap(fileName, map));

  // Check the xattrs map's size

  EXPECT_EQ(2, map.size());

  // Check the xattrs map's value

  EXPECT_EQ(map[attr], value);

  // Set an attribute in a directory

  const std::string dirAttr("usr.dir-attr");
  EXPECT_EQ(0, radosFs.setXAttr(dir.path(), dirAttr, "check"));

  // Check if the attribute got into the dir inode's omap

  stat.reset();
  ASSERT_EQ(0, radosFsPriv()->stat(dir.path(), &stat));

  omap.clear();
  omapKeys.clear();

  omapKeys.insert(dirAttr);
  ASSERT_EQ(0, stat.pool->ioctx.omap_get_vals(stat.translatedPath, "", UINT_MAX,
                                              &omap));

  ASSERT_TRUE(omap.find(dirAttr) != omap.end());
}

TEST_F(RadosFsTest, XAttrsInInfo)
{
  AddPool();

  radosfs::Dir dir(&radosFs, "/user");

  EXPECT_EQ(0, dir.create((S_IRWXU | S_IRGRP | S_IROTH),
                          false, TEST_UID, TEST_GID));

  testXAttrInFsInfo(dir);

  radosFs.setIds(TEST_UID, TEST_GID);

  // Create a file for the xattrs

  radosfs::File file(&radosFs, dir.path() + "file",
                     radosfs::File::MODE_READ_WRITE);

  EXPECT_EQ(0, file.create((S_IRWXU | S_IRGRP | S_IROTH)));

  testXAttrInFsInfo(file);
}

TEST_F(RadosFsTest, DirCache)
{
  AddPool();

  const size_t maxSize = 4;

  // Set a maximum size for the cache and verify

  radosFs.setDirCacheMaxSize(maxSize);

  EXPECT_EQ(maxSize, radosFs.dirCacheMaxSize());

  EXPECT_EQ(0, radosFsPriv()->dirCache.size());

  // Instantiate a dir and check that the cache size stays the same

  radosfs::Dir dir(&radosFs, "/dir");

  EXPECT_EQ(0, radosFsPriv()->dirCache.size());

  // Create that dir and check that the cache size increments

  EXPECT_EQ(0, dir.create());

  EXPECT_EQ(1, radosFsPriv()->dirCache.size());

  // Check that the most recent cached dir has the same inode
  // as the one we created

  EXPECT_EQ(radosFsDirPriv(dir)->fsStat()->translatedPath,
            radosFsPriv()->dirCache.head->cachePtr->inode());

  // Instantiate another dir from the one before and verify the cache
  // stays the same

  radosfs::Dir otherDir(dir);

  EXPECT_EQ(1, radosFsPriv()->dirCache.size());

  // Change the path and verify the cache size increments

  otherDir.setPath("/dir1");
  otherDir.create();

  EXPECT_EQ(2, radosFsPriv()->dirCache.size());

  // Check that the most recent cached dir has the same inode
  // as the one we created

  EXPECT_EQ(radosFsDirPriv(otherDir)->fsStat()->translatedPath,
            radosFsPriv()->dirCache.head->cachePtr->inode());

  // Create a sub directory and verify that the cache size increments

  radosfs::Dir subdir(&radosFs, "/dir/subdir");
  EXPECT_EQ(0, subdir.create());

  EXPECT_EQ(3, radosFsPriv()->dirCache.size());

  // Check that the most recent cached dir has the same inode
  // as the one we created

  EXPECT_EQ(radosFsDirPriv(subdir)->fsStat()->translatedPath,
            radosFsPriv()->dirCache.head->cachePtr->inode());

  // Update the parent dir of the one we created and verify
  // that the cache size increments (because now it has an entry)

  dir.update();

  EXPECT_EQ(4, radosFsPriv()->dirCache.size());

  // Check that the most recent cached dir has the same inode
  // as the one we updated

  EXPECT_EQ(radosFsDirPriv(dir)->fsStat()->translatedPath,
            radosFsPriv()->dirCache.head->cachePtr->inode());

  // Change the cache's max size so it allows to hold only one dir
  // with no entries

  radosFs.setDirCacheMaxSize(1);

  // Verify that the cache's contents were cleaned due to the
  // ridiculously small size

  EXPECT_EQ(0, radosFsPriv()->dirCache.size());

  // Update dir with one entry and verify it doesn't get cached
  // (because the cache size would be greater than the maximum)

  dir.update();

  EXPECT_EQ(0, radosFsPriv()->dirCache.size());

  // Update the subdir (with no entries) and verify the cache
  // size increments

  subdir.update();

  EXPECT_EQ(1, radosFsPriv()->dirCache.size());

  // Remove the cached dir and verify the cache size decrements

  subdir.remove();

  EXPECT_EQ(0, radosFsPriv()->dirCache.size());

  // Create an uncacheable dir and verify the cache isn't affected

  radosFs.setDirCacheMaxSize(100);

  radosfs::Dir notCachedDir(&radosFs, "/notcached", false);
  EXPECT_EQ(0, notCachedDir.create());

  notCachedDir.update();

  EXPECT_EQ(0, radosFsPriv()->dirCache.size());
}

TEST_F(RadosFsTest, CompactDir)
{
  AddPool();

  radosfs::Filesystem otherClient;
  otherClient.init("", conf());

  otherClient.addDataPool(TEST_POOL, "/", 50 * 1024);
  otherClient.addMetadataPool(TEST_POOL_MTD, "/");

  // Set a different compact ratio
  // (and a lower one as well, so it doesn't trigger compaction)

  const float newRatio = 0.01;

  radosFs.setDirCompactRatio(newRatio);
  EXPECT_EQ(newRatio, radosFs.dirCompactRatio());

  // Create files and remove half of them

  const size_t numFiles = 10;

  createNFiles(numFiles);
  removeNFiles(numFiles / 2);

  // Check that the size of the object is greater than the original one,
  // after the dir is updated

  const std::string dirPath("/");
  struct stat statBefore, statAfter;

  radosFs.stat(dirPath, &statBefore);

  radosfs::Dir dir(&radosFs, dirPath);
  dir.update();

  radosFs.stat(dirPath, &statAfter);

  EXPECT_GT(statBefore.st_size, 0);

  EXPECT_EQ(statAfter.st_size, statBefore.st_size);

  // Get the entries before the compaction

  std::set<std::string> entriesBefore, entriesAfter;
  dir.entryList(entriesBefore);

  // Instance the same dir from a different client

  radosfs::Dir sameDir(&otherClient, dir.path());

  sameDir.update();

  std::set<std::string> otherClientEntries;
  sameDir.entryList(otherClientEntries);

  // Check that it gets the same number of entries for the same dir

  EXPECT_EQ(entriesBefore.size(), otherClientEntries.size());

  // Set a hight compact ratio so it automatically compacts
  // when we update the dir

  radosFs.setDirCompactRatio(0.9);

  dir.update();

  // Check if it compacted after the update

  radosFs.stat(dirPath, &statAfter);

  EXPECT_LT(statAfter.st_size, statBefore.st_size);

  // Compact it "manually"

  radosFs.setDirCompactRatio(0.01);

  createNFiles(numFiles);
  removeNFiles(numFiles / 2);

  dir.compact();

  radosFs.stat(dirPath, &statAfter);

  EXPECT_LT(statAfter.st_size, statBefore.st_size);

  // Check the integrity of the entries in the dir, before and after the
  // compaction

  dir.update();

  dir.entryList(entriesAfter);

  EXPECT_EQ(entriesBefore, entriesAfter);

  // Check that the other client's dir instance also gets the same entries
  // after it had been compacted from a different client

  sameDir.update();

  otherClientEntries.clear();
  sameDir.entryList(otherClientEntries);

  EXPECT_EQ(entriesAfter, otherClientEntries);

  // Compact when metadata exists

  const int totalMetadata(5);
  const std::string key("mykey"), value("myvalue");
  std::stringstream fileNameStr;
  fileNameStr << "file" << (numFiles / 2 + 1);

  for (int i = 0; i < totalMetadata; i++)
  {
    std::ostringstream keyStr, valueStr;

    keyStr << key << i;
    valueStr << value << i;

    EXPECT_EQ(0, dir.setMetadata(fileNameStr.str(),
                                 keyStr.str(),
                                 valueStr.str()));
  }

  radosFs.stat(dirPath, &statBefore);

  dir.compact();

  radosFs.stat(dirPath, &statAfter);

  EXPECT_LT(statAfter.st_size, statBefore.st_size);

  for (int i = 0; i < totalMetadata; i++)
  {
    std::string valueSet;
    std::ostringstream keyStr, valueStr;

    keyStr << key << i;
    valueStr << value << i;

    EXPECT_EQ(0, dir.getMetadata(fileNameStr.str(), keyStr.str(), valueSet));
    EXPECT_EQ(valueStr.str(), valueSet);
  }
}

TEST_F(RadosFsTest, RenameDir)
{
  AddPool();

  std::string originalPath("/my-dir/");
  std::string path("/moved-dir/");
  std::string userDirPath("/user-dir/");

  radosfs::Dir dir(&radosFs, originalPath);

  // Rename dir that doesn't exist

  EXPECT_EQ(-ENOENT, dir.rename(path));

  EXPECT_EQ(0, dir.create());

  // Move dir to a path that doesn't exist

  EXPECT_EQ(-ENOENT, dir.rename("/phony/" + path));

  // Create a user directory

  radosfs::Dir userDir(&radosFs, userDirPath);

  EXPECT_EQ(0, userDir.create(-1, false, TEST_UID, TEST_GID));

  radosFs.setIds(TEST_UID, TEST_GID);

  dir.setPath(path);

  // Rename dir without the required permissions

  EXPECT_EQ(-EACCES, userDir.rename(originalPath));

  // Create a dir as user

  originalPath = userDir.path() + "other-dir";
  path = originalPath + "-moved";

  dir.setPath(originalPath);

  EXPECT_EQ(0, dir.create());

  // Move the dir inside the same parent

  EXPECT_EQ(0, dir.rename(path));

  radosfs::Dir sameDir(&radosFs, path);

  EXPECT_TRUE(sameDir.exists());

  // Rename the dir (owned by the user) as root

  radosFs.setIds(ROOT_UID, ROOT_UID);

  path = "/other-dir-moved";

  EXPECT_EQ(0, sameDir.rename(path));

  dir.setPath(path);

  EXPECT_TRUE(dir.exists());

  // Move the dir to the user's dir

  path = userDir.path() + path;

  EXPECT_EQ(0, dir.rename(path));

  sameDir.setPath(path);

  EXPECT_TRUE(sameDir.exists());

  // Rename the dir to an empty path argument

  EXPECT_EQ(-EINVAL, dir.rename(""));

  // Rename the dir to the same name

  EXPECT_EQ(-EPERM, dir.rename(dir.path()));

  EXPECT_EQ(-EPERM, dir.rename(dir.path() + "/other"));

  // Create a file in the user dir to see if it is moved

  dir.setPath(userDirPath);

  std::string fileName = "my-file";
  radosfs::File file(&radosFs, dir.path() + fileName);

  EXPECT_EQ(0, file.create());

  // Rename the user dir to a different name

  userDirPath = "/moved-user-dir";

  EXPECT_EQ(0, dir.rename(userDirPath));

  // Check that the subdir of the user dir no longer exists (it was moved)

  sameDir.update();

  EXPECT_FALSE(sameDir.exists());

  // Check that the new subdir (with the new user dir path as parent) now exists

  sameDir.setPath(userDirPath + "/other-dir-moved");

  EXPECT_TRUE(sameDir.exists());

  // Check that the file in the old user dir no longer exists

  file.update();

  EXPECT_FALSE(file.exists());

  // Check that the file in the new user dir exists

  file.setPath(dir.path() + fileName);

  EXPECT_TRUE(file.exists());

  // Rename the dir to a file path

  EXPECT_EQ(-EPERM, dir.rename(file.path()));

  // Rename the dir to an existing dir path

  EXPECT_EQ(-EPERM, sameDir.rename(dir.path()));
}

TEST_F(RadosFsTest, RenameWithLinks)
{
  AddPool();

  std::string dirPath("/dir"), linkPath("/dir-link"), filePath("/file");

  // Create a dir and a link to it

  radosfs::Dir dir(&radosFs, dirPath);

  EXPECT_EQ(0, dir.create());

  EXPECT_EQ(0, dir.createLink(linkPath));

  // Create a file and rename it to a path that includes the dir link

  radosfs::File file(&radosFs, filePath);

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(0, file.rename(linkPath + filePath));

  // Create a dir and rename it to a path that includes the dir link

  EXPECT_EQ(-EPERM, dir.rename(linkPath + "/dir-moved"));

  // Rename the dir to the link path

  EXPECT_EQ(-EPERM, dir.rename(linkPath));

  radosfs::Dir linkDir(&radosFs, linkPath);

  EXPECT_TRUE(linkDir.exists());

  // Rename the file with link path in the name

  EXPECT_EQ(dir.path() + "file", file.path());

  EXPECT_EQ(0, file.rename(linkPath));

  // Verify that the old dir link object is now the file we renamed

  linkDir.update();

  EXPECT_TRUE(linkDir.exists());

  EXPECT_FALSE(linkDir.isDir());

  EXPECT_FALSE(linkDir.isLink());
}

TEST_F(RadosFsTest, Metadata)
{
  AddPool();

  const std::string &basePath = "f1";

  radosfs::Dir dir(&radosFs, "/");

  std::string key = "mykey", value = "myvalue";

  // Set metadata on an inexistent file

  EXPECT_EQ(-ENOENT, dir.setMetadata(basePath, key, value));

  // Create the file and check again

  radosfs::File file(&radosFs, "/" + basePath,
                     radosfs::File::MODE_READ_WRITE);

  file.create();

  EXPECT_EQ(0, dir.setMetadata(basePath, key, value));

  // Verify the value set

  std::string newValue = "";

  EXPECT_EQ(0, dir.getMetadata(basePath, key, newValue));

  EXPECT_EQ(value, newValue);

  // Remove inexistent metadata

  EXPECT_EQ(-ENOENT, dir.removeMetadata(basePath, key + "_fake"));

  // Remove the metadata set before

  EXPECT_EQ(0, dir.removeMetadata(basePath, key));

  // Get the metadata previously removed

  EXPECT_EQ(-ENOENT, dir.getMetadata(basePath, key, newValue));

  // Set metadata with an empty string as key

  EXPECT_EQ(-EINVAL, dir.setMetadata(basePath, "", value));

  // Set metadata with an empty string as value

  EXPECT_EQ(0, dir.setMetadata(basePath, "empty", ""));

  // Set metadata with non-ascii chars and whitespace

  key = "\n acções \n  über \n\n   %%   #  caractères \n \"extraños\" \n%";
  value = "\n value of " + key + " \n value";

  EXPECT_EQ(0, dir.setMetadata(basePath, key, value));

  EXPECT_EQ(0, dir.getMetadata(basePath, key, newValue));

  EXPECT_EQ(value, newValue);

  // Get the metadata with an unauthorized user

  radosFs.setIds(TEST_UID, TEST_GID);

  EXPECT_EQ(-EACCES, dir.setMetadata(basePath, key, value));
}

TEST_F(RadosFsTest, LinkDir)
{
  AddPool();

  const std::string &linkName("dirLink");

  radosfs::Dir dir(&radosFs, "/dir");

  // Create a link to a dir that doesn't exist

  EXPECT_EQ(-ENOENT, dir.createLink(linkName));

  dir.create();

  // Create a link to a dir that exists

  EXPECT_EQ(0, dir.createLink(linkName));

  // Verify the link

  radosfs::Dir dirLink(&radosFs, linkName);

  EXPECT_TRUE(dirLink.exists());

  EXPECT_TRUE(dirLink.isDir());

  EXPECT_TRUE(dirLink.isLink());

  EXPECT_EQ(dir.path(), dirLink.targetPath());

  struct stat buff;

  EXPECT_EQ(0, radosFs.stat(dirLink.path(), &buff));

  EXPECT_NE(0, buff.st_mode & S_IFLNK);

  // Create a file in the original dir

  radosfs::File file(&radosFs,
                     dir.path() + "f1",
                     radosfs::File::MODE_READ_WRITE);

  file.create();

  // Get the dir's entries using the link and verify them

  dirLink.update();

  std::set<std::string> entries, entriesAfter;

  EXPECT_EQ(0, dirLink.entryList(entries));

  EXPECT_NE(entries.end(), entries.find("f1"));

  // Verify dealing with metadata through the link

  std::string mdKey = "testLink", mdValue = "testLinkValue", value;

  EXPECT_EQ(0, dirLink.setMetadata("f1", mdKey, mdValue));

  EXPECT_EQ(0, dirLink.getMetadata("f1", mdKey, value));

  EXPECT_EQ(mdValue, value);

  value = "";

  EXPECT_EQ(0, dir.getMetadata("f1", mdKey, value));

  EXPECT_EQ(mdValue, value);

  EXPECT_EQ(0, dirLink.removeMetadata("f1", mdKey));

  EXPECT_EQ(-ENOENT, dir.getMetadata("f1", mdKey, value));

  // Verify dealing with xattrs through the link

  std::map<std::string, std::string> map;

  value = "";
  mdKey = "sys.myattr";

  EXPECT_EQ(0, dirLink.setXAttr(mdKey, mdValue));

  EXPECT_GT(dirLink.getXAttr(mdKey, value), 0);

  EXPECT_EQ(mdValue, value);

  EXPECT_EQ(0, dirLink.getXAttrsMap(map));

  EXPECT_GT(map.size(), 0);

  EXPECT_EQ(mdValue.length(), radosFs.getXAttr(dirLink.path(), mdKey, value));

  // Create a dir using the link as parent

  radosfs::Dir otherDir(&radosFs, dirLink.path() + "d2");

  otherDir.create();

  EXPECT_EQ(dir.path() + "d2/", otherDir.path());

  // Check that the subdir was correctly created

  dir.update();

  entries.clear();

  EXPECT_EQ(0, dirLink.entryList(entries));

  EXPECT_NE(entries.end(), entries.find("d2/"));

  // Create another link

  EXPECT_EQ(0, dir.createLink("/dir/dirLink2"));

  radosfs::Dir otherDirLink(&radosFs, dir.path() + "dirLink2");

  EXPECT_TRUE(otherDirLink.isDir());

  EXPECT_TRUE(otherDirLink.isLink());

  // Create a file inside with a path with two links as intermediate ones

  file.setPath("dirLink/dirLink2/f2");

  EXPECT_EQ(0, file.create());

  EXPECT_EQ(dir.path() + "f2", file.path());

  // Create a dir with mkpath=true inside a link

  otherDir.setPath(dirLink.path() + "/d1/d2/d3");

  EXPECT_EQ(0, otherDir.create(-1, true));

  EXPECT_EQ(dir.path() + "d1/d2/d3/", otherDir.path());

  // Delete a link and check that its object is removed but not the target dir

  entries.clear();

  dir.update();

  EXPECT_EQ(0, dir.entryList(entries));

  EXPECT_EQ(0, otherDirLink.remove());

  dir.update();

  EXPECT_EQ(0, dir.entryList(entriesAfter));

  EXPECT_LT(entriesAfter.size(), entries.size());

  dir.update();

  EXPECT_TRUE(dir.exists());

  // Create link with a path to an existing file

  EXPECT_EQ(-EEXIST, dir.createLink(dir.path() + "f2"));

  // Create link with a path that has a file as intermediate path

  EXPECT_EQ(-ENOTDIR, dir.createLink(dir.path() + "f2" + "/newLink"));
}

TEST_F(RadosFsTest, LinkFile)
{
  AddPool();

  const std::string &linkName("fileLink");

  radosfs::File file(&radosFs, "/file",
                     radosfs::File::MODE_READ_WRITE);

  // Create a link to a file that doesn't exist

  EXPECT_EQ(-ENOENT, file.createLink(linkName));

  file.create();

  // Create a link to a file that exists

  EXPECT_EQ(0, file.createLink(linkName));

  radosfs::File fileLink(&radosFs, linkName,
                         radosfs::File::MODE_READ_WRITE);

  // Make a link of a link

  EXPECT_EQ(-EPERM, fileLink.createLink("linkOfALink"));

  // Call truncate on the link

  const int newSize = 1024;

  EXPECT_EQ(0, fileLink.truncate(newSize));

  // Verify the link

  EXPECT_TRUE(fileLink.exists());

  EXPECT_TRUE(fileLink.isFile());

  EXPECT_TRUE(fileLink.isLink());

  EXPECT_EQ(file.path(), fileLink.targetPath());

  struct stat buff;

  EXPECT_EQ(0, radosFs.stat(fileLink.path(), &buff));

  EXPECT_NE(0, buff.st_mode & S_IFLNK);

  EXPECT_EQ(0, buff.st_size);

  // Verify that truncate happened on the target dir

  EXPECT_EQ(0, radosFs.stat(file.path(), &buff));

  EXPECT_EQ(newSize, buff.st_size);

  // Write to link

  std::string text = "this is a link";
  char contents[1024];

  EXPECT_EQ(0, fileLink.write(text.c_str(), 0, text.length()));

  // Read from file and check contents

  EXPECT_EQ(text.length(), file.read(contents, 0, text.length()));
  contents[text.length()] = '\0';

  EXPECT_EQ(0, strcmp(contents, text.c_str()));

  // Verify that link's size hasn't changed

  EXPECT_EQ(0, radosFs.stat(fileLink.path(), &buff));

  EXPECT_EQ(0, buff.st_size);

  // Write to file

  text = "this is a file";

  EXPECT_EQ(0, file.write(text.c_str(), 0, text.length()));

  // Read from link and check contents

  EXPECT_EQ(text.length(), fileLink.read(contents, 0, text.length()));

  EXPECT_EQ(0, strcmp(contents, text.c_str()));

  // Remove file

  EXPECT_EQ(0, file.remove());

  // Re-start file link (make it drop the shared IO object)

  file.setPath("/fake");
  fileLink.setPath("/fake");

  file.setPath("/file");
  fileLink.setPath(linkName);

  EXPECT_FALSE(file.exists());

  EXPECT_TRUE(fileLink.exists());

  // Write to a link whose target doesn't exist

  EXPECT_EQ(-ENOLINK, fileLink.read(contents, 0, text.length()));

  EXPECT_EQ(-ENOLINK, fileLink.write(contents, 0, text.length()));

  // Delete a link and check that its object is removed but not the target file

  EXPECT_EQ(-ENOLINK, fileLink.remove());
}

TEST_F(RadosFsTest, LinkPermissions)
{
  AddPool();

  // Create user dir

  radosfs::Dir dir(&radosFs, "/user");

  EXPECT_EQ(0, dir.create(-1, false, TEST_UID, TEST_GID));

  // Create a dir as root

  dir.setPath("/dir");

  EXPECT_EQ(0, dir.create(S_IWUSR));

  // Create a dir link as user

  radosFs.setIds(TEST_UID, TEST_GID);

  std::string linkName = "/user/dirLink";

  EXPECT_EQ(0, dir.createLink(linkName));

  // Read the entries from the link as user

  radosfs::Dir dirLink(&radosFs, linkName);

  std::set<std::string> entries;

  EXPECT_EQ(-EACCES, dirLink.entryList(entries));

  // Read the entries from the link as root

  radosFs.setIds(ROOT_UID, ROOT_UID);

  EXPECT_EQ(0, dirLink.entryList(entries));

  // Create a file as root

  radosfs::File file(&radosFs, "/file",
                     radosfs::File::MODE_READ_WRITE);

  EXPECT_EQ(0, file.create(S_IWUSR));

  // Create a file link as user

  radosFs.setIds(TEST_UID, TEST_GID);

  linkName = "/user/fileLink";

  EXPECT_EQ(0, file.createLink(linkName));

  // Read the file contents through the link as user

  radosfs::File fileLink(&radosFs, linkName,
                         radosfs::File::MODE_READ_WRITE);

  char buff[] = {"X"};
  EXPECT_EQ(-EACCES, fileLink.read(buff, 0, 1));

  // Read the file contents through the link as root

  radosFs.setIds(ROOT_UID, ROOT_UID);

  fileLink.update();

  EXPECT_NE(-EACCES, fileLink.read(buff, 0, 1));

  // Write in the file through the link as root

  EXPECT_EQ(0, fileLink.write(buff, 0, 1));

  // Write in the file through the link as user

  radosFs.setIds(TEST_UID, TEST_UID);

  fileLink.update();

  EXPECT_EQ(-EACCES, fileLink.write(buff, 0, 1));
}

TEST_F(RadosFsTest, Find)
{
  AddPool();

  radosfs::Dir dir(&radosFs, "/");

  // Create files and directories

  const int numDirsPerLevel = 5;
  const int numFilesPerLevel = numDirsPerLevel / 2;
  const int levels = 3;

  int numDirs = 0;
  for (int i = levels; i > 0; i--)
    numDirs += pow(numDirsPerLevel, i);

  fprintf(stdout, "[ CREATING CONTENTS... ");

  EXPECT_EQ(0, createContentsRecursively("/",
                                         numDirsPerLevel,
                                         numDirsPerLevel / 2,
                                         levels));

  fprintf(stdout, "DONE]\n");

  std::set<std::string> results;

  dir.setPath("/");
  dir.update();

  // Find contents using an empty search string

  EXPECT_EQ(-EINVAL, dir.find(results, ""));

  // Find contents whose name begins with a "d" and measure its time
  // (all directories)

  struct timespec startTime, endTime;

  clock_gettime(CLOCK_REALTIME, &startTime);

  int ret = dir.find(results, "name=\"^d.*\"");

  clock_gettime(CLOCK_REALTIME, &endTime);

  double secsBefore = (double) startTime.tv_sec + NSEC_TO_SEC(startTime.tv_nsec);
  double secsAfter = (double) endTime.tv_sec + NSEC_TO_SEC(endTime.tv_nsec);

  fprintf(stdout, "[Searched %d directories in %.3f s]\n",
          numDirs, secsAfter - secsBefore);

  EXPECT_EQ(0, ret);

  EXPECT_EQ(numDirs, results.size());

  results.clear();

  // Find contents whose name begins with a "f" (all files)

  EXPECT_EQ(0, dir.find(results, "name=\"^f.*\""));

  int numFiles = 1;
  for (int i = levels - 1; i > 0; i--)
    numFiles += pow(numDirsPerLevel, i);

  numFiles *= numFilesPerLevel;

  EXPECT_EQ(numFiles, results.size());

  results.clear();

  // Find contents whose size is 0 (all files + dirs of the last level)

  EXPECT_EQ(0, dir.find(results, "size = 0"));

  EXPECT_EQ(numFiles + pow(numDirsPerLevel, levels), results.size());

  radosfs::File f(&radosFs, "/d0/d0/f0",
                  radosfs::File::MODE_READ_WRITE);

  EXPECT_EQ(0, f.truncate(100));

  f.setPath("/d0/d0/d0/newFile");

  EXPECT_EQ(0, f.create());

  EXPECT_EQ(0, f.truncate(100));

  results.clear();

  // Find contents whose size is 100 and name begins with "new"

  EXPECT_EQ(0, dir.find(results, "name=\"^new.*\" size = 100"));

  EXPECT_EQ(1, results.size());

  results.clear();

  // Find contents whose size is 100 and name begins with "f"

  EXPECT_EQ(0, dir.find(results, "name=\"^.*f.*\" size = 100"));

  EXPECT_EQ(1, results.size());

  results.clear();

  // Find contents whose size is 100

  EXPECT_EQ(0, dir.find(results, "size = 100"));

  EXPECT_EQ(2, results.size());

  results.clear();

  // Find contents whose size is 100 and the name contains an "f"

  EXPECT_EQ(0, dir.find(results, "iname='.*f.*' size = \"100\""));

  EXPECT_EQ(2, results.size());

  results.clear();

  // Find contents whose name contains a "0" but does not contain an "f"

  dir.setPath("/d0/d0/");

  EXPECT_EQ(0, dir.find(results, "name!=\"^.*f.*\" name='^.*0.*'"));

  EXPECT_EQ(1, results.size());
}

TEST_F(RadosFsTest, PoolAlignment)
{
  AddPool();

  const size_t alignment(3);
  const size_t stripeSize(128);
  const size_t alignedStripeSize = (stripeSize % alignment == 0) ?
                                     stripeSize :
                                     (stripeSize / alignment) * alignment;

  radosFs.setFileStripeSize(stripeSize);

  radosfs::File file(&radosFs, "/file");

  // Pretend the file is in an aligned pool

  radosFsFilePriv(file)->dataPool->alignment = alignment;

  file.update();

  // Create contents which should go into stripes with a size that is a multiple
  // of the alignment and less than the stripe size originally set

  EXPECT_EQ(0, file.create(-1, "", 0, 0));

  const size_t contentsSize(stripeSize * 3);
  char contents[contentsSize];
  memset(contents, 'x', contentsSize);

  EXPECT_EQ(0, file.writeSync(contents, 0, contentsSize));

  Stat stat;
  struct stat statBuff;

  EXPECT_EQ(0, radosFsPriv()->stat(file.path(), &stat));

  // Check the consistency of the contents written

  char readBuff[contentsSize];

  EXPECT_EQ(contentsSize, file.read(readBuff, 0, contentsSize));

  EXPECT_EQ(0, strncmp(contents, readBuff, contentsSize));

  radosfs::FileIO *fileIO = radosFsFilePriv(file)->getFileIO().get();
  ssize_t lastStripe = fileIO->getLastStripeIndex();

  u_int64_t size;

  // Get the size of the last stripe

  EXPECT_EQ(0, stat.pool->ioctx.stat(
                    makeFileStripeName(stat.translatedPath, lastStripe),
                    &size,
                    0));

  // Check the real stored size of the stripes

  EXPECT_EQ(alignedStripeSize, size);

  size_t totalStoredSize = (lastStripe + 1) * alignedStripeSize;

  EXPECT_EQ(totalStoredSize, lastStripe * fileIO->stripeSize() + size);

  // Check that the file size still reports the same as the contents' originally
  // set

  EXPECT_EQ(0, file.stat(&statBuff));

  EXPECT_EQ(contentsSize, statBuff.st_size);

  // Check that truncate (down and up) still make the stripes with the aligned
  // size and that the file still reports the expected truncated size

  EXPECT_EQ(0, file.truncate(contentsSize / 2));

  lastStripe = fileIO->getLastStripeIndex();

  EXPECT_EQ(0, stat.pool->ioctx.stat(
                    makeFileStripeName(stat.translatedPath, lastStripe).c_str(),
                    &size,
                    0));

  EXPECT_EQ(alignedStripeSize, size);

  EXPECT_EQ(0, file.stat(&statBuff));

  EXPECT_EQ(contentsSize / 2, statBuff.st_size);

  EXPECT_EQ(0, file.truncate(contentsSize * 2));

  EXPECT_EQ(0, file.stat(&statBuff));

  EXPECT_EQ(contentsSize * 2, statBuff.st_size);
}

TEST_F(RadosFsTest, DirTimes)
{
  AddPool();

  // Create a dir

  std::string dirPath = "/my-dir";
  radosfs::Dir dir(&radosFs, dirPath);

  ASSERT_EQ(0, dir.create());

  dir.update();

  // Check the creation and modification time

  struct stat statBuff;

  ASSERT_EQ(0, dir.stat(&statBuff));

  EXPECT_EQ(statBuff.st_ctim.tv_sec, statBuff.st_mtim.tv_sec);

  radosfs::File file(&radosFs, dir.path() + "file");

  // sleep for one sec before creating the file so the dir's mtime will be
  // significantly different
  sleep(1);

  // Create a file in the dir and see if it changed its modification time

  ASSERT_EQ(0, file.create());

  struct stat newStatBuff;

  ASSERT_EQ(0, dir.stat(&newStatBuff));

  EXPECT_LT(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  // Remove the file and see if it changed its modification time

  statBuff = newStatBuff;

  sleep(1);

  ASSERT_EQ(0, file.remove());

  ASSERT_EQ(0, dir.stat(&newStatBuff));

  EXPECT_LT(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  // Create a subdirectory and see if it changed its modification time

  sleep(1);

  radosfs::Dir subdir(&radosFs, dir.path() + "a/b/c");

  ASSERT_EQ(0, subdir.create(-1, true));

  statBuff = newStatBuff;

  ASSERT_EQ(0, dir.stat(&newStatBuff));

  EXPECT_LT(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  // Set the use of TM time in a sub directory

  sleep(1);

  dir.setPath(dirPath + "/a/b");

  ASSERT_EQ(0, dir.useTMTime(true));

  timespec oldTMTime, newTMTime;

  ASSERT_EQ(0, dir.stat(&statBuff, &oldTMTime));

  sleep(1);

  dir.setPath(dirPath + "/a");

  ASSERT_EQ(0, dir.stat(&statBuff, &oldTMTime));

  subdir.setPath(dirPath + "/a/b/c1");

  // Create a subdirectory of the when that has the TM time set but verify that
  // does not affect other parents

  ASSERT_EQ(0, subdir.create());

  ASSERT_EQ(0, dir.stat(&newStatBuff, &newTMTime));

  EXPECT_EQ(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  EXPECT_EQ(oldTMTime.tv_sec, newTMTime.tv_sec);

  // Set the TM time to yet another parent

  dir.setPath(dirPath + "/a");

  oldTMTime = newTMTime;

  ASSERT_EQ(0, dir.useTMTime(true));

  statBuff = newStatBuff;

  sleep(1);

  // Remove the previously created deeper subdir and verify how it affects
  // its grandparent's TM time

  ASSERT_EQ(0, subdir.remove());

  ASSERT_EQ(0, dir.stat(&newStatBuff, &newTMTime));

  EXPECT_EQ(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  EXPECT_LT(oldTMTime.tv_sec, newTMTime.tv_sec);

  subdir.setPath(dirPath + "/a/b");

  ASSERT_EQ(0, subdir.stat(&statBuff));

  // Set and remove metadata and see how it affects the times

  sleep(1);

  subdir.update();

  ASSERT_EQ(0, subdir.setMetadata("c/", "metadata", "value"));

  ASSERT_EQ(0, subdir.stat(&newStatBuff));

  EXPECT_LT(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  oldTMTime = newTMTime;

  ASSERT_EQ(0, dir.stat(&newStatBuff, &newTMTime));

  EXPECT_LT(oldTMTime.tv_sec, newTMTime.tv_sec);

  statBuff = newStatBuff;

  sleep(1);

  ASSERT_EQ(0, subdir.removeMetadata("c/", "metadata"));

  ASSERT_EQ(0, subdir.stat(&newStatBuff));

  EXPECT_LT(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  oldTMTime = newTMTime;

  ASSERT_EQ(0, dir.stat(&newStatBuff, &newTMTime));

  EXPECT_LT(oldTMTime.tv_sec, newTMTime.tv_sec);
}

TEST_F(RadosFsTest, FileTimes)
{
  AddPool();

  // Create a file

  radosfs::File file(&radosFs, "/my-file");

  ASSERT_EQ(0, file.create());

  // Check the creation and modification time

  struct stat statBuff, newStatBuff;

  ASSERT_EQ(0, file.stat(&statBuff));

  EXPECT_EQ(statBuff.st_ctim.tv_sec, statBuff.st_mtim.tv_sec);

  // Write to the file

  // Sleep to affect the tested times
  sleep(1);

  const std::string &contents = "CERN · 60 Years of Science of Peace!";

  ASSERT_EQ(0, file.write(contents.c_str(), 0, contents.length()));

  file.sync();

  ASSERT_EQ(0, file.stat(&newStatBuff));

  EXPECT_LT(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);

  // Truncate the file

  // Sleep to affect the tested times
  sleep(1);

  ASSERT_EQ(0, file.truncate(4));

  statBuff = newStatBuff;

  ASSERT_EQ(0, file.stat(&newStatBuff));

  EXPECT_LT(statBuff.st_mtim.tv_sec, newStatBuff.st_mtim.tv_sec);
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
      }
      else if (arg.find("--user=") == 0)
      {
	setenv(CONF_USR_VAR, arg.substr(arg.find('=') + 1).c_str(), 1);
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
