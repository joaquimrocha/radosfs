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

#include <sys/stat.h>

#include "radosfsdefines.h"
#include "radosfscommon.h"
#include "File.hh"
#include "FileInode.hh"
#include "FileInodePriv.hh"
#include "FilePriv.hh"
#include "Dir.hh"
#include "FilesystemPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

FilePriv::FilePriv(File *fsFile, File::OpenMode mode)
  : fsFile(fsFile),
    target(0),
    permissions(File::MODE_NONE),
    mode(mode),
    inlineBufferSize(DEFAULT_FILE_INLINE_BUFFER_SIZE)
{
  FileInodePriv *mPriv = new FileInodePriv(fsFile->filesystem(), FileIOSP());
  inode = new FileInode(mPriv);

  updatePath();
}

FilePriv::~FilePriv()
{
  if (target)
    delete target;

  delete inode;
}

void
FilePriv::updateDataPool(const std::string &pool)
{
  dataPool = fsFile->filesystem()->mPriv->getDataPool(fsFile->path(), pool);
}

void
FilePriv::updatePath()
{
  Stat *stat = fsStat();

  parentDir = getParentDir(fsFile->path(), 0);

  Filesystem *radosFs = fsFile->filesystem();

  mtdPool = radosFs->mPriv->getMetadataPoolFromPath(fsFile->path());

  if (!mtdPool)
    return;

  updatePermissions();

  inode->mPriv->io.reset();

  if (!fsFile->exists())
  {
    updateDataPool("");

    return;
  }

  // Only set the pool if they differ. Besides saving it from wasting time in
  // getting the same assignments, getting the alignment when the data pool
  // differs allows us to use it directly in the unit tests to fake an arbitrary
  // alignment.
  if (stat->pool.get() != dataPool.get())
  {
    dataPool = stat->pool;
  }

  if (stat->extraData.count(XATTR_FILE_INLINE_BUFFER_SIZE) > 0)
  {
    const std::string &bufferSize =
        stat->extraData[XATTR_FILE_INLINE_BUFFER_SIZE];
    inlineBufferSize = strtoul(bufferSize.c_str(), 0, 10);
  }

  if (!dataPool)
    return;

  if (target)
  {
    delete target;
    target = 0;
  }

  if (fsFile->isLink())
  {
    target = new File(fsFile->filesystem(),
                      fsFile->targetPath(),
                      mode);
  }
  else if (stat && stat->translatedPath != "")
  {
    inode->mPriv->setFileIO(radosFs->mPriv->getOrCreateFileIO(stat->translatedPath,
                                                              stat));
    if (inlineBufferSize > 0)
      inode->mPriv->io->setInlineBuffer(fsFile->path(), inlineBufferSize);
  }
}

int
FilePriv::verifyExistanceAndType()
{
  if (fsFile->isLink() && !target->exists())
    return -ENOLINK;

  if (!fsFile->exists())
    return -ENOENT;

  if (!fsFile->isFile())
    return -EISDIR;

  return 0;
}

void
FilePriv::updatePermissions()
{
  permissions = File::MODE_NONE;
  Stat *parentStat, *fileStat;

  if (!mtdPool.get())
    return;

  parentStat = reinterpret_cast<Stat *>(fsFile->parentFsStat());

  if (!parentStat)
    return;

  parentDir = parentStat->path;

  uid_t uid;
  gid_t gid;

  fsFile->filesystem()->getIds(&uid, &gid);

  bool canWriteParent =
      statBuffHasPermission(parentStat->statBuff, uid, gid, O_WRONLY);

  bool canReadParent =
      statBuffHasPermission(parentStat->statBuff, uid, gid, O_RDONLY);

  fileStat = fsStat();

  if (fsFile->exists() && !fsFile->isFile())
    return;

  if (canWriteParent && (mode & File::MODE_WRITE))
  {
    if (!fsFile->exists() ||
        statBuffHasPermission(fileStat->statBuff, uid, gid, O_WRONLY))
      permissions =
          (File::OpenMode) (permissions | File::MODE_WRITE);
  }

  if (canReadParent && (mode & File::MODE_READ) &&
      statBuffHasPermission(fileStat->statBuff, uid, gid, O_RDONLY))
  {
    permissions = (File::OpenMode) (permissions | File::MODE_READ);
  }
}

int
FilePriv::removeFile()
{
  if (!getFileIO())
    return 0;

  if (!FileIO::hasSingleClient(inode->mPriv->io))
    getFileIO()->setLazyRemoval(true);
  else
  {
    int ret = getFileIO()->remove();

    // Ignore the fact that the inode might not exist because we're also dealing
    // with the logical file name
    if (ret == -ENOENT)
      return 0;
  }

  return 0;
}

Stat *
FilePriv::fsStat(void)
{
  return reinterpret_cast<Stat *>(fsFile->fsStat());
}

int
FilePriv::rename(const std::string &destination)
{
  int index;
  int ret;
  Stat stat, parentStat;
  std::string destParent = getParentDir(destination, &index);
  std::string baseName;

  if (destParent != "")
  {
    baseName = destination.substr(index);
  }

  if (destParent == parentDir)
  {
    parentStat = *reinterpret_cast<Stat *>(fsFile->parentFsStat());
  }
  else
  {
    uid_t uid;
    gid_t gid;
    std::string realParentPath;

    ret = fsFile->filesystem()->mPriv->getRealPath(destParent, &parentStat,
                                                   realParentPath);

    if (ret == 0 && S_ISLNK(parentStat.statBuff.st_mode))
    {
      destParent = parentStat.translatedPath;
      ret = fsFile->filesystem()->mPriv->stat(destParent,
                                              &parentStat);
    }
    else
    {
      destParent = realParentPath;
    }

    if (ret != 0)
    {
      radosfs_debug("Problem statting destination's parent when moving %s: %s",
                    destination.c_str(), strerror(-ret));
      return ret;
    }

    fsFile->filesystem()->getIds(&uid, &gid);

    if (!statBuffHasPermission(parentStat.statBuff, uid, gid, O_WRONLY))
    {
      radosfs_debug("No permissions to write in parent dir when moving %s",
                    destination.c_str());
      return -EACCES;
    }
  }

  const std::string &newPath = destParent + baseName;

  ret = fsFile->filesystem()->mPriv->stat(newPath, &stat);

  if (ret == 0)
  {
    if (S_ISDIR(stat.statBuff.st_mode))
      return -EISDIR;

    if (newPath == fsFile->path())
      return -EPERM;
  }
  else if (ret != -ENOENT)
  {
    return ret;
  }

  stat = *fsStat();
  stat.path = newPath;

  Stat *oldParentStat = reinterpret_cast<Stat *>(fsFile->parentFsStat());

  ret = moveLogicalFile(*oldParentStat, parentStat, fsFile->path(), newPath);

  if (ret != 0)
    return ret;

  inode->mPriv->io->setPath(newPath);

  std::string oldPath = fsFile->path();
  inode->mPriv->io->updateBackLink(&oldPath);

  fsFile->setPath(newPath);

  return ret;
}

void
FilePriv::setInode(const size_t chunkSize)
{
  size_t chunk= alignChunkSize(chunkSize, dataPool->alignment);
  FileIOSP fileIO = FileIOSP(new FileIO(fsFile->filesystem(),
                                        dataPool,
                                        generateUuid(),
                                        fsFile->path(),
                                        chunk));
  inode->mPriv->setFileIO(fileIO);

  if (inlineBufferSize > 0)
    inode->mPriv->io->setInlineBuffer(fsFile->path(), inlineBufferSize);
}

int
FilePriv::create(int mode, uid_t uid, gid_t gid, size_t chunk, Stat *fileStatRet)
{
  setInode(chunk ? chunk : fsFile->filesystem()->fileChunkSize());
  Stat *parentStat = parentFsStat();

  int ret = inode->mPriv->registerFileWithStats(fsFile->path(), uid, gid, mode,
                                                inlineBufferSize, *parentStat,
                                                fileStatRet);

  getFsPriv()->updateTMId(fsStat());

  return ret;
}

/**
 * @class File
 *
 * Represents a file in the filesystem.
 *
 * This class is used to manage the most common file operations such as creation,
 * removal, reading, writing, etc.
 *
 * @enum File::OpenMode
 *
 * The open mode for the file. Even though files do not have an "open" method,
 * this mode offers an extra protection as it may restrict which operations are
 * allowed on the File instance.
 *
 * @note The open mode does not bypass the file's permissions for the current
 * user and group id. It works in addition to checking the permissions.
 *
 * @var File::OpenMode File::MODE_NONE
 *      Do not allow any read or write operations.
 *
 * @var File::OpenMode File::MODE_READ
 *      Allow read operations.
 *
 * @var File::OpenMode File::MODE_WRITE
 *      Allow write operations only.
 *
 * @var File::OpenMode File::MODE_READ_WRITE
 *      Allow read and write operations.
 */

/**
 * Creates an new instance of File.
 *
 * @param radosFs a pointer to the Filesystem that contains this file.
 * @param path an absolute file path.
 * @param mode the mode for instantiating the file (see File::OpenMode).
 */
File::File(Filesystem *radosFs, const std::string &path, File::OpenMode mode)
  : FsObj(radosFs, getFilePath(path)),
    mPriv(new FilePriv(this, mode))
{}

File::~File()
{
  delete mPriv;
}

/**
 * Copy constructor for creating a file instance.
 *
 * @param otherFile a reference to a File instance.
 */
File::File(const File &otherFile)
  : FsObj(otherFile),
    mPriv(new FilePriv(this, otherFile.mode()))
{}

/**
 * Copy assignment operator.
 *
 * @param otherFile a reference to a File instance.
 * @return a reference to a File.
 */
File &
File::operator=(const File &otherFile)
{
  if (this != &otherFile)
  {
    this->mPriv->mode = otherFile.mode();
    this->setPath(otherFile.path());
  }

  return *this;
}

/**
 * Gets the File::OpenMode that was used to create this file.
 *
 * @return a File::OpenMode.
 */
File::OpenMode
File::mode() const
{
  return mPriv->mode;
}

/**
 * Reads the file's contents into \a buff (synchronously).
 *
 * @param buff a char buffer. Its size has to be big enough for the contents
 *        being read (at least \a blen).
 * @param offset the offset in the file's contents from which to start reading
 *        (in bytes).
 * @param blen the maximum number of bytes to read from the file's contents
 *        (in bytes).
 * @return The number of bytes read (it might differ from \a blen as it only
 *         reads the file up to its contents' size) on success, or a negative
 *         error code otherwise.
 */
ssize_t
File::read(char *buff, off_t offset, size_t blen)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  ret = -EACCES;

  if (mPriv->permissions & File::MODE_READ)
  {
    if (isLink())
      return mPriv->target->read(buff, offset, blen);

    ret = mPriv->inode->read(buff, offset, blen);

    // If there is no inode object, we treat the file as having a size of 0
    // instead
    if (ret == -ENOENT)
    {
      ret = 0;
    }
  }

  return ret;
}

/**
 * Reads multiple chunks of the file contents asynchronously.
 *
 * @see FileReadData.
 * @param intervals a vector of FileReadData objects describing which portions
 *        of the file to read.
 * @param[out] asyncOpId a string location to return the id of the
 *             asynchonous operation (or a null pointer in case none should be
 *             returned).
 * @param callback a function to be called upon the end of the read operation.
 * @param callbackArg a pointer representing user-defined argumetns, to be
 *        passed to the \a callback.
 * @return 0 if the operation was initialized, an error code otherwise.
 */
int
File::read(const std::vector<FileReadData> &intervals, std::string *asyncOpId,
           AsyncOpCallback callback, void *callbackArg)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  ret = -EACCES;

  if (mPriv->permissions & File::MODE_READ)
  {
    if (isLink())
      return mPriv->target->read(intervals, asyncOpId, callback, callbackArg);

    ret = mPriv->inode->read(intervals, asyncOpId, callback, callbackArg);
  }

  return ret;
}

/**
 * Write the contens of \a buff to the file asynchronously.
 *
 * @param buff a char buffer.
 * @param offset the offset in the file to write the new contents.
 * @param blen the number of bytes (from the \a buff) that should be written.
 * @param copyBuffer whether \a buff should be copied or not.
 * @param asyncOpId a string location to return the asynchronous operation's
 *        address or a null pointer if this is not desired.
 * @param callback an AsyncOpCallback to be called after the asynchronous
 *        operation is finished
 * @param callbackArg a pointer to the user arguments that will be passed to the
 *        \a callback .
 * @return 0 if the operation was initialized, an error code otherwise.
 */
int
File::write(const char *buff, off_t offset, size_t blen, bool copyBuffer,
            std::string *asyncOpId, AsyncOpCallback callback,
            void *callbackArg)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (mPriv->permissions & File::MODE_WRITE)
  {
    if (isLink())
      return mPriv->target->write(buff, offset, blen, copyBuffer, asyncOpId,
                                  callback, callbackArg);

    ret = mPriv->inode->write(buff, offset, blen, copyBuffer, asyncOpId,
                               callback, callbackArg);

    mPriv->getFsPriv()->updateTMId(mPriv->fsStat());

    return ret;
  }

  return -EACCES;
}

/**
 * Write the contens of \a buff to the file synchronously.
 *
 * @param buff a char buffer.
 * @param offset the offset in the file to write the new contents.
 * @param blen the number of bytes (from the \a buff) that should be written.
 * @return 0 on success, an error code otherwise.
 */
int
File::writeSync(const char *buff, off_t offset, size_t blen)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (mPriv->permissions & File::MODE_WRITE)
  {
    if (isLink())
      return mPriv->target->writeSync(buff, offset, blen);

    ret = mPriv->inode->writeSync(buff, offset, blen);

    mPriv->getFsPriv()->updateTMId(mPriv->fsStat());

    return ret;
  }

  return -EACCES;
}

/**
 * Creates the file object in the system.
 * This method has to be called for the file object to be actually created (and
 * before calling any operations on the file, like read/write).
 *
 * @param mode the file mode bits (from sys/stat.h), S_IRWXU, etc. Use -1 for
 *        the default (S_IRWXU | S_IRGRP | S_IROTH).
 * @param pool the data pool where the file's object should be stored. Use this
 *        argument to override the default behavior of getting the pool that has
 *        been mapped for the file's prefix in Filesystem.
 * @param stripe the size of the stripes for this file (in bytes). Use this
 *        argument to override the default size for the file stripes (which is
 *        128 MB).
 * @param inlineBufferSize the size for the file's inline buffer. Use this
 *        argument to override the default inline buffer's size (which is
 *        128 KB).
 *
 * @note If a value of 0 is given to \a inlineBufferSize, then no inline buffer
 *       will be used and file stripes will always be created when writing to
 *       it.
 * @return 0 on success, an error code otherwise.
 */
int
File::create(int mode, const std::string pool, size_t chunk,
             ssize_t inlineBufferSize)
{
  int ret;

  if (mPriv->dataPool.get() == 0)
    return -ENODEV;

  Stat *parentStat = mPriv->parentFsStat();
  if (!parentStat || !parentStat->pool)
    return -ENOENT;

  if (pool != "")
    mPriv->updateDataPool(pool);

  // we don't allow object names that end in a path separator
  const std::string filePath = path();
  if ((exists() && !isFile()) ||
      (filePath != "" && isDirPath(filePath)))
    return -EISDIR;

  if (exists())
  {
    if (mPriv->getFileIO() && mPriv->getFileIO()->lazyRemoval())
    {
      mPriv->getFileIO()->setLazyRemoval(false);
    }
    else
    {
      return -EEXIST;
    }
  }

  if ((mPriv->permissions & File::MODE_WRITE) == 0)
    return -EACCES;

  if (inlineBufferSize > MAX_FILE_INLINE_BUFFER_SIZE)
  {
    radosfs_debug("Error: Cannot create a file with an inline size > %u. The "
                  "given size was %u.", MAX_FILE_INLINE_BUFFER_SIZE,
                  inlineBufferSize);
    return -EINVAL;
  }

  uid_t uid;
  gid_t gid;

  filesystem()->getIds(&uid, &gid);

  if (inlineBufferSize > -1)
    mPriv->inlineBufferSize = inlineBufferSize;

  Stat fileStat;
  ret = mPriv->create(mode, uid, gid, chunk, &fileStat);

  if (ret == 0)
  {
    setFsStat(&fileStat);
    setExists(true);
    mPriv->updatePath();
  }

  return ret;
}

/**
 * Removes the file.
 *
 * @return 0 on success, an error code otherwise.
 */
int
File::remove()
{
  int ret;

  FsObj::update();

  ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  uid_t uid;
  gid_t gid;

  Stat *stat = mPriv->fsStat();

  filesystem()->getIds(&uid, &gid);

  if (statBuffHasPermission(stat->statBuff, uid, gid,
                            O_WRONLY | O_RDWR))
  {
    ret = mPriv->removeFile();
    Stat *parentStat = reinterpret_cast<Stat *>(parentFsStat());
    indexObject(parentStat, stat, '-');

    mPriv->getFsPriv()->updateTMId(mPriv->fsStat());
  }
  else
    return -EACCES;

  FsObj::update();

  return ret;
}

/**
 * Truncates the file.
 *
 * @param size the new size for the file.
 * @return 0 on success, an error code otherwise.
 */
int
File::truncate(unsigned long long size)
{
  int ret;
  if ((ret = mPriv->verifyExistanceAndType()) != 0)
    return ret;

  if (isLink())
    return mPriv->target->truncate(size);

  uid_t uid;
  gid_t gid;
  Stat *stat = mPriv->fsStat();

  filesystem()->getIds(&uid, &gid);

  if (!statBuffHasPermission(stat->statBuff, uid, gid,
                             O_WRONLY | O_RDWR))
  {
    return -EACCES;
  }

  ret = mPriv->inode->truncate(size);

  mPriv->getFsPriv()->updateTMId(mPriv->fsStat());

  return ret;
}

/**
 * Checks whether the file is writable. This takes into account the file's
 * permissions and the current File::OpenMode).
 *
 * @note This operation works over the information that this instance has of the
 *       permissions and the File::OpenMode, it does not get the latest values
 *       from the object in the cluster. For updating those values, File::update
 *       has to be called.
 *
 * @return true is the file is writable, false otherwise.
 */
bool
File::isWritable()
{
  return (mPriv->permissions & File::MODE_WRITE) != 0;
}

/**
 * Checks whether the file is readable. This takes into account the file's
 * permissions and the current File::OpenMode).
 *
 * @note This operation works over the information that this instance has of the
 *       permissions and the File::OpenMode, it does not get the latest values
 *       from the object in the cluster. For updating those values, File::update
 *       has to be called.
 *
 * @return true is the file is readable, false otherwise.
 */
bool
File::isReadable()
{
  return (mPriv->permissions & File::MODE_READ) != 0;
}

/**
 * Updates the state of this File instance according to the status of the actual
 * file object in the system.
 *
 * Traditionally this could be considered as reopening the file. It is used to
 * get the latest status of the file in the system. E.g. if a file instance has
 * been used for a long time and the status of the permissions, existance, etc.
 * needs to be checked, update should be called before checking it.
 */
void
File::update()
{
  FsObj::update();
  mPriv->updatePath();
}

/**
 * Changes the file object that this File instance refers to. This works as if
 * instantiating the file again using a different path.
 *
 * @param path a path to a file.
 */
void
File::setPath(const std::string &path)
{
  std::string filePath = getFilePath(path);

  FsObj::setPath(filePath);
}

/**
 * Stats the file.
 *
 * @param[out] buff a stat struct to fill with the details of the file.
 * @return 0 on success, an error code otherwise.
 */
int
File::stat(struct stat *buff)
{
  Stat *stat;

  FsObj::update();

  if (!exists())
    return -ENOENT;

  stat = mPriv->fsStat();


  if (isLink())
  {
    getTimeFromXAttr(stat, XATTR_MTIME, &stat->statBuff.st_mtim,
                     &stat->statBuff.st_mtime);
  }
  else
  {
    // If there is an inline buffer and it is not fully filled, then it already
    // gives us the size of the object, otherwise, we need to check the size set
    // in the inode

    std::string inlineContents;
    FileInlineBuffer *inlineBuffer = mPriv->getFileIO()->inlineBuffer();

    if (inlineBuffer)
    {
      inlineBuffer->read(&stat->statBuff.st_mtim, &inlineContents);

      stat->statBuff.st_mtime = stat->statBuff.st_mtim.tv_sec;
      stat->statBuff.st_size = inlineContents.length();
    }

    if (!inlineBuffer || inlineContents.length() == inlineBuffer->capacity())
    {
      size_t fileIOSize = mPriv->getFileIO()->getSize();

      if (fileIOSize != 0)
        stat->statBuff.st_size = fileIOSize;

      getTimeFromXAttr(stat, XATTR_MTIME, &stat->statBuff.st_mtim,
                       &stat->statBuff.st_mtime);
    }
  }

  *buff = stat->statBuff;

  return 0;
}

/**
 * Changes the permissions of the file.
 *
 * @param permissions the new permissions (mode bits from sys/stat.h).
 * @return 0 on success, an error code otherwise.
 */
int
File::chmod(long int permissions)
{
  long int mode;

  if (!exists())
    return -ENOENT;

  mode = permissions | S_IFREG;

  Stat fsStat = *mPriv->fsStat();
  Stat *parentStat = reinterpret_cast<Stat *>(parentFsStat());

  uid_t uid = filesystem()->uid();

  if (uid != ROOT_UID && fsStat.statBuff.st_uid != uid)
    return -EPERM;

  fsStat.statBuff.st_mode = mode;
  const std::string &baseName = path().substr(mPriv->parentDir.length());
  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_FILE_PREFIX + baseName].append(getFileXAttrDirRecord(&fsStat));

  return mPriv->mtdPool->ioctx.omap_set(parentStat->translatedPath, omap);
}

/**
 * Sets the owner uid and gid of the file.
 *
 * @note This function can only be used by *root* (the uid of Filesystem needs
 *       to be the root's, see Filesystem::setIds).
 *
 * @param uid a user id.
 * @param gid a group id.
 * @return 0 on success, an error code otherwise.
 */
int
File::chown(uid_t uid, gid_t gid)
{
  if (!exists())
    return -ENOENT;

  uid_t currentUid = filesystem()->uid();

  if (currentUid != ROOT_UID)
    return -EPERM;

  Stat fsStat = *mPriv->fsStat();
  fsStat.statBuff.st_uid = uid;
  fsStat.statBuff.st_gid = gid;

  std::string fileEntry = getFileXAttrDirRecord(&fsStat);
  const std::string &baseName = path().substr(mPriv->parentDir.length());
  std::map<std::string, librados::bufferlist> omap;
  omap[XATTR_FILE_PREFIX + baseName].append(fileEntry);

  Stat parentStat = *reinterpret_cast<Stat *>(parentFsStat());

  return parentStat.pool->ioctx.omap_set(parentStat.translatedPath, omap);
}

/**
 * Sets the owner uid of the file.
 *
 * @note This function can only be used by *root* (the uid of Filesystem needs
 *       to be the root's, see Filesystem::setIds).
 *
 * @param uid a user id.
 * @return 0 on success, an error code otherwise.
 */
int
File::setUid(uid_t uid)
{
  if (!exists())
    return -ENOENT;

  Stat fsStat = *mPriv->fsStat();

  return chown(uid, fsStat.statBuff.st_gid);
}

/**
 * Sets the owner gid of the file.
 *
 * @note This function can only be used by *root* (the uid of Filesystem needs
 *       to be the root's, see Filesystem::setIds).
 *
 * @param gid a group id.
 * @return 0 on success, an error code otherwise.
 */
int
File::setGid(gid_t gid)
{
  if (!exists())
    return -ENOENT;

  Stat fsStat = *mPriv->fsStat();

  return chown(fsStat.statBuff.st_uid, gid);
}

/**
 * Renames or moves the file.
 *
 * @param newPath the new path or name of the file.
 * @note  If \a newPath is a relative path, then it will be appended to the
 *        current parent directory.
 *
 *        This is a rename operation so the new path cannot be an existing file.
 *        In the same way, if a directory path is given, the file will not be
 *        moved into that directory.
 * @return 0 on success, an error code otherwise.
 */
int
File::rename(const std::string &newPath)
{
  int ret;

  ret = mPriv->verifyExistanceAndType();

  if (ret != 0)
    return ret;

  if (newPath == "")
    return -EINVAL;

  if (!isWritable())
    return -EACCES;

  std::string dest = newPath;

  if (dest[0] != PATH_SEP)
  {
    dest = getParentDir(path(), 0) + dest;
  }

  if (dest == "/")
    return -EISDIR;

  dest = getFilePath(sanitizePath(dest));

  return mPriv->rename(dest);
}

/**
 * Waits for the file's asynchronous operations to be finished. If \a opId is
 * given, it waits only for the operation with the matching id, otherwise it
 * does it for every single operation.
 *
 * @param opId the id of an asynchronous operation.
 * @return 0 on success, an error code otherwise.
 */
int
File::sync(const std::string &opId)
{
  int ret = mPriv->inode->sync(opId);

  if (ret == -ENODEV)
    ret = 0;

  return ret;
}

/**
 * Gets the inline buffer's size currently set for the file.
 *
 * @return 0 on success, an error code otherwise.
 */
size_t
File::inlineBufferSize(void) const
{
  return mPriv->inlineBufferSize;
}

RADOS_FS_END_NAMESPACE
