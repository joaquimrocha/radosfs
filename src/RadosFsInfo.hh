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

#ifndef RADOS_FS_INFO_HH
#define RADOS_FS_INFO_HH

#include <cstdlib>
#include <set>
#include <memory>

#include "RadosFs.hh"

RADOS_FS_BEGIN_NAMESPACE

class RadosFsInfoPriv;

class RadosFsInfo
{
public:
  RadosFsInfo(RadosFs *radosFs, const std::string &path);

  virtual ~RadosFsInfo();

  RadosFsInfo(const RadosFsInfo &otherInfo);

  virtual bool isWritable(void) = 0;

  virtual bool isReadable(void) = 0;

  virtual std::string path(void) const;

  virtual void setPath(const std::string &path);

  virtual RadosFs *filesystem(void) const;

  virtual void setFilesystem(RadosFs *radosFs);

  virtual bool isFile(void) const;

  virtual bool isDir(void) const;

  virtual bool exists(void) const;

  virtual int stat(struct stat *buff);

  virtual void update(void);

  virtual int setXAttr(const std::string &attrName,
               const std::string &value);

  virtual int getXAttr(const std::string &attrName,
               std::string &value,
               size_t length);

  virtual int removeXAttr(const std::string &attrName);

  virtual int getXAttrsMap(std::map<std::string, std::string> &map);

  virtual int setMetadata(const std::string &key, const std::string &value);

private:
  std::auto_ptr<RadosFsInfoPriv> mPriv;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_INFO_HH */
