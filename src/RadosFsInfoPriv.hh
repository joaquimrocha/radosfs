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

#ifndef RADOS_FS_INFO_IMPL_HH
#define RADOS_FS_INFO_IMPL_HH

#include <tr1/memory>
#include <rados/librados.h>

#include "radosfscommon.h"
#include "radosfsdefines.h"
#include "RadosFsPriv.hh"

RADOS_FS_BEGIN_NAMESPACE

class RadosFs;
class RadosFsInfo;

class RadosFsInfoPriv
{
public:
  RadosFsInfoPriv(RadosFs *radosFs, const std::string &path);
  ~RadosFsInfoPriv();

  void setPath(const std::string &path);

  int makeLink(std::string &path);

  int makeRealPath(std::string &path, rados_ioctx_t *ioctxOut = 0);

  RadosFsPriv * radosFsPriv(void) const { return radosFs->mPriv; }

  std::string path;
  RadosFs *radosFs;
  RadosFsInfo *target;
  rados_ioctx_t ioctx;
  RadosFsStat stat;
  bool exists;
};

RADOS_FS_END_NAMESPACE

#endif /* RADOS_FS_INFO_IMPL_HH */
