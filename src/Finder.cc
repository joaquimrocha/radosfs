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

#include <errno.h>
#include <regex.h>
#include <sys/stat.h>
#include <utility>

#include "radosfscommon.h"
#include "Finder.hh"
#include "Logger.hh"

#define DEFAULT_NUM_FINDER_THREADS 100
#define MAX_INACTIVE_THREAD_TIME 5

RADOS_FS_BEGIN_NAMESPACE

Finder::Finder(Filesystem *fs)
  : radosFs(fs)
{}

Finder::~Finder()
{}

int
makeRegexFromExp(const std::string &expression,
                 regex_t regex,
                 const std::string &entry)
{
  int ret = regexec(&regex, entry.c_str(), 0, 0, 0);

  if (ret != 0)
  {
    if (ret != REG_NOMATCH)
      radosfs_debug("Error looking for regex: %s", expression.c_str());
  }

  return ret;
}

int
checkEntryNameRegex(FinderData *data, const Finder::FindOptions option,
                    std::string &exp,
                    regex_t *regex)
{
  int ret = 0;
  int flags = 0;

  exp = data->args->at(option).valueStr;

  if (exp == "")
    return -EINVAL;

  if (data->args->at(option).valueInt == 1)
    flags = REG_ICASE;

  ret = regcomp(regex, exp.c_str(), flags);

  return ret;
}

int
checkEntryRegex(const std::string &exp, bool icase, regex_t *regex)
{
  int ret = 0;
  int flags = 0;

  if (exp == "")
    return -EINVAL;

  if (icase)
    flags = REG_ICASE;

  ret = regcomp(regex, exp.c_str(), flags);

  return ret;
}

int
Finder::checkEntrySize(FinderData *data, const std::string &entry,
                       const Dir &dir, struct stat &buff)
{
  int ret = 0;
  const int sizeOptions[] = {FIND_SIZE_EQ,
                             FIND_SIZE_NE,
                             FIND_SIZE_GE,
                             FIND_SIZE_GT,
                             FIND_SIZE_LE,
                             FIND_SIZE_LT,
                             0
                            };

  for (int i = 0; sizeOptions[i] != 0; i++)
  {
    FindOptions option = static_cast<FindOptions>(sizeOptions[i]);

    if (data->args->count(option) == 0)
      continue;

    off_t size = data->args->at(option).valueInt;

    if (buff.st_nlink == 0)
    {
      ret = radosFs->stat(dir.path() + entry, &buff);

      if (ret != 0)
      {
        radosfs_debug("Error stating %s", (dir.path() + entry).c_str());
        break;
      }
    }

    ret = -1;

    switch (option)
    {
      case FIND_SIZE_EQ:
        if (buff.st_size == size)
          ret = 0;
        break;
      case FIND_SIZE_GE:
        if (buff.st_size >= size)
          ret = 0;
        break;
      case FIND_SIZE_GT:
        if (buff.st_size > size)
          ret = 0;
        break;
      case FIND_SIZE_LE:
        if (buff.st_size <= size)
          ret = 0;
        break;
      case FIND_SIZE_LT:
        if (buff.st_size < size)
          ret = 0;
        break;
      default:
        break;
    }

    if (ret != 0)
      break;
  }

  return ret;
}

int
Finder::checkEntryMtd(FinderData *data, const std::string &entry,
                      Dir &dir)
{
  int ret = 0;
  const int mtdOptions[] = {FIND_MTD_EQ,
                            FIND_MTD_NE,
                            0
                           };

  for (int i = 0; mtdOptions[i] != 0; i++)
  {
    FindOptions option = static_cast<FindOptions>(mtdOptions[i]);

    if (data->args->count(option) == 0)
      continue;

    FinderArg arg = data->args->at(option);

    std::string mtdValue;
    bool checkingName = arg.key.empty();
    std::string key = checkingName ? arg.valueStr : arg.key;

    ret = dir.getMetadata(entry, key, mtdValue);

    if (ret == 0)
    {
      regex_t regex;
      ret = checkEntryRegex(arg.valueStr, arg.valueInt == 1, &regex);

      if (ret != 0)
      {
        radosfs_debug("Error making regex for %s on directory '%s'.",
                      arg.valueStr.c_str(), dir.path().c_str());
        return -EINVAL;
      }

      if (checkingName)
        ret = makeRegexFromExp(arg.valueStr, regex, key);
      else
        ret = makeRegexFromExp(arg.valueStr, regex, mtdValue);

      regfree(&regex);

      if ((option == FIND_MTD_EQ && ret == 0) ||
          (option == FIND_MTD_NE && ret == REG_NOMATCH))
      {
        ret = 0;
        continue;
      }

      ret = -1;
    }
    else if (checkingName && option == FIND_MTD_NE)
    {
      ret = 0;
    }

    if (ret != 0)
      break;
  }

  return ret;
}

int
Finder::checkXAttrKeyPresence(FinderArg &arg, FindOptions option,
                              const std::map<std::string, std::string> &xattrs)
{
  regex_t regex;
  int ret = checkEntryRegex(arg.valueStr, arg.valueInt == 1, &regex);

  if (ret != 0)
  {
    radosfs_debug("Error making regex for %s.",
                  arg.valueStr.c_str());
    return -EINVAL;
  }

  bool matched = false;

  std::map<std::string, std::string>::const_iterator it;
  for (it = xattrs.begin(); it != xattrs.end(); it++)
  {
    ret = makeRegexFromExp(arg.valueStr, regex, (*it).first);
    if (ret == 0)
    {
      matched = true;
      break;
    }
  }

  if ((matched && option == FIND_XATTR_EQ) ||
      (!matched && option == FIND_XATTR_NE))
    return 0;

  return -1;
}

int
Finder::checkEntryXAttrs(FinderData *data, const std::string &entry, Dir &dir)
{
  int ret = 0;
  const int mtdOptions[] = {FIND_XATTR_EQ,
                            FIND_XATTR_NE,
                            0
                           };

  for (int i = 0; mtdOptions[i] != 0; i++)
  {
    FindOptions option = static_cast<FindOptions>(mtdOptions[i]);

    if (data->args->count(option) == 0)
      continue;

    FinderArg arg = data->args->at(option);

    std::string mtdValue;
    bool checkingName = arg.key.empty();
    std::string key = checkingName ? arg.valueStr : arg.key;
    std::map<std::string, std::string> xattrs;

    if (checkingName)
    {
      radosFs->getXAttrsMap(dir.path() + entry, xattrs);
      ret = checkXAttrKeyPresence(arg, option, xattrs);
    }
    else
    {
      ret = radosFs->getXAttr(dir.path() + entry, key, mtdValue);

      if (ret >= 0)
      {
        regex_t regex;
        ret = checkEntryRegex(arg.valueStr, arg.valueInt == 1, &regex);

        if (ret != 0)
        {
          radosfs_debug("Error making regex for %s on directory '%s'.",
                        arg.valueStr.c_str(), dir.path().c_str());
          return -EINVAL;
        }

        ret = makeRegexFromExp(arg.valueStr, regex, mtdValue);

        regfree(&regex);

        if ((option == FIND_XATTR_EQ && ret == 0) ||
            (option == FIND_XATTR_NE && ret == REG_NOMATCH))
        {
          ret = 0;
          continue;
        }

        ret = -1;
      }
    }

    if (ret != 0)
      break;
  }

  return ret;
}

int
Finder::find(FinderData *data)
{
  int ret;
  std::string nameExpEQ = "", nameExpNE = "";
  regex_t nameRegexEQ, nameRegexNE;
  std::set<std::string> entries;
  Dir dir(radosFs, data->dir);
  std::set<std::string>::iterator it;

  dir.update();

  if (dir.isLink())
    return 0;

  if ((ret = dir.entryList(entries)) != 0)
    return ret;

  if (data->args->count(FIND_NAME_EQ))
  {
    ret = checkEntryNameRegex(data, FIND_NAME_EQ, nameExpEQ, &nameRegexEQ);

    if (ret != 0)
      goto bailout;
  }

  if (data->args->count(FIND_NAME_NE))
  {
    ret = checkEntryNameRegex(data,
                              FIND_NAME_NE,
                              nameExpNE,
                              &nameRegexNE);

    if (ret != 0)
      goto bailout;
  }

  for (it = entries.begin(); it != entries.end(); it++)
  {
    struct stat buff;
    buff.st_nlink = 0;

    const std::string &entry = *it;

    bool isDir = isDirPath(entry);

    if (isDir)
      data->dirEntries.insert(dir.path() + entry);

    if (nameExpEQ != "")
    {
      ret = makeRegexFromExp(nameExpEQ, nameRegexEQ, entry);

      if (ret != 0)
      {
        ret = 0;

        continue;
      }
    }

    if (nameExpNE != "" &&
        makeRegexFromExp(nameExpNE, nameRegexNE, entry) != REG_NOMATCH)
    {
        continue;
    }

    ret = checkEntrySize(data, entry, dir, buff);

    if (ret != 0)
    {
      ret = 0;
      continue;
    }

    ret = checkEntryMtd(data, entry, dir);

    if (ret != 0)
    {
      ret = 0;
      continue;
    }

    ret = checkEntryXAttrs(data, entry, dir);

    if (ret != 0)
    {
      ret = 0;
      continue;
    }

    data->results.insert(dir.path() + entry);
  }

bailout:
  if (nameExpEQ != "")
    regfree(&nameRegexEQ);

  if (nameExpNE != "")
    regfree(&nameRegexNE);

  return ret;
}

RADOS_FS_END_NAMESPACE
