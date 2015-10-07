/*
 * PyStat.hh
 *
 *  Created on: Oct 5, 2015
 *      Author: simonm
 */

#ifndef BINDINGS_PYTHON_PYSTAT_HH_
#define BINDINGS_PYTHON_PYSTAT_HH_

#include "Filesystem.hh"


RADOS_FS_BEGIN_NAMESPACE

class PyStat
{
  public:

    PyStat(const struct stat &st) : st( st ) {}

    PyStat(const PyStat & stat) : st( stat.st ) {}

    virtual ~PyStat() {}

    mode_t getMode()
    {
      return st.st_mode;
    }

    ino_t getINo()
    {
      return st.st_ino;
    }

    dev_t getDev()
    {
      return st.st_dev;
    }

    nlink_t getNLink()
    {
      return st.st_nlink;
    }

    uid_t getUid()
    {
      return st.st_uid;
    }

    gid_t getGid()
    {
      return st.st_gid;
    }

    off_t getSize()
    {
      return st.st_size;
    }

    double getATime()
    {
      return double(st.st_atim.tv_sec) + double(st.st_atim.tv_nsec) / 1000000000.0;
    }

    double getMTime()
    {
      return double(st.st_mtim.tv_sec) + double(st.st_mtim.tv_nsec) / 1000000000.0;
    }

    double getCTime()
    {
      return double(st.st_ctim.tv_sec) + double(st.st_ctim.tv_nsec) / 1000000000.0;
    }

    static void export_bindings();

  private:
    const struct stat st;
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_PYSTAT_HH_ */
