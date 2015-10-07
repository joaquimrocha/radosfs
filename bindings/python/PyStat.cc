/*
 * PyStat.cc
 *
 *  Created on: Oct 5, 2015
 *      Author: simonm
 */

#include "PyStat.hh"

#include <boost/python.hpp>

namespace py = boost::python;

void radosfs::PyStat::export_bindings()
{
  py::class_<PyStat>( "stat", py::init<PyStat>() )
      .add_property( "st_mode",  &PyStat::getMode )
      .add_property( "st_ino",   &PyStat::getINo )
      .add_property( "st_dev",   &PyStat::getDev )
      .add_property( "st_nlink", &PyStat::getNLink )
      .add_property( "st_uid",   &PyStat::getUid )
      .add_property( "st_gid",   &PyStat::getGid )
      .add_property( "st_size",  &PyStat::getSize )
      .add_property( "st_atime",  &PyStat::getATime )
      .add_property( "st_mtime",  &PyStat::getMTime )
      .add_property( "st_ctime",  &PyStat::getCTime )
  ;
}
