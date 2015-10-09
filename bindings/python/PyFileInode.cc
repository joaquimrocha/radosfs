/*
 * PyFileInode.cc
 *
 *  Created on: Oct 7, 2015
 *      Author: simonm
 */

#include "PyFileInode.hh"
#include "PyFilesystem.hh"

#include <utility>
#include "ReadWriteHelper.h"


void radosfs::PyFileInode::export_bindings()
{
  py::class_<PyFileInode>("FileInode", py::init<PyFilesystem&, py::str>())
      // other constructors
      .def( py::init<PyFilesystem&, py::str, py::str>() )
      .def( py::init<PyFilesystem&, py::str, py::str, size_t>() )
      .def( py::init<PyFilesystem&, py::str, size_t>() )
      // 'read' method
      .def( "read", &PyFileInode::read )
      .def( "read", &PyFileInode::read1 )
      .def( "read", &PyFileInode::read2 )
      .def( "read", &PyFileInode::read3 )
      // 'write' method
      .def( "write", &PyFileInode::write5 )
      .def( "write", &PyFileInode::write4 )
      .def( "write", &PyFileInode::write3 )
      .def( "write", &PyFileInode::write2 )
      // 'writeSync' method
      .def( "writeSync", &PyFileInode::writeSync )
      // 'remove' method
      .def( "remove", &PyFileInode::remove )
      // 'truncate' method
      .def( "truncate", &PyFileInode::truncate )
      // 'sync' method
      .def( "sync", &PyFileInode::sync1 )
      .def( "sync", &PyFileInode::sync0 )
      // 'name' method
      .def( "name", &PyFileInode::name )
      // 'registerFile' method
      .def( "registerFile", &PyFileInode::registerFile4 )
      .def( "registerFile", &PyFileInode::registerFile3 )
      // 'getBackLink' method
      .def( "getBackLink", &PyFileInode::getBackLink)
  ;
}
