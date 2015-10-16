/*
 * PyFile.cc
 *
 *  Created on: Oct 9, 2015
 *      Author: simonm
 */

#include "PyFile.hh"
#include "PyFilesystem.hh"


radosfs::PyFile::PyFile(PyFilesystem &radosFs, const py::str &path) : File( &radosFs, py::extract<std::string>( path ) ) {}

radosfs::PyFile::PyFile(PyFilesystem &radosFs, const py::str &path, OpenMode mode) : File( &radosFs, py::extract<std::string>( path ), mode ) {}


void radosfs::PyFile::export_bindings()
{
  py::enum_<File::OpenMode>( "OpenMode" )
      .value( "MODE_NONE",  MODE_NONE )
      .value( "MODE_READ", MODE_READ )
      .value( "MODE_WRITE", MODE_WRITE )
      .value( "MODE_READ_WRITE", MODE_READ_WRITE )
  ;

  py::class_<PyFile>( "File", py::init<PyFilesystem&, py::str, OpenMode>() )
    // copy constructor
    .def( py::init<PyFile&>() )
    // 'mode' method
    .def( "mode",             &PyFile::mode)
    // 'read' method
    .def( "read",             &PyFile::read )
    .def( "read",             &PyFile::read3 )
    .def( "read",             &PyFile::read2 )
    .def( "read",             &PyFile::read1 )
    // 'write' method
    .def( "write",            &PyFile::write5 )
    .def( "write",            &PyFile::write4 )
    .def( "write",            &PyFile::write3 )
    .def( "write",            &PyFile::write2 )
    // 'writeSync' method
    .def( "writeSync",        &PyFile::writeSync )
    // 'create' method
    .def( "create",           &PyFile::create4 )
    .def( "create",           &PyFile::create3 )
    .def( "create",           &PyFile::create2 )
    .def( "create",           &PyFile::create1 )
    .def( "create",           &PyFile::create0 )
    // 'remove' method
    .def( "remove",           &PyFile::remove )
    // 'truncate' method
    .def( "truncate",         &PyFile::truncate )
    // 'isWritable' method
    .def( "isWritable",       &PyFile::isWritable )
    // 'isReadable' method
    .def( "isReadable",       &PyFile::isReadable )
    // 'refresh' method
    .def( "refresh",          &PyFile::refresh )
    // 'setPath' method
    .def( "setPath",          &PyFile::setPath )
    // 'stat' method
    .def( "stat",             &PyFile::stat )
    // 'chmod' method
    .def( "chmod",            &PyFile::chmod )
    // 'chown' method
    .def( "chown",            &PyFile::chown )
    // 'setUid' method
    .def( "setUid",           &PyFile::setUid )
    // 'setGid' method
    .def( "setGid",           &PyFile::setGid )
    // 'rename' method
    .def( "rename",           &PyFile::rename )
    // 'sync' method
    .def( "sync",             &PyFile::sync )
    // 'inlineBufferSize' method
    .def( "inlineBufferSize", &PyFile::inlineBufferSize )
  ;

}
