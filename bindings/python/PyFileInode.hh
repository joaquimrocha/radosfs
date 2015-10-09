/*
 * PyFileInode.hh
 *
 *  Created on: Oct 7, 2015
 *      Author: simonm
 */

#ifndef BINDINGS_PYTHON_PYFILEINODE_HH_
#define BINDINGS_PYTHON_PYFILEINODE_HH_

#include "FileInode.hh"

#include "PyFilesystem.hh"

#include <iostream>


RADOS_FS_BEGIN_NAMESPACE

class PyFileInode : public FileInode
{
  public:

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // Constructors
    //
    /////////////////////////////////////////////////////////////////////////////////////

    PyFileInode(PyFilesystem &fs, const py::str &pool) : FileInode( &fs, py::extract<std::string>( pool ) ) {}
    PyFileInode(PyFilesystem &fs, const py::str &pool, const py::str &name) : FileInode( &fs, py::extract<std::string>( pool ), py::extract<std::string>( name ) ) {}
    PyFileInode(PyFilesystem &fs, const py::str &pool, const py::str &name, const size_t chunkSize) : FileInode( &fs, py::extract<std::string>( pool ), py::extract<std::string>( name ), chunkSize ) {}
    PyFileInode(PyFilesystem &fs, const py::str &pool, const size_t chunkSize) : FileInode( &fs, py::extract<std::string>( pool ), chunkSize ) {}

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'read' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    ssize_t read(py::object arr, off_t offset)
    {
      if( ! PyByteArray_Check( arr.ptr() ) )
      {
        PyErr_SetString(PyExc_TypeError, "A bytearray was expected !");
        throw py::error_already_set();
      }

      char *buff  = PyByteArray_AsString( arr.ptr() );
      size_t blen = PyByteArray_Size( arr.ptr() );

      return FileInode::read( buff, offset, blen );
    }

    py::tuple read3(const py::list& intervals, py::object callback, py::object callbackArg);

    py::tuple read2(const py::list& intervals, py::object callback);

    py::tuple read1(const py::list& intervals);

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'write' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple write5(py::object arr, off_t offset, bool copyBuffer, py::object callback, py::object callbackArg);

    py::tuple write4(py::object arr, off_t offset, bool copyBuffer, py::object callback);

    py::tuple write3(py::object arr, off_t offset, bool copyBuffer);

    py::tuple write2(py::object arr, off_t offset);

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'writeSync' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int writeSync(py::object arr, off_t offset)
    {
      if( ! PyByteArray_Check( arr.ptr() ) )
      {
        PyErr_SetString(PyExc_TypeError, "A bytearray was expected !");
        throw py::error_already_set();
      }

      char *buff  = PyByteArray_AsString( arr.ptr() );
      size_t blen = PyByteArray_Size( arr.ptr() );

      return FileInode::writeSync( buff, offset, blen );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'sync' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int sync1(const py::str &opId )
    {
      return FileInode::sync( py::extract<std::string>( opId ) );
    }

    int sync0()
    {
      return FileInode::sync( "" );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'registerFile' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int registerFile4(const py::str &path, uid_t uid, gid_t gid, int mode)
    {
      return FileInode::registerFile( py::extract<std::string>( path ), uid, gid, mode );
    }

    int registerFile3(const py::str &path, uid_t uid, gid_t gid)
    {
      return FileInode::registerFile( py::extract<std::string>( path ), uid, gid, -1 );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getBackLink' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getBackLink()
    {
      std::string backLink;
      int rc = FileInode::getBackLink(&backLink);
      return py::make_tuple( rc, py::str( backLink ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // export python bindings
    //
    /////////////////////////////////////////////////////////////////////////////////////

    static void export_bindings();
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_PYFILEINODE_HH_ */
