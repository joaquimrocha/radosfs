/*
 * PyFile.hh
 *
 *  Created on: Oct 9, 2015
 *      Author: simonm
 */

#ifndef BINDINGS_PYTHON_PYFILE_HH_
#define BINDINGS_PYTHON_PYFILE_HH_

#include "File.hh"

#include "ReadWriteHelper.h"

#include <boost/python.hpp>

namespace py = boost::python;


RADOS_FS_BEGIN_NAMESPACE

class PyFilesystem;

class PyFile : public File
{
  public:

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // Constructors
    //
    /////////////////////////////////////////////////////////////////////////////////////

    PyFile(PyFilesystem &radosFs, const py::str &path);

    PyFile(PyFilesystem &radosFs, const py::str &path, OpenMode mode);

    PyFile(const PyFile &file) : File( file ) {}

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'read' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    ssize_t read(py::object arr, off_t offset)
    {
      return ReadWriteHelper::read_impl<File>( arr, offset, *this );
    }

    py::tuple read3(const py::list& intervals, py::object callback, py::object callbackArg)
    {
      return ReadWriteHelper::read_impl<File>( intervals, callback, callbackArg, *this );
    }

    py::tuple read2(const py::list& intervals, py::object callback)
    {
      return ReadWriteHelper::read_impl<File>( intervals, callback, py::object(), *this );
    }

    py::tuple read1(const py::list& intervals)
    {
      return ReadWriteHelper::read_impl<File>( intervals, py::object(), py::object(), *this );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'write' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple write5(py::object arr, off_t offset, bool copyBuffer, py::object callback, py::object callbackArg)
    {
      return ReadWriteHelper::write_impl<File>( arr, offset, copyBuffer, callback, callbackArg, *this );
    }

    py::tuple write4(py::object arr, off_t offset, bool copyBuffer, py::object callback)
    {
      return ReadWriteHelper::write_impl<File>( arr, offset, copyBuffer, callback, py::object(), *this );
    }

    py::tuple write3(py::object arr, off_t offset, bool copyBuffer)
    {
      return ReadWriteHelper::write_impl<File>( arr, offset, copyBuffer, py::object(), py::object(), *this );
    }

    py::tuple write2(py::object arr, off_t offset)
    {
      return ReadWriteHelper::write_impl<File>( arr, offset, false, py::object(), py::object(), *this );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'writeSync' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int writeSync(py::object arr, off_t offset)
    {
      return ReadWriteHelper::writeSync_impl<File>( arr, offset, *this );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'create' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int create4(int permissions, const std::string &pool, size_t chunkSize, ssize_t inlineBufferSize)
    {
      return File::create( permissions, pool, chunkSize, inlineBufferSize );
    }

    int create3(int permissions, const std::string &pool, size_t chunkSize)
    {
      return File::create( permissions, pool, chunkSize, -1 );
    }

    int create2(int permissions, const std::string &pool)
    {
      return File::create( permissions, pool, 0, -1 );
    }

    int create1(int permissions)
    {
      return File::create( permissions, "", 0, -1 );
    }

    int create0()
    {
      return File::create( -1, "", 0, -1 );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'stat' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple stat(void)
    {
      struct stat buff;
      int rc = File::stat( &buff );
      return py::make_tuple( rc, PyStat( buff ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // export Python bindings
    //
    /////////////////////////////////////////////////////////////////////////////////////

    static void export_bindings();
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_PYFILE_HH_ */
