/*
 * PyFileInode.cc
 *
 *  Created on: Oct 7, 2015
 *      Author: simonm
 */

#include "PyFileInode.hh"
#include "PyFilesystem.hh"

#include <utility>



void async_op_callback(const std::string &opId, int retCode, void *pair)
{
  // extract the python callback and real args from args
  std::pair<py::object, py::object>* p = (std::pair<py::object, py::object>*) pair;
  py::object callback = p->first;
  py::object args     = p->second;
  // delete the wrapper
  delete p;
  // delegate the job to Python callback
  callback( py::str( opId ), retCode, args );
}

py::tuple radosfs::PyFileInode::read3(const py::list& l, py::object pyCallback, py::object callbackArg)
{
  // py::list to std::vector<PyFileReadData>
  std::vector<FileReadData> intervals;
  py::ssize_t size = py::len( l );
  for( int i = 0; i < size; ++i )
  {
    intervals.push_back( py::extract<PyFileReadData>( l[i] ) );
  }
  // the callback
  AsyncOpCallback callback = ( pyCallback == py::object() ? 0 : async_op_callback );
  // wrap the callback and callbackArg into a pair
  // (send arguments to callback only if a callback exists)
  std::pair<py::object, py::object>* pair = ( callback ? new std::pair<py::object, py::object>( pyCallback, callbackArg ) : 0 );
  // delegate the work to c++ API
  std::string asyncOpId;
  int rc = FileInode::read(intervals, &asyncOpId, callback, pair);

  return py::make_tuple( rc, py::str( asyncOpId ) );
}

py::tuple radosfs::PyFileInode::read2(const py::list& l, py::object callback)
{
  return read3( l, callback, py::object() );
}

py::tuple radosfs::PyFileInode::read1(const py::list& l)
{
  return read3( l, py::object(), py::object() );
}

py::tuple radosfs::PyFileInode::write5(py::object arr, off_t offset, bool copyBuffer, py::object pyCallback, py::object callbackArg)
{
  if( ! PyByteArray_Check( arr.ptr() ) )
  {
    PyErr_SetString(PyExc_TypeError, "A bytearray was expected !");
    throw py::error_already_set();
  }

  char *buff = PyByteArray_AsString( arr.ptr() );
  size_t blen = PyByteArray_Size( arr.ptr() );

  // the callback
  AsyncOpCallback callback = ( pyCallback == py::object() ? 0 : async_op_callback );
  // wrap the callback and callbackArg into a pair
  // (send arguments to callback only if a callback exists)
  std::pair<py::object, py::object>* pair = ( callback ? new std::pair<py::object, py::object>( pyCallback, callbackArg ) : 0 );
  // delegate the work to c++ API
  std::string asyncOpId;
  int rc = FileInode::write( buff, offset, blen, copyBuffer, &asyncOpId, callback, pair);

  return py::make_tuple( rc, py::str( asyncOpId ) );
}

py::tuple radosfs::PyFileInode::write4(py::object arr, off_t offset, bool copyBuffer, py::object callback)
{
  return write5(arr, offset, copyBuffer, callback, py::object());
}

py::tuple radosfs::PyFileInode::write3(py::object arr, off_t offset, bool copyBuffer)
{
  return write5(arr, offset, copyBuffer, py::object(), py::object());
}

py::tuple radosfs::PyFileInode::write2(py::object arr, off_t offset)
{
  return write5( arr, offset, false, py::object(), py::object() );
}

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
