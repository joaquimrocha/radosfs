/*
 * async_op_callback.h
 *
 *  Created on: Oct 9, 2015
 *      Author: simonm
 */

#ifndef BINDINGS_PYTHON_READWRITEHELPER_H_
#define BINDINGS_PYTHON_READWRITEHELPER_H_

#include "PyFilesystem.hh"

#include <string>
#include <utility>
#include <boost/python.hpp>

namespace py = boost::python;


RADOS_FS_BEGIN_NAMESPACE

class ReadWriteHelper
{
  public :

    static void async_op_callback(const std::string &opId, int retCode, void *pair)
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

    template<class READER>
    static ssize_t read_impl(py::object arr, off_t offset, READER &reader)
    {
      if( ! PyByteArray_Check( arr.ptr() ) )
      {
        PyErr_SetString(PyExc_TypeError, "A bytearray was expected !");
        throw py::error_already_set();
      }

      char *buff  = PyByteArray_AsString( arr.ptr() );
      size_t blen = PyByteArray_Size( arr.ptr() );

      return reader.read( buff, offset, blen );
    }

    template<class READER>
    static py::tuple read_impl(const py::list &l, py::object pyCallback, py::object callbackArg, READER &reader)
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
      int rc = reader.read(intervals, &asyncOpId, callback, pair);

      return py::make_tuple( rc, py::str( asyncOpId ) );
    }

    template<class WRITER>
    static py::tuple write_impl(py::object arr, off_t offset, bool copyBuffer, py::object pyCallback, py::object callbackArg, WRITER &writer)
    {
      if( ! PyByteArray_Check( arr.ptr() ) )
      {
        PyErr_SetString(PyExc_TypeError, "A bytearray was expected !");
        throw py::error_already_set();
      }

      char *buff = PyByteArray_AsString( arr.ptr() );
      size_t blen = PyByteArray_Size( arr.ptr() );

      // the callback
      AsyncOpCallback callback = ( pyCallback == py::object() ? 0 : ReadWriteHelper::async_op_callback );
      // wrap the callback and callbackArg into a pair
      // (send arguments to callback only if a callback exists)
      std::pair<py::object, py::object>* pair = ( callback ? new std::pair<py::object, py::object>( pyCallback, callbackArg ) : 0 );
      // delegate the work to c++ API
      std::string asyncOpId;
      int rc = writer.write( buff, offset, blen, copyBuffer, &asyncOpId, callback, pair);

      return py::make_tuple( rc, py::str( asyncOpId ) );
    }

    template<class WRITER>
    static int writeSync_impl(py::object arr, off_t offset, WRITER &writer)
    {
      if( ! PyByteArray_Check( arr.ptr() ) )
      {
        PyErr_SetString(PyExc_TypeError, "A bytearray was expected !");
        throw py::error_already_set();
      }

      char *buff  = PyByteArray_AsString( arr.ptr() );
      size_t blen = PyByteArray_Size( arr.ptr() );

      return writer.writeSync( buff, offset, blen );
    }
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_READWRITEHELPER_H_ */
