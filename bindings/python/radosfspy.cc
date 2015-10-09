/*
 * radosfspy.cc
 *
 *  Created on: Oct 6, 2015
 *      Author: simonm
 */

#include "PyStat.hh"
#include "PyFsObj.hh"
#include "PyFilesystem.hh"
#include "PyFileInode.hh"

#include <Python.h>
#include <boost/python.hpp>

namespace py = boost::python;

/////////////////////////////////////////////////////////////////////////////////////
//
// vector of string to python list converter
//
/////////////////////////////////////////////////////////////////////////////////////
struct strvec_to_list
{
    static PyObject* convert( const std::vector<std::string> & v )
    {
      py::list ret;
      for( std::vector<std::string>::const_iterator it = v.begin(); it != v.end(); ++it )
      {
        ret.append( py::str( *it ) );
      }
      return py::incref( ret.ptr() );
    }
};

BOOST_PYTHON_MODULE(libradosfspy)
{
  // register string vector to python converter
  py::to_python_converter<std::vector<std::string>, strvec_to_list>();

  // export bindings for 'stat'
  radosfs::PyStat::export_bindings();

  // export bindings for 'FsObj'
  radosfs::PyFsObj::export_bindings();

  // export bindings for 'Filesystem'
  radosfs::PyFilesystem::export_bindings();

  // export bindings for 'FileInode'
  radosfs::PyFileInode::export_bindings();
}
