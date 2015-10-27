/*
 * PyFsObj.hh
 *
 *  Created on: Oct 6, 2015
 *      Author: simonm
 */

#ifndef BINDINGS_PYTHON_PYFSOBJ_HH_
#define BINDINGS_PYTHON_PYFSOBJ_HH_

#include "FsObj.hh"

#include "PyStat.hh"

#include <boost/python.hpp>

namespace py = boost::python;


RADOS_FS_BEGIN_NAMESPACE

class PyFilesystem;

class PyFsObj : public virtual FsObj
{
  public:

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // Constructors (not exposed)
    //
    /////////////////////////////////////////////////////////////////////////////////////

    PyFsObj(PyFilesystem &radosFs, const py::str &path);

    PyFsObj(const FsObj & obj) : FsObj( obj ) {}

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'isWritable' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    bool isWritable(void)
    {
      PyErr_SetString(PyExc_TypeError, "Not implemented!");
      throw py::error_already_set();
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'isRaedable' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    bool isReadable(void)
    {
      PyErr_SetString(PyExc_TypeError, "Not implemented!");
      throw py::error_already_set();
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'filesystem_impl' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    virtual PyFilesystem* filesystem_impl(void) const;

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'stat' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple stat(void)
    {
      struct stat buff;
      int rc = FsObj::stat( &buff );
      return py::make_tuple( rc, PyStat( buff ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getXAttr' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getXAttr(py::str attrName)
    {
      std::string value;
      int rc = FsObj::getXAttr( py::extract<std::string>( attrName ), value );
      return py::make_tuple( rc, py::str( value ) );
    }

    py::tuple getXAttrsMap(void)
    {
      std::map<std::string, std::string> m;
      int rc = FsObj::getXAttrsMap( m );
      py::dict d;
      for( std::map<std::string, std::string>::iterator it = m.begin(); it != m.end(); ++it )
      {
        d[py::str( it->first )] = py::str( it->second );
      }
      return py::make_tuple( rc, d );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // export Python bindings
    //
    /////////////////////////////////////////////////////////////////////////////////////

    static void export_bindings();
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_PYFSOBJ_HH_ */
