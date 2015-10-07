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

class PyFsObj : public FsObj, public py::wrapper<PyFsObj>
{
  public:

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // Constructor (not exposed)
    //
    /////////////////////////////////////////////////////////////////////////////////////
    PyFsObj(const FsObj & obj) : FsObj( obj ) {}

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'isWritable' method
    //
    /////////////////////////////////////////////////////////////////////////////////////
    bool isWritable(void)
    {
      return this->get_override("isWritable")();
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'isRaedable' method
    //
    /////////////////////////////////////////////////////////////////////////////////////
    bool isReadable(void)
    {
      return this->get_override("isReadable")();
    }

//    virtual Filesystem *filesystem(void) const; //TODO
//    virtual void setFilesystem(Filesystem *radosFs); // TODO

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

    static void export_bindings();
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_PYFSOBJ_HH_ */
