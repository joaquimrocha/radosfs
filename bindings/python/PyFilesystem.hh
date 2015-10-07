/*
 * PyFilesystem.hh
 *
 *  Created on: Oct 5, 2015
 *      Author: simonm
 */

#ifndef BINDINGS_PYTHON_PYFILESYSTEM_HH_
#define BINDINGS_PYTHON_PYFILESYSTEM_HH_

#include "Filesystem.hh"

#include "PyStat.hh"
#include "PyFsObj.hh"

#include <Python.h>

#include <boost/python.hpp>

namespace py = boost::python;

RADOS_FS_BEGIN_NAMESPACE

struct PyFileReadData : public FileReadData
{
  PyFileReadData(py::object arr, off_t offset, size_t length)
    : FileReadData( 0, offset, length, &retValue ), retValue( 0 )
  {
    if( ! PyByteArray_Check( arr.ptr() ) )
    {
      PyErr_SetString(PyExc_TypeError, "A bytearray was expected !");
      throw py::error_already_set();
    }

    buff = PyByteArray_AsString( arr.ptr() );
  }

  ssize_t getRetValue()
  {
    return retValue;
  }

  ssize_t retValue;
};

class PyFilesystem : public radosfs::Filesystem
{
  public:

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'init' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int init2( const py::str &userName, const py::str &configurationFile ) { return init( py::extract<std::string>( userName), py::extract<std::string>( configurationFile ) ); }

    int init1( const py::str &userName ) { return init( py::extract<std::string>( userName) ); }

    int init0() { return init(); }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'addDataPool' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int addDataPool3(const py::str &name, const py::str &prefix, size_t size) { return addDataPool( py::extract<std::string>( name ), py::extract<std::string>( prefix ), size); }

    int addDataPool2(const py::str &name, const py::str &prefix) { return addDataPool( py::extract<std::string>( name ), py::extract<std::string>( prefix ) ); }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getIds' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getIds() const
    {
      uid_t uid;
      gid_t gid;
      Filesystem::getIds(&uid, &gid);
      return py::make_tuple(uid, gid);
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'statCluster' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple statCluster()
    {
      uint64_t totalSpaceKb, usedSpaceKb, availableSpaceKb, numberOfObjects;
      Filesystem::statCluster( &totalSpaceKb, &usedSpaceKb, &availableSpaceKb, &numberOfObjects );
      return py::make_tuple(totalSpaceKb, usedSpaceKb, availableSpaceKb, numberOfObjects );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'stat' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple stat0()
    {
      std::string path;
      struct stat buff;
      int rc = Filesystem::stat(path, &buff);
      return py::make_tuple( rc, PyStat( buff ) );
    }

    py::list stat1( const py::list &paths )
    {
      // unpack the py::list and delegate work to real stat
      std::vector<std::pair<int, struct stat> > v = stat( list_to_strvec( paths ) );
      // pack the result into py::list
      py::list ret;
      for( std::vector<std::pair<int, struct stat> >::const_iterator it = v.begin(); it != v.end(); ++it )
      {
        ret.append( py::tuple( py::make_tuple( it->first, it->second ) ) );
      }
      return ret;
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getXAttr' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getXAttr(const py::str &path, const py::str &attrName)
    {
      std::string value;
      int rc = Filesystem::getXAttr( py::extract<std::string>( path ), py::extract<std::string>( attrName ), value );
      return py::make_tuple( rc, py::str( value ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getXAttrsMap' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getXAttrsMap(const py::str &path)
    {
      std::map<std::string, std::string> m;
      int rc = Filesystem::getXAttrsMap(py::extract<std::string>( path ), m );
      py::dict d;
      for( std::map<std::string, std::string>::iterator it = m.begin(); it != m.end(); ++it )
      {
        d[py::str( it->first )] = py::str( it->second );
      }
      return py::make_tuple( rc, d );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getFsObj' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    PyFsObj* getFsObj(const py::str &path)
    {
      FsObj* obj = Filesystem::getFsObj( py::extract<std::string>( path ) );
      if( obj )
      {
        PyFsObj *ret = new PyFsObj( *obj );
        delete obj;
        return ret;
      }
      else
        return 0;
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getInodeAndPool' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getInodeAndPool(const py::str &path )
    {
      std::string inode, pool;
      int rc = Filesystem::getInodeAndPool( py::extract<std::string>( path ), &inode, &pool );
      return py::make_tuple( rc, py::str( inode ), py::str( pool ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // export python bindings
    //
    /////////////////////////////////////////////////////////////////////////////////////

    static void export_bindings();

  private:

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // convert a list into a vector of strings
    //
    /////////////////////////////////////////////////////////////////////////////////////

    static std::vector<std::string> list_to_strvec( const py::list & l)
    {
      std::vector<std::string> ret;
      py::ssize_t size = py::len( l );
      for( int i = 0; i < size; ++i )
      {
        ret.push_back( py::extract<std::string>( l[i] ) );
      }
      return ret;
    }
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_PYFILESYSTEM_HH_ */
