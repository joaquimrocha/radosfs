/*
 * PyDir.hh
 *
 *  Created on: Oct 9, 2015
 *      Author: simonm
 */

#ifndef BINDINGS_PYTHON_PYDIR_HH_
#define BINDINGS_PYTHON_PYDIR_HH_

#include "Dir.hh"

#include "ReadWriteHelper.h"

#include <boost/python.hpp>

namespace py = boost::python;


RADOS_FS_BEGIN_NAMESPACE

class PyFilesystem;

class PyDir : public Dir
{
  public:

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // Constructors
    //
    /////////////////////////////////////////////////////////////////////////////////////

    PyDir(PyFilesystem &radosFs, const py::str &path);

    PyDir(PyFilesystem &radosFs, const py::str &path, bool cacheable);

    PyDir(const PyDir &file) : Dir( file ) {}

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getParent' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    static py::tuple getParent(const py::str &path)
    {
      int pos;
      std::string ret = Dir::getParent( py::extract<std::string>( path ), &pos );
      return py::make_tuple( py::str( ret ), pos );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'create' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    int create4(int mode, bool mkPath, int ownerUid, int ownerGid)
    {
      return Dir::create( mode, mkPath, ownerUid, ownerGid );
    }

    int create3(int mode, bool mkPath, int ownerUid)
    {
      return Dir::create( mode, mkPath, ownerUid, -1 );
    }

    int create2(int mode, bool mkPath)
    {
      return Dir::create( mode, mkPath, -1, -1 );
    }

    int create1(int mode)
    {
      return Dir::create( mode, false, -1, -1 );
    }

    int create0()
    {
      return Dir::create( -1, false, -1, -1 );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'entryList' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple entryList()
    {
      std::set<std::string> entries;
      int rc = Dir::entryList( entries );

      py::list l;
      for( std::set<std::string>::iterator it = entries.begin(); it != entries.end(); ++it )
        l.append( py::str( *it ) );

      return py::make_tuple( rc, l );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'entry' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple entry(int entryIndex)
    {
      std::string path;
      int rc = Dir::entry( entryIndex, path );
      return py::make_tuple( rc, py::str( path ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'stat' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple stat(void)
    {
      struct stat buff;
      int rc = Dir::stat( &buff );
      return py::make_tuple( rc, PyStat( buff ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getMetadata' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getMetadata(const py::str &entry, const py::str &key)
    {
      std::string value;
      int rc = Dir::getMetadata( py::extract<std::string>( entry ), py::extract<std::string>( key ), value );
      return  py::make_tuple( rc, py::str( value ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getMetadataMap' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getMetadataMap(const py::str &entry)
    {
      std::map<std::string, std::string> mtdMap;
      int rc = Dir::getMetadataMap( py::extract<std::string>( entry ), mtdMap );

      py::dict d;
      for( std::map<std::string, std::string>::iterator it = mtdMap.begin(); it != mtdMap.end(); ++it )
      {
        d[py::str( it->first )] = py::str( it->second );
      }

      return py::make_tuple( rc, d );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'find' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple find(const py::str &args)
    {
      std::set<std::string> results;
      int rc = Dir::find( py::extract<std::string>( args ), results );

      py::list l;
      for( std::set<std::string>::iterator it = results.begin(); it != results.end(); ++it )
      {
        l.append( py::str( *it ) );
      }

      return py::make_tuple( rc, l );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // 'getTMId' method
    //
    /////////////////////////////////////////////////////////////////////////////////////

    py::tuple getTMId()
    {
      std::string id;
      int rc = Dir::getTMId( id );
      return py::make_tuple( rc, py::str( id ) );
    }

    /////////////////////////////////////////////////////////////////////////////////////
    //
    // export Python bindings
    //
    /////////////////////////////////////////////////////////////////////////////////////

    static void export_bindings();
};

RADOS_FS_END_NAMESPACE

#endif /* BINDINGS_PYTHON_PYDIR_HH_ */
