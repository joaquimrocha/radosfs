/*
 * PyDir.cc
 *
 *  Created on: Oct 9, 2015
 *      Author: simonm
 */

#include "PyDir.hh"

#include "radosfscommon.h"

radosfs::PyDir::PyDir(PyFilesystem &radosFs, const py::str &path) : FsObj( &radosFs, getDirPath( py::extract<std::string>( path ) ) ), Dir( &radosFs, py::extract<std::string>( path ) ), PyFsObj( radosFs, path ) {}

radosfs::PyDir::PyDir(PyFilesystem &radosFs, const py::str &path, bool cacheable) : FsObj( &radosFs, getDirPath( py::extract<std::string>( path ) ) ), Dir( &radosFs, py::extract<std::string>( path ), cacheable ), PyFsObj( radosFs, path ) {}

void radosfs::PyDir::export_bindings()
{
  py::class_<PyDir, py::bases<PyFsObj> >( "Dir", py::init<PyFilesystem&, py::str>() )
      // other constructors
      .def( py::init<PyFilesystem&, py::str, bool>() )
      .def( py::init<PyDir&>() )
      // 'getParent' method
      .def( "getParent",      &PyDir::getParent )
      // 'remove' method
      .def( "remove",         &PyDir::remove )
      // 'create' method
      .def( "create",         &PyDir::create4 )
      .def( "create",         &PyDir::create3 )
      .def( "create",         &PyDir::create2 )
      .def( "create",         &PyDir::create1 )
      .def( "create",         &PyDir::create0 )
      // 'entryList' method
      .def( "entryList",      &PyDir::entryList )
      // 'refresh' method
      .def( "refresh",        &PyDir::refresh )
      // 'entry' method
      .def( "entry",          &PyDir::entry )
      // 'setPath' method
      .def( "setPath",        &PyDir::setPath )
      // 'isWritable' method
      .def( "isWritable",     &PyDir::isWritable )
      // 'isReadable' method
      .def( "isReadable",     &PyDir::isReadable )
      // 'stat' method
      .def( "stat",           &PyDir::stat )
      // 'compact' method
      .def( "compact",        &PyDir::compact )
      // 'setMetadata'
      .def( "setMetadata",    &PyDir::setMetadata )
      // 'getMetadata' method
      .def( "getMetadata",    &PyDir::getMetadata )
      // 'getMetadataMap' method
      .def( "getMetadataMap", &PyDir::getMetadataMap )
      // 'removeMetadata' method
      .def( "removeMetadata", &PyDir::removeMetadata )
      // 'find' method
      .def( "find",           &PyDir::find )
      // 'chmod' method
      .def( "chmod",          &PyDir::chmod )
      // 'chown' method
      .def( "chown",          &PyDir::chown )
      // 'setUid' method
      .def( "setUid",         &PyDir::setUid )
      // 'setGid' method
      .def( "setGid",         &PyDir::setGid )
      // 'rename' method
      .def( "rename",         &PyDir::rename )
      // 'useTMId' method
      .def( "useTMId",        &PyDir::useTMId )
      // 'usingTMId' method
      .def( "usingTMId",      &PyDir::usingTMId )
      // 'getTMId' method
      .def( "getTMId",        &PyDir::getTMId )
  ;

}
