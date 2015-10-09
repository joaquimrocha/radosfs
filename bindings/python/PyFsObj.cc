/*
 * PyFsObj.cc
 *
 *  Created on: Oct 6, 2015
 *      Author: simonm
 */

#include "PyFsObj.hh"
#include "PyFilesystem.hh"

radosfs::PyFsObj::PyFsObj(PyFilesystem &radosFs, const py::str &path) : FsObj( &radosFs, py::extract<std::string>( path ) ) {}

radosfs::PyFilesystem* radosfs::PyFsObj::filesystem_impl(void) const
{
  Filesystem* ptr = FsObj::filesystem();
  return dynamic_cast<PyFilesystem*>( ptr );
}

void radosfs::PyFsObj::export_bindings()
{
  py::class_<PyFsObj>( "FsObj", py::no_init )
      // 'isWritable' method
      .def( "isWritable", py::pure_virtual( &PyFsObj::isWritable ) )
      // 'isReadable' method
      .def( "isReadable", py::pure_virtual( &PyFsObj::isReadable ) )
      // 'path' method
      .def( "path",          &PyFsObj::path )
      // 'setPath' method
      .def( "setPath",       &PyFsObj::setPath )
      // 'isFile' method
      .def( "isFile",        &PyFsObj::isFile )
      // 'isDir' method
      .def( "isDir",         &PyFsObj::isDir )
      // 'exists' method
      .def( "exists",        &PyFsObj::exists )
      // 'stat' method
      .def( "stat",          &PyFsObj::stat )
      // 'update' method
      .def( "update",        &PyFsObj::refresh )
      // 'setXAttr' method
      .def( "setXAttr",      &PyFsObj::setXAttr )
      // 'getXAttr' method
      .def( "getXAttr",      &PyFsObj::getXAttr )
      // 'removeXAttr'
      .def( "removeXAttr",   &PyFsObj::removeXAttr )
      // 'getXAttrsMap' method
      .def( "getXAttrsMap",  &PyFsObj::getXAttrsMap )
      // 'createLink' method
      .def( "createLink",    &PyFsObj::createLink )
      // 'isLink' method
      .def( "isLink",        &PyFsObj::isLink )
      // 'targetPath' method
      .def( "targetPath",    &PyFsObj::targetPath )
      // 'chmod' method
      .def( "chmod",         &PyFsObj::chmod )
      // 'chown' method
      .def( "chown",         &PyFsObj::chown )
      // 'setUid' method
      .def( "setUid",        &PyFsObj::setUid )
      // 'setGid' method
      .def( "setGid",        &PyFsObj::setGid )
      // 'rename' method
      .def( "rename",        &PyFsObj::rename )
      // 'filesystem' method
      .def( "filesystem",    &PyFsObj::filesystem_impl, py::return_value_policy<py::reference_existing_object>() )
  ;
}
