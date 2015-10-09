/*
 * PyFilesystem.cc
 *
 *  Created on: Oct 5, 2015
 *      Author: simonm
 */

#include "PyFilesystem.hh"

void radosfs::PyFilesystem::export_bindings()
{
  py::enum_<Filesystem::LogLevel>("LogLevel")
      .value("LOG_LEVEL_NONE",  LOG_LEVEL_NONE)
      .value("LOG_LEVEL_DEBUG", LOG_LEVEL_DEBUG)
  ;

  py::class_<PyFileReadData>("FileReadData", py::init<py::object, off_t>())
      .add_property( "retValue",  &PyFileReadData::getRetValue )
      .add_property( "buff",  &PyFileReadData::getBuff )
  ;

  py::class_<PyFilesystem>("Filesystem")
      // 'init' method
      .def( "init",                   &PyFilesystem::init2 )
      .def( "init",                   &PyFilesystem::init1 )
      .def( "init",                   &PyFilesystem::init0 )
      // 'addDataPool' method
      .def( "addDataPool",            &PyFilesystem::addDataPool3 )
      .def( "addDataPool",            &PyFilesystem::addDataPool2 )
      // 'removeDataPool' method
      .def( "removeDataPool",         &PyFilesystem::removeDataPool )
      // 'dataPools' method
      .def( "dataPools",              &PyFilesystem::dataPools )
      // 'dataPoolPrefix' method
      .def( "dataPoolPrefix",         &PyFilesystem::dataPoolPrefix )
      // 'dataPoolSize' method
      .def( "dataPoolSize",           &PyFilesystem::dataPoolSize)
      // 'addMetadataPool' method
      .def( "addMetadataPool",        &PyFilesystem::addMetadataPool )
      // 'removeMetadataPool' method
      .def( "removeMetadataPool",     &PyFilesystem::removeMetadataPool )
      // 'dataPools' method
      .def( "dataPools",              &PyFilesystem::dataPools )
      // 'metadataPools' method
      .def( "metadataPools",          &PyFilesystem::metadataPools )
      // 'metadataPoolPrefix' method
      .def( "metadataPoolPrefix",     &PyFilesystem::metadataPoolPrefix )
      // 'metadataPoolFromPrefix' method
      .def( "metadataPoolFromPrefix", &PyFilesystem::metadataPoolFromPrefix )
      // 'setIds' method
      .def( "setIds",                 &PyFilesystem::setIds )
      // 'getIds' method
      .def( "getIds",                 &PyFilesystem::getIds )
      // 'uid' method
      .def( "uid",                    &PyFilesystem::uid )
      // 'gid' method
      .def( "gid",                    &PyFilesystem::gid )
      // 'statCluster' method
      .def( "statCluster",            &PyFilesystem::statCluster )
      // 'stat' method
      .def( "stat",                   &PyFilesystem::stat0 )
      .def( "stat",                   &PyFilesystem::stat1 )
      // 'allPoolsInCluster' method
      .def( "allPoolsInCluster",      &PyFilesystem::allPoolsInCluster )
      // 'setXAttr' method
      .def( "setXAttr",               &PyFilesystem::setXAttr )
      // 'getXAttr' method
      .def( "getXAttr",               &PyFilesystem::getXAttr )
      // 'removeXAttr' method
      .def( "removeXAttr",            &PyFilesystem::removeXAttr )
      // 'getXAttrsMap' method
      .def( "getXAttrsMap",           &PyFilesystem::getXAttrsMap )
      // 'setDirCacheMaxSize' method
      .def( "setDirCacheMaxSize",     &PyFilesystem::setDirCacheMaxSize )
      // 'dirCacheMaxSize' mthod
      .def( "dirCacheMaxSize",        &PyFilesystem::dirCacheMaxSize )
      // 'setDirCompactRatio' method
      .def( "setDirCompactRatio",     &PyFilesystem::setDirCompactRatio )
      // 'dirCompactRatio' method
      .def( "dirCompactRatio",        &PyFilesystem::dirCompactRatio )
      // 'setLogLevel' method
      .def( "setLogLevel",            &PyFilesystem::setLogLevel )
      // 'logLevel' method
      .def( "logLevel",               &PyFilesystem::logLevel )
      // 'setFileChunkSize' method
      .def( "setFileChunkSize",       &PyFilesystem::setFileChunkSize )
      // 'fileChunkSize' method
      .def( "fileChunkSize",          &PyFilesystem::fileChunkSize )
      // 'getFsObj' method
      .def( "getFsObj",               &PyFilesystem::getFsObj, py::return_value_policy<py::manage_new_object>() )
      // 'getInodeAndPool' method
      .def( "getInodeAndPool",        &PyFilesystem::getInodeAndPool )
      // 'setNumGenericWorkers' method
      .def( "setNumGenericWorkers",   &PyFilesystem::setNumGenericWorkers )
      // 'numGenericWorkers' method
      .def( "numGenericWorkers",      &PyFilesystem::numGenericWorkers )
  ;
}

