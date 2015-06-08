\tableofcontents

Introduction
=============

RadosFs is a complete client-side implementation of filesystem-like
functionality based on the [RADOS](http://ceph.com/docs/master/rados/) object
store ([Ceph](http://ceph.com)).
RadosFs was designed according to the requirements of the [CERN Data Storage
Services](http://information-technology.web.cern.ch/about/organisation/data-storage-services)
group in terms of scalability and flexibility: it provides a scale-out namespace
and pseudo-hierarchical storage view with optimized directory and file access
(no strict POSIX semantics), parallel metadata queries, modification time
propagation, file striping and other features.

RadosFs should not be confused with
[CephFS](http://ceph.com/docs/master/cephfs/). CephFS is a high-performance
POSIX-compliant filesystem implementation built on top of RADOS and supported by
[Inktank/Redhat](http://www.redhat.com/en/technologies/storage/ceph).

License
--------

RadosFs is published under the terms of the
[LGPLv3 license](http://www.gnu.org/licenses/lgpl-3.0.html).

