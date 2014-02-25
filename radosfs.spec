%define _unpackaged_files_terminate_build 0
Name:		radosfs
Version:	0.1.0
Release:	1
Summary:	A file system library based in librados
Prefix:         /usr
Group:		Applications/File
License:	GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

Source:        %{name}-%{version}-%{release}.tar.gz
BuildRoot:     %{_tmppath}/%{name}-root

BuildRequires: cmake >= 2.6
BuildRequires: ceph-libs ceph-devel

Requires:      ceph-libs

%description
A file system library based in librados

#-------------------------------------------------------------------------------
# devel
#------------------------------------------------------------------------------
%package devel
Summary: Development files for radosfs
Group: Development/Libraries
Provides: %{name}-devel = %{epoch}:%{version}-%{release}
Provides: %{name}-devel%{?_isa} = %{epoch}:%{version}-%{release}
Obsoletes:%{name}-devel < %{epoch}:%{version}-%{release}
Requires: %{name}%{?_isa} = %{epoch}:%{version}-%{release}

%description devel
This package contains the header files for RadosFs.

%prep
%setup -n %{name}-%{version}-%{release}

%build
test -e $RPM_BUILD_ROOT && rm -r $RPM_BUILD_ROOT
%if 0%{?rhel} < 6
export CC=/usr/bin/gcc44 CXX=/usr/bin/g++44
%endif

mkdir -p build
cd build
cmake ../ -DRELEASE=%{release} -DCMAKE_BUILD_TYPE=RelWithDebInfo
%{__make} %{_smp_mflags}

%install
cd build
%{__make} install DESTDIR=$RPM_BUILD_ROOT
echo "Installed!"

%post
/sbin/ldconfig

%postun
/sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_libdir}/src/libradosfs.so

%files devel
%defattr(-,root,root,-)
%{_includedir}/libradosfs.hh
%{_includedir}/rados/RadosFs.hh
%{_includedir}/rados/RadosFsDir.hh
%{_includedir}/rados/RadosFsFile.hh
