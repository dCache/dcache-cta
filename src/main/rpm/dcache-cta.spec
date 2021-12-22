Summary: dCache nearline storage provider for CTA
Vendor: dCache.org
Name: dcache-cta
URL: https://dcache.org
Packager: dCache.org <support@dcache.org>
License: Distributable
Group: Applications/System

Version: @dist.version@
Release: 1
BuildArch: noarch
Prefix: /

AutoReqProv: no
Requires: dcache >= 7.2.2

Source0: %{name}-%{version}.tar.gz

%description
dCache Nearline storage provider for integration with CERN Tape Archive (CTA)

%prep
%setup -q -a 0 -n %{name}-%{version}

%install
mkdir -p %{buildroot}%{_datadir}/dcache/plugins
cp -a %{name}-%{version} %{buildroot}%{_datadir}/dcache/plugins

%files
%defattr(-,root,root,-)
%{_datadir}

%changelog
* Wed Dec 22 2021 Tigran Mkrtchyan <tigran.mkrtchyan@desy.de>
- Initialize package creation