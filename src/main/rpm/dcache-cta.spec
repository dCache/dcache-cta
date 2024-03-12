# compatibility with older Linux versions
%define _source_payload w9.gzdio
%define _binary_payload w9.gzdio
%define _source_filedigest_algorithm 1
%define _binary_filedigest_algorithm 1

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
%{?systemd_requires}

Source0: %{name}-%{version}.tar.gz

%description
dCache Nearline storage provider for integration with CERN Tape Archive (CTA)

%prep
%setup -q -a 0 -n %{name}-%{version}

%install
mkdir -p %{buildroot}%{_datadir}/dcache/plugins
cp -a %{name}-%{version} %{buildroot}%{_datadir}/dcache/plugins

%post
/usr/bin/systemctl daemon-reload >/dev/null 2>&1 ||:

%postun
/usr/bin/systemctl daemon-reload >/dev/null 2>&1 ||:

%files
%defattr(-,root,root,-)
%{_datadir}

%changelog
* Thu Mar 17 2022 Tigran Mkrtchyan <tigran.mkrtchyan@desy.de>
- reload systemd daemon after install/upgrade/uninstall
* Wed Dec 22 2021 Tigran Mkrtchyan <tigran.mkrtchyan@desy.de>
- Initialize package creation
