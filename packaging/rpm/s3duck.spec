# Built by build_rpm.sh (`make rpm`), which passes:
#   version  -- __VERSION__ from version.py, the single source of truth
#   payload  -- the /usr/lib/s3duck tree staged by tools/stage_payload.sh,
#               the same file set the .deb ships
#   srcdir   -- the repository checkout (launcher scripts, desktop entry)
#
# The app is pure Python and runs on the system interpreter and system PyQt6,
# so the package is noarch, like the .deb's payload.

Name:           s3duck
Version:        %{version}
Release:        1%{?dist}
Summary:        Simple open-source cross-platform client for S3 storage
License:        MIT
URL:            https://github.com/nexusriot/s3duck
BuildArch:      noarch

# Every Requires is imported directly by the app, except qt6-qtsvg: Qt needs
# its image plugin to render the bundled SVG icons at all, and without it the
# toolbar buttons come out blank. On Fedora python3-pyqt6 carries QtPdf,
# QtSvg and QtMultimedia bindings, so the preview has what it imports.
Requires:       python3 >= 3.10
Requires:       python3-boto3
Requires:       python3-botocore
Requires:       python3-cryptography
Requires:       python3-urllib3
Requires:       python3-pyqt6
Requires:       qt6-qtsvg
# Imported behind a guard: keyring is one of three places the credential key
# can live; awscrt adds the CRC32C/CRC64NVME upload checksums.
Recommends:     python3-keyring
Recommends:     python3-awscrt

# The payload is source, not a Python distribution: nothing to byte-compile,
# no shebangs in it to mangle, no debuginfo for noarch.
%global __brp_python_bytecompile %{nil}
%global __brp_mangle_shebangs %{nil}
%global debug_package %{nil}
# No %changelog: the version history lives in git, not in the spec.
%global source_date_epoch_from_changelog 0
# s3duck.py carries `#!/usr/bin/python`, but the launchers run it with
# python3; the shebang must not drag in python-unversioned-command.
%global __requires_exclude ^/usr/bin/python$

%description
S3 Duck is a desktop client for Amazon S3 and S3-compatible storage:
browse buckets, transfer files, preview media and manage versions.

%prep

%build

%install
install -d %{buildroot}%{_prefix}/lib/s3duck
cp -a %{payload}/. %{buildroot}%{_prefix}/lib/s3duck/
# The desktop entry goes straight to its final place; the .deb has to move it
# in postinst, which is why stage_payload.sh also leaves a copy in the tree.
rm -f %{buildroot}%{_prefix}/lib/s3duck/s3duck.desktop
install -Dm644 %{srcdir}/resources/s3duck.desktop \
    %{buildroot}%{_datadir}/applications/s3duck.desktop
# The .deb launchers use `#!/bin/env bash`; written out here so the RPM does
# not depend on a usr-merged /bin.
install -d %{buildroot}%{_bindir}
printf '#!/usr/bin/bash\nexec python3 /usr/lib/s3duck/s3duck.py "$@"\n' \
    > %{buildroot}%{_bindir}/s3duck
printf '#!/usr/bin/bash\nexec python3 /usr/lib/s3duck/s3duck_cli.py "$@"\n' \
    > %{buildroot}%{_bindir}/s3duck-cli
chmod 755 %{buildroot}%{_bindir}/s3duck %{buildroot}%{_bindir}/s3duck-cli

# The desktop entry claims x-scheme-handler/s3, which only takes effect once
# the MIME cache knows about it.
%post
update-desktop-database -q %{_datadir}/applications &>/dev/null || :

%postun
update-desktop-database -q %{_datadir}/applications &>/dev/null || :

%files
%{_bindir}/s3duck
%{_bindir}/s3duck-cli
%{_prefix}/lib/s3duck/
%{_datadir}/applications/s3duck.desktop
