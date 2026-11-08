# syntax=docker/dockerfile:1
# The .rpm, built on the distribution it targets rather than with whatever
# rpmbuild the host has: Fedora's macros decide what a noarch package looks
# like. Base image pinned by digest; only the .rpm leaves the build.
#
# Build with `make rpm` (BuildKit required). The package lands in dist/.

FROM fedora:43@sha256:f01112b9a1f1d0504ad962d9adfa401425033f8ab067e0a821904ebdde5b8ca5 AS build

RUN dnf install -y --setopt=install_weak_deps=False rpm-build findutils \
 && dnf clean all

WORKDIR /src
COPY . .

RUN --network=none \
    version=$(sed -n 's/^__VERSION__ = "\(.*\)"$/\1/p' version.py) \
 && test -n "$version" \
 && bash tools/stage_payload.sh /tmp/payload \
 && rpmbuild -bb packaging/rpm/s3duck.spec \
        --define "_topdir /tmp/rpmbuild" \
        --define "version $version" \
        --define "payload /tmp/payload" \
        --define "srcdir /src" \
 && mkdir /out && cp /tmp/rpmbuild/RPMS/noarch/*.rpm /out/

FROM scratch AS artifact
COPY --from=build /out/ /
