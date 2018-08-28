# Build self contained package

This folder contains scripts and configuration to build the ETL as a
self contained binary package (with embedded dependencies and python VM)
for CentOS 6.

CentOS 6 was chosen as a build target for compatibility issue but the
binary package should work on more recent linux distributions.

This build is achieved using [pyinstaller](https://www.pyinstaller.org/) 
in a CentOS 6 [Vagrant](https://www.vagrantup.com) VM (**Vagrant must be
installed before running the build script**).

```sh
./build.sh
```

This script will:
 - (On the first time) Build a CentOS 6 Vagrant VM
    - Download & Install CentOS 6
    - Install python
    - Install dependencies and `pyinstaller`
 - Build the binary distribution with `pyinstaller` on `../etl/main.py`
 - Package the binaries and configuration files into an archive in [../dist](../dist)

See [../README.md](../README.md) to check how to use the binary distribution.

You can use `vagrant destroy` to delete the Vagrant VM if you want to
re-generate it from scratch or if you need to free disk space.