# Build self contained package

The folder contains scripts and configuration to build the ETL as a
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
 - (if first time) Build a CentOS 6 Vagrant VM
    - Download & Install CentOS 6
    - Install python
    - Install dependencies and `pyinstaller`
 - Run `pyinstaller` to build a binary distribution on `../etl/main.py`

The binary distribution will be available in the `dist-etl` folder and
can be executed as:

```sh
./dist-etl/main
```

You can use `vagrant destroy` to delete the Vagrant VM if you want to
re-generate it from scratch or if you need to free disk space.