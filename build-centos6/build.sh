#!/bin/bash

cd $(dirname $0)

PYTHON_VERSION=$(cat ../.python-version)
DIST_NAME=dist-etl
DIST_PKG="$DIST_NAME".tar
ls "$DIST_PKG"* | xargs -n1 rm -rf

# Start VM
vagrant up

# Run in VM 
COMMANDS=$(cat <<EOF
	echo "Preparing environment to python $PYTHON_VERSION...";
	cd /vagrant/build-centos6;
	pyenv shell $PYTHON_VERSION;

	echo "Updating Python project dependencies...";
	pip install -r ../requirements.txt;

	echo "Packaging with pyinstaller...";
	rm -rf dist* etl*
	pyinstaller -y ../main.py;

	echo "Building a tar.gz ...";
	mv dist/main etl;
	tar cf "$DIST_PKG" etl;
EOF
)
eval "vagrant ssh -c '$COMMANDS'"

# Copy the distribution package from the VM to local FS
vagrant ssh-config > /tmp/vagrant-ssh-config.txt
scp -F /tmp/vagrant-ssh-config.txt -r default:/vagrant/build-centos6/"$DIST_PKG" ./

# Stop VM
vagrant halt &

# Add config files
tar rf "$DIST_PKG" ../config
tar rf "$DIST_PKG" ../sources
gzip "$DIST_PKG"

wait