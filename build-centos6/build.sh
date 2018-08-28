#!/bin/bash

cd $(dirname $0)

# Should match the version in the Vagrantfile provisioning
PYTHON_VERSION=3.6.4

DIST_DIR=../dist
DIST_NAME=plant-brapi-etl-data-lookup-gnpis

TAR_FILE=${DIST_NAME}.tar
TGZ_FILE=${DIST_NAME}.tar.gz

find ${DIST_DIR} -name "${DIST_NAME}*" | xargs -n1 rm -rf

# Start VM
vagrant up

# Run in VM 
COMMANDS=$(cat <<EOF
	echo "Preparing environment to python ${PYTHON_VERSION}...";
	cd /vagrant/build-centos6;
	pyenv shell ${PYTHON_VERSION};

	echo "Updating Python project dependencies...";
	pip install -r ../requirements.txt;

	echo "Packaging with pyinstaller...";
	rm -rf dist* etl*
	pyinstaller -y ../main.py;

	echo "Building a tar.gz ...";
	mv dist/main etl;
	tar cf ${TAR_FILE} etl;
EOF
)
eval "vagrant ssh -c '${COMMANDS}'"

# Copy the distribution package from the VM to local FS
vagrant ssh-config > /tmp/vagrant-ssh-config.txt
scp -F /tmp/vagrant-ssh-config.txt -r default:/vagrant/build-centos6/${TAR_FILE} ${DIST_DIR}/

# Stop VM
vagrant halt &

# Add config files
tar rf ${DIST_DIR}/${TAR_FILE} ../config
tar rf ${DIST_DIR}/${TAR_FILE} ../sources
gzip ${DIST_DIR}/${TAR_FILE}

wait