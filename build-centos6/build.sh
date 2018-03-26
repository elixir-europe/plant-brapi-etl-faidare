#!/bin/bash

cd $(dirname $0)
rm -rf dist-etl
vagrant up
vagrant ssh -c 'pyenv shell 3.6.4; cd /vagrant/build-centos6; pyinstaller -y ../main.py'
vagrant ssh-config > /tmp/config.txt
scp -F /tmp/config.txt -r default:/vagrant/build-centos6/dist/main dist-etl
vagrant halt