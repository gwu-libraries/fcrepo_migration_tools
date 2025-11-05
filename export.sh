#!/bin/bash

# exit when any command fails:
set -e
# line by line tracing
set -x

sudo apt-get -y install openjdk-11-jre-headless

sudo chown -R ubuntu:ubuntu /data/fedora-4.7.5-export/

wget https://github.com/fcrepo-exts/fcrepo-import-export/releases/download/fcrepo-import-export-0.3.0/fcrepo-import-export-0.3.0.jar

java -jar fcrepo-import-export-0.3.0.jar \
     --dir /data/fedora-4.7.5-export \
     --user fedoraAdmin:fedoraAdmin \
     --mode export \
     --resource http://localhost:8984/rest/prod \
     --binaries \
     --versions > ./export_4_`date +%Y%m%dT%H%M%S`.log 2>&1
