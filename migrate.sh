#!/bin/bash

# exit when any command fails:
set -e
# line by line tracing
set -x

sub_export() {
    echo "Exporting objects from Fedora 4."
    mkdir -p /data/fedora-4.7.5-export
    java -jar fcrepo-import-export-0.3.0.jar \
     --dir /data/fedora-4.7.5-export \
     --user fedoraAdmin:fedoraAdmin \
     --mode export \
     --resource http://localhost:8984/rest \
     --binaries \
     --versions > /data/export_4_`date +%Y%m%dT%H%M%S`.log 2>&1
}

sub_to5() {
    echo "Migrating objects to Fedora 5."
    mkdir -p /data/fedora5
    java -jar fcrepo-upgrade-utils-6.3.0-AVALON.jar --input-dir /data/fedora-4.7.5-export --output-dir /data/fedora5 --source-version 4.7.5 --target-version 5+ > /data/upgrade_5_`date +%Y%m%dT%H%M%S`.log 2>&1

}

sub_to6() {
    echo "Migrating objects to Fedora 6/OCFL."
    mkdir -p /data/fedora6
    java --add-opens java.base/java.util.concurrent=ALL-UNNAMED -jar fcrepo-upgrade-utils-6.3.0-AVALON.jar --input-dir  /data/fedora5 --output-dir /data/fedora6  --source-version 5+ --target-version 6+ --base-uri http://localhost:8984/rest > /data/upgrade_6_`date +%Y%m%dT%H%M%S`.log 2>&1
}
subcommand=$1
case $subcommand in
        "export" | "to5" | "to6")
        shift
        sub_${subcommand}
        ;;
esac
