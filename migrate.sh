#!/bin/bash

# exit when any command fails:
set -e
# line by line tracing
set -x

sub_install() {
    # For installing the exporter outside of a Docker container
    # Installing Java
    sudo apt-get -y install openjdk-11-jre-headless
    # Creating the directory to hold the export
    sudo mkdir -p /data/fedora-4.7.5-export && sudo chown -R ${USER:=$(/usr/bin/id -run)}:$USER /data/fedora-4.7.5-export/
    # Downloading the export utility
    wget https://github.com/fcrepo-exts/fcrepo-import-export/releases/download/fcrepo-import-export-0.3.0/fcrepo-import-export-0.3.0.jar
}

sub_export() {
    # Should be run *outside* of a Docker container, otherwise, localhost will be unreachable
    echo "Exporting objects from Fedora 4."
    java -jar fcrepo-import-export-0.3.0.jar \
         --dir /data/migrate/fedora-4.7.5-export \
         --user fedoraAdmin:fedoraAdmin \
         --mode export \
         --resource http://localhost:8984/rest/prod \
         --binaries \
         --versions > ./export_4_`date +%Y%m%dT%H%M%S`.log 2>&1
}

sub_export_rest() {
    # Exports just the rest.ttl object (for editing to exclude non /prod objects)
    # Should be run *outside* of a Docker container
    java -jar fcrepo-import-export-0.3.0.jar \
         --dir /data/migrate/fedora-4.7.5-export \
         --user fedoraAdmin:fedoraAdmin \
         --mode export \
         --resource http://localhost:8984/rest \
         --predicates http://example.org/fake \
         --binaries \
         --versions > ./export_4_`date +%Y%m%dT%H%M%S`.log 2>&1

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
        "export" | "install" | "export_rest" | "to5" | "to6")
        shift
        sub_${subcommand}
        ;;
esac
