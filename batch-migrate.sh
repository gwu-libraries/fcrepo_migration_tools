#!/bin/bash

# exit when any command fails:
set -e
# line by line tracing
set -x

sub_prepare() {
    # Prepare list of zipped files for ingest
    ls /data/migration/bulkrax-imports/*.zip  > /data/migration/bulkrax-imports/zip_files.txt
    # Replace paths with those for use inside the container
    sed -i 's/\/data\/migration\/bulkrax-imports\//tmp\/imports\//'
}

sub_run_import() {
    # Pop one line off the list
    nextfile=`sed -e \\\$$'{w/dev/stdout\n;d}' -i~ /data/migration/bulkrax-imports/zip_files.txt`
    # If not empty, run the Bulkrax import task
    [[ ! -z ${nextfile//$'\n'/} ]] && nextfile=`sed -e \\\$$'{w/dev/stdout\n;d}' -i~ /data/migration/bulkrax-imports/zip_files.txt`

}
subcommand=$1
case $subcommand in
        "prepare" | "run_import" )
        shift
        sub_${subcommand}
        ;;
esac
