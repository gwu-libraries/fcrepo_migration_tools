### Migrating GW ScholarSpace Fedora Repository

This repo contains a few scripts to facilitate the migration of content from our Hyrax 3.6 version of GW ScholarSpace (using Fedora 4.75) to Hyrax 5.x/Fedora 6.x. 

The outline of the process is as follows:

1. Use the community-provided Fedora export utility to export the Fedora 4 repository objects, including binaries.
2. Use the included Python scripts to generate an RDF database from the exported triples, and to generate batched import files for Bulkrax from the RDF metadata and the exported binaries.
3. Load the import batches into a Hyrax 5 app using Bulkrax.

#### Setup

The following steps presuppose a Dockerized application running on an Ubuntu server.

On the server where the Fedora 4 container is running, clone this repo:

`git clone https://github.com/gwu-libraries/fcrepo_migration_tools.git`

#### Stage 1: Export the Fedora 4 repository

This stage must be run *outside* of a Docker container (alternately, within the Fedora 4 container itself) because of the way in which the export tool creates the object URI's (which must contain `localhost` in order to be valid through the migration process).

It's recommended that the following process, as well as the subsequent upgrade steps, be run in detachable shell process (using `tmux` or similar).

1. Install Java 11 on the server, if necessary: `sudo apt-get install openjdk-11-jre-headless`

2. Download the Fedora export utility: `wget https://github.com/fcrepo-exts/fcrepo-import-export/releases/download/fcrepo-import-export-0.3.0/fcrepo-import-export-0.3.0.jar`

3. If a `/data` directory does not exist at root, create it.

4. Create `fedora-4.7.5-export` directory under `/data` to hold the exported objects..

5. Run the first export. This will export the `/rest/prod` object and all its descendants to `/data/migration/fedora-4.7.5-export`: `bash ./migrate.sh export`.
   - The logs from the export utility can be found at `./export_4_[timestamp].log`.

Because we want to exclude objects outside of `/rest/prod` from the migration (including a very large number of objects nested under `/rest/audit`), we need to export the `/rest` object separately and then remove links to any children other than `/rest/prod`. (The upgrade utility needs `rest.ttl` to be present at the top level of the exported objects.)

6. Run `bash ./bash ./migrate.sh export_rest`

7. Run the following script, providing the path to the exported `rest.ttl` file from step 6: `docker run --rm -v /data/fedora-4.7.5-export:/data fcrepo_pytools remove-audits --ttl /data/rest.ttl`
  - If you `cat` the `rest.ttl` file in the `/data/fedora-4.7.5-export` directory, you should see only one `ldp:contains` line: `ldp:contains <http://localhost:8984/rest/prod> ;`
