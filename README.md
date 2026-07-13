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

Build the image:

`docker buildx build -t Dockerfile-pytools -t fcrepo_pytools .`

#### Stage 1: Export the Fedora 4 repository

This stage must be run *outside* of a Docker container (alternately, within the Fedora 4 container itself) because of the way in which the export tool creates the object URI's (which must contain `localhost` in order to be valid through the migration process).

It's recommended that the following process, as well as the subsequent upgrade steps, be run in detachable shell process (using `tmux` or similar).

1. Install Java 11 on the server, if necessary: `sudo apt-get install openjdk-11-jre-headless`

2. Download the Fedora export utility: `wget https://github.com/fcrepo-exts/fcrepo-import-export/releases/download/fcrepo-import-export-0.3.0/fcrepo-import-export-0.3.0.jar`

3. If a `/data/migration` directory does not exist at root, create it.

4. Create `fedora-4.7.5-export` directory under `/data/migration` to hold the exported objects..

5. Run the first export. This will export the `/rest/prod` object and all its descendants to `/data/migration/fedora-4.7.5-export`: `bash ./migrate.sh export`.
   - The logs from the export utility can be found at `./export_4_[timestamp].log`.

Because we want to exclude objects outside of `/rest/prod` from the migration (including a very large number of objects nested under `/rest/audit`), we need to export the `/rest` object separately and then remove links to any children other than `/rest/prod`. (The upgrade utility needs `rest.ttl` to be present at the top level of the exported objects.)

6. Run `bash ./bash ./migrate.sh export_rest`

7. Run the following script, providing the path to the exported `rest.ttl` file from step 6: `docker run --rm -v /data/fedora-4.7.5-export:/data fcrepo_pytools remove-audits --ttl /data/rest.ttl `
  - If you `cat` the `rest.ttl` file in the `/data/fedora-4.7.5-export` directory, you should see only one `ldp:contains` line: `ldp:contains <http://localhost:8984/rest/prod> ;`

8. At this point, you can stop the running Fedora 4 service. The rest of the migration prep uses the exported `.ttl` files.

9. Prepare a directory on `/data` to hold the files prepared for import. (This may be a shared mount to an NFS volume, etc.) In what follows, it is assumed that this directory is called `/data/migration/bulkrax-imports`.

10. Prepare the RDF graph from the exported files. This step will walk all subdirectories under the export Fedora repository root, loading `.ttl` files into an RDF database (using [Pyoxigraph](https://pyoxigraph.readthedocs.io/en/stable/).
    - Make a directory on `/data` to hold the graph data, e.g., `mkdir /data/gwss-rdf`.
    - Run the Dockrized Python script: `docker run --rm -v /data:/data fcrepo_pytools parse-graph --root /data/fedora-4.7.5-export --output /data/gwss-rdf`

11. Update mappings and prepare the change set and config files:
    - `fedora_bulkrax_mapping.csv`: Maps Hyrax predicates to Bulkrax columns. May require customization depending on the exact version of Hyrax you are migrating from and the configuration of the metadata in your local repository.
    - `bulkrax_change_set.csv`: As needed, this file can be populated with identifiers for works and collections (NOT filesets) that require metadata updates during migration. For each resource to be changed, create a new row, and provide one or more new values to be inserted in columns matching columns in a Bulkrax import. For more detailed instructions, see the comments in `pytools/fcrepo_to_bulkrax.py` under the `ChangeSet` class definition.
    - `fcrepo_to_bulkrax.yml`: Ensure paths are correct to configuration files, input and output directories, and custom resource classes to be migrated. Note that paths are relative to the Docker container, not the host.

11. For each Admin Set in the repository, run the following script, updating the argument to the `--admint-set` option accordingly. 
