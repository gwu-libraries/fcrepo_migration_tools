### Migrating GW ScholarSpace Fedora Repository

This repo contains a few scripts to facilitate the migration of content from Fedora 4.7.5 (used with GW ScholarSpace 2.3, Hyrax 3.6) to Fedora 6.x (for use with Hyrax 5). These scripts use versions of the community-provided Fedora export and upgrade tools, while performing some data cleanup on the Fedora 4 repository's contents.

#### Setup

The following steps presuppose a Dockerized application running on an Ubuntu server.

On the server where the Fedora 4 container is running, clone this repo:

`git clone https://github.com/gwu-libraries/fcrepo_migration_tools.git`

#### Stage 1: Remove orphaned objects

Our Fedora 4 repository contains a number of objects that will cause errors in the Fedora 6 indexing process. These are objects with a Fedora model of `ActiveFedora::Aggregation::Proxy` that lack a valid `proxyFor` relation.

Hyrax uses such Proxy objects to link `Work` objects to `FileSet` objects. These orphans are associated with works but not with file sets, either because the work has no files, or because other proxy objects exist for the same work linking to the associated files. Deleting them appears to have no effect on the behavior of the works with which they are associated.

Deleting these orphans *prior* to exporting the objects from the repository is the best approach (since Fedora will delete the links to these objects as well).

1. Download the [list of URIs](https://drive.google.com/file/d/1ipTGORLiCuLQan00DLha0TD1ges9gt7I/view?usp=drive_link) for orphaned objects and place a copy in the `fcrepo_migration_tools` directory on the server. (These URIs were obtained by querying an RDF graph derived from a prior export of the Fedora 4 repo. See the `migration-troubleshooting` Python notebook in this GitHub repo for details on the query.)

2. Build the Python tools Docker image: `docker buildx build -f Dockerfile-pytools -t fcrepo_pytools .`

3. Ensure that the Fedora 4 container is running: `docker ps`.

4. Run the script to remove the orphans, providing the path to the list of URIs:
  ```
  docker run --rm \
              --network="host" \
              -v ./orphan-objects.txt:/data fcrepo_pytools \
              remove-orphans \
              --objects /data/orphan-objects.txt
  ```
The script should report the progress of deleting each URI and complete without errors.

#### Stage 2: Export the Fedora 4 repository

This stage must be run *outside* of a Docker container (alternately, within the Fedora 4 container itself) because of the way in which the export tool creates the object URI's (which must contain `localhost` in order to be valid through the migration process).

It's recommended that the following process, as well as the subsequent upgrade steps, be run in detachable shell process (using `tmux` or similar).

1. Install Java 11 on the server, if necessary: `sudo apt-get install openjdk-11-jre-headless`

2. Download the Fedora export utility: `wget https://github.com/fcrepo-exts/fcrepo-import-export/releases/download/fcrepo-import-export-0.3.0/fcrepo-import-export-0.3.0.jar`

3. If a `/data` directory does not exist at root, create it.

4. Create `fedora-4.7.5-export` directory under `/data` to hold the exported objects..

5. Run the first export. This will export the `/rest/prod` object and all its descendants to `/data/fedora-4.7.5-export`: `bash ./migrate.sh export`.
   - The logs from the export utility can be found at `./export_4_[timestamp].log`.

Because we want to exclude objects outside of `/rest/prod` from the migration (including a very large number of objects nested under `/rest/audit`), we need to export the `/rest` object separately and then remove links to any children other than `/rest/prod`. (The upgrade utility needs `rest.ttl` to be present at the top level of the exported objects.)

6. Run `bash ./bash ./migrate.sh export_rest`

7. Run the following script, providing the path to the exported `rest.ttl` file from step 6: `docker run --rm -v /data/fedora-4.7.5-export:/data fcrepo_pytools remove-audits --ttl /data/rest.ttl`
  - If you `cat` the `rest.ttl` file in the `/data/fedora-4.7.5-export` directory, you should see only one `ldp:contains` line: `ldp:contains <http://localhost:8984/rest/prod> ;`

#### Stage 3: Upgrade the Fedora 4 objects

The upgrade is a two-step process. Both steps can be run within a Docker container for convenience.

You can shut down the Fedora 4 container at this stage; it's no longer necessary to have Fedora 4 running.

1. Build the migration tools Docker image: `docker buildx build -t fcrepo-migration-tools .`

2. Execute the first migration stage: `docker run --rm -v /data:/data fcrepo-migration-tools ./migrate.sh to5`
   - The logs from this stage will be written to `/data/fedora5_upgrade_[timestamp].log`

3. Once the first has completed without errors, execute the second migration stage: `docker run --rm -v /data:/data fcrepo-migration-tools ./migrate.sh to6`
   - The logs from this stage will be written to `/data/fedora6_upgrade_[timestamp].log`

#### Stage 4: Create the Fedora 6 repository

Fortunately, there's nothing to import at this stage: pointing a Fedora 6.x repo to a directory with OCFL objects will suffice to populate it. However, Fedora 6 does need to run a reindexing job on the content initially, which can take several hours.

1. Make sure the previous Fedora 4/GWSS containers are stopped, including any postgres containers.

2. Run `docker compose -f docker-compose-fcrepo.yml up -d` to start a Fedora 6 container and postgres container.

This Fedora 6 container is built from a [patch](https://github.com/fcrepo/fcrepo/commit/dcee5f0745201e0d2b4fe171b8a698675e56475a) of the Fedora code that has not yet been released. The patch includes a check during the indexing process that will report any errors with orphaned objects, instead of failing.

3. Monitoring the Docker logs (`docker logs fedora -f`), you should see the reindexing job run. Upon completion, examine the logs for errors:
   - `docker logs fedora >& fedora6-reindex.log`
   - `grep "ERROR" fedora6-reindex.log `

If no errors are present, reindexing has completed successfully, and the Fedora 6 repository is ready for use.
