# Usage
# To run the migration process, run this image once per stage. The /data bound volume should be the directory where you want the migrated data to reside.
# 0. Ensure that the existing Fedora4 container is running and has the 8984 port open to the host network.
# 1. Build the image: docker buildx build -t fcrepo-migration-tools .
# 2. Perform the export: docker run --rm -v /data:/data fcrepo-migration-tools ./migrate.sh export
# 3. Execute the first migration stage: docker run --rm -v /data:/data fcrepo-migration-tools ./migrate.sh to5
# 4. Execute the second migration stage: docker run --rm -v /data:/data fcrepo-migration-tools ./migrate.sh to6
# 5. Logs from each stage can be found in the /data directory.
# 6. Remove the Fedora 4 container and the associated postgres container.
# 7. Start Fedora 6: docker compose -f docker-compose-fcrepo.yml up -d
# 8. Monitor the logs as the reindexing job runs.


FROM fcrepo/fcrepo:6.5.1-tomcat9

# Dependency for adding update WAR file
RUN apt-get update && apt-get install unzip

# Patch (courtesy of Ben Pennel) not yet incorporated into official Docker image
COPY ./fcrepo-webapp-6.5.1.war /tmp

# Unzip WAR file contents to app directory
RUN unzip -q -o -d /usr/local/tomcat/webapps/fcrepo /tmp/fcrepo-webapp-6.5.1.war

# Directory for migration tooling
WORKDIR /opt/fedora-migration-tools

# Patched version
RUN wget https://github.com/avalonmediasystem/fcrepo-upgrade-utils/releases/download/6.3.0-AVALON/fcrepo-upgrade-utils-6.3.0-AVALON.jar

# Per Feddora documentation
RUN wget https://github.com/fcrepo-exts/fcrepo-import-export/releases/download/fcrepo-import-export-0.3.0/fcrepo-import-export-0.3.0.jar

# Utility script
COPY ./migrate.sh ./

RUN chmod +x ./migrate.sh
