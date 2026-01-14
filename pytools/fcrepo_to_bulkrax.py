import csv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictReader
from datetime import datetime
from itertools import groupby
from pathlib import Path
from shutil import copy2
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

from pyoxigraph import Store


def batched(data: Iterable[tuple[Any, Any]], size: int) -> Iterator[Dict[Any, Any]]:
    """Based on itertools.batched, but designed to return batches from a dictionary."""
    if not isinstance(data, dict):
        raise TypeError
    batch = {}
    counter = 0
    for k, v in data.items():
        if counter < size:
            batch.update({k: v})
            counter += 1
        else:
            yield batch
            counter = 0
            batch = {}


def uri_to_id(uri: str | List[str]):
    if isinstance(uri, list):
        return [uri_to_id(element) for element in uri]
    return uri.split("/")[-1]


def is_active_embargo(record):
    return (
        datetime.fromisoformat(record["release_date"]).replace(tzinfo=None)
        >= datetime.now()
    )


class FedoraGraph:
    MEMBERSHIP_CHILD = "http://pcdm.org/models#memberOf"
    MEMBERSHIP_PARENT = "http://pcdm.org/models#hasMember"

    def __init__(
        self,
        path_to_graph,
        path_to_root,
        path_to_mapping,
        output_path: str,
        models,
        pipe_delimited: List[str] | str,
        batch_size: int = 50,
    ):
        """Provide a path to an Oxigraph RDF store, a path to a mapping of RDF predicates to Bulkrax fields, and a list satisfying the predicate info:fedora/fedora-system:def/model#hasModel for the types of works to be extracted.
        The mapping should be a CSV with headers "predicate" and "bulkrax_field".
        The list of models may either be a list of strings or a comma-separated string."""
        self.store = Store.read_only(str(path_to_graph))
        self.path_to_root = path_to_root
        self.output_path = output_path
        with open(path_to_mapping) as f:
            reader = DictReader(f)
            mapping = [r for r in reader]
        self.mapping = {row["predicate"]: row["bulkrax_field"] for row in mapping}
        if isinstance(models, str):
            self.models = models.split(",")
        else:
            self.models = models
        self.pipe_delimited = pipe_delimited
        self.batch_size = batch_size
        # Fedora graph data, URI's mapped to predicates
        # Load all data on initialization
        self.collections = self.get_resources()
        self.works = self.get_resources(self.models)
        self.file_sets = self.get_file_sets()
        self.permissions = self.get_group_permissions()
        self.embargos = self.get_embargos()
        self.parents = self.map_parents_children()

    def get_resources(
        self, models: Optional[List[str]] = None
    ) -> Dict[str, List[Tuple[str, str]]]:
        """Returns a map of resouurce ID's to their predicates."""
        if models:
            models_str = " ".join([f'"{model}"' for model in models])
            query = """
            prefix fedora: <info:fedora/fedora-system:def/model#>

            select distinct ?s ?p ?o
            where {{
                values ?model {{ {models} }}
                ?s ?p ?o.
                ?s fedora:hasModel ?model
            }}
            """.format(models=models_str)
        else:
            query = """
            prefix fedora: <info:fedora/fedora-system:def/model#>

            select ?s ?p ?o
            where {
                ?s fedora:hasModel "Collection".
                ?s ?p ?o

            }
            """
        results = sorted(
            [
                (r["s"].value, r["p"].value, r["o"].value)
                for r in self.store.query(query)
            ],
            key=lambda x: x[0],
        )
        return {k: list(g) for k, g in groupby(results, key=lambda x: x[0])}

    def select_latest_version(
        self, file_sets: List[Dict[str, str]]
    ) -> Iterator[Dict[str, str | datetime]]:
        """If necessary, select the latest version of each file, based on the Fedora modification date."""
        for _, group in groupby(
            sorted(file_sets, key=lambda x: x["file_uri"]), key=lambda x: x["file_uri"]
        ):
            # calculate latest version
            latest_version = {}
            for version in group:
                version["last_modified"] = datetime.fromisoformat(
                    version["last_modified"]
                )
                if not latest_version or (
                    latest_version["last_modified"] < version["last_modified"]
                ):
                    latest_version = version
            yield latest_version

    def map_parents_children(self) -> Dict[str, str]:
        """Matches parent works to their children."""
        children_query = """
            prefix pcdm: <http://pcdm.org/models#>
            prefix ns: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            select ?parent ?resource
            where {
                values  ?root { <http://localhost:8984/rest/prod> }
                ?parent pcdm:hasMember ?resource.
                ?resource ns:type <http://projecthydra.org/works/models#Work>.
                filter (?parent != ?root )

            }
        """
        child_to_parents = defaultdict(list)
        for row in self.store.query(children_query):
            child_to_parents[row["resource"].value].append(row["parent"].value)
        return child_to_parents

    def get_file_sets(self) -> Dict[str, List[Dict[str, str | datetime]]]:
        """Returns all filesets with references to parent works and file URI's. Selects the most recent version (by Fedora modification date) when multiple versions of a file exist."""
        fileset_query = """
            prefix fedora: <info:fedora/fedora-system:def/model#>
            PREFIX fedora_repo: <http://fedora.info/definitions/v4/repository#>
            prefix ns: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            prefix pcdm: <http://pcdm.org/models#>

            select distinct (?s as ?work) (?fs as ?fileset) (?fn as ?filename) (?fu as ?file_uri) # ?version ?modified
            where {
                ?s pcdm:hasMember ?fs.
                ?fs ns:type ?fm.
                ?fs fedora:downloadFilename ?fn.
                ?fs pcdm:hasFile ?fu.
                ?fu ns:type <http://pcdm.org/use#OriginalFile>.
                # ?fu fedora_repo:hasVersion ?version.
                # ?version fedora_repo:lastModified ?modified
                filter(str(?fm) = "http://projecthydra.org/works/models#FileSet")
            }
        """
        file_sets = [
            {
                "parents": r["work"].value,
                "id": r["fileset"].value,
                "file": r["filename"].value,  # Bulkrax CSV fields
                "title": r["filename"].value,  # Bulkrax CSV fields
                "file_uri": r["file_uri"].value,
                #      "version": r["version"].value,
                #      "last_modified": r["modified"].value,
            }
            for r in self.store.query(fileset_query)
        ]
        # group by parent work ID for batching
        return {
            k: list(g)
            for k, g in groupby(
                sorted(file_sets, key=lambda x: x["parents"]),
                key=lambda x: x["parents"],
            )
        }

    def get_group_permissions(self) -> Dict[str, List[str]]:
        """Returns group-level permissions mapped to the resource ID's they control."""
        query_agents = """
            prefix fedoraModel: <info:fedora/fedora-system:def/model#>
            prefix acl: <http://www.w3.org/ns/auth/acl#>

            select distinct ?agent ?resource

            where {

                ?s fedoraModel:hasModel ?model.
                ?s acl:agent ?agent.
                ?s acl:accessTo ?resource.
                filter(str(?model) = "Hydra::AccessControls::Permission")
                filter(contains(str(?agent), "group"))
            }
            """
        results = [
            (r["agent"].value, r["resource"].value)
            for r in self.store.query(query_agents)
        ]
        results = sorted(results, key=lambda x: x[1])
        permissions_per_resource = {}
        # Since a resource can have multiple permissions, return them as a list
        for k, g in groupby(results, key=lambda x: x[1]):
            permissions_per_resource[k] = []
            for row in g:
                permissions_per_resource[k].append(row[0])
        return permissions_per_resource

    def get_embargos(self) -> Dict[str, Dict[str, str]]:
        "Extracts embargo details, mapping to embargoed resource ID's."
        query_embargos = """
        prefix fedora_model: <info:fedora/fedora-system:def/model#>
        prefix hydra_acl: <http://projecthydra.org/ns/auth/acl#>
        prefix pcdm_model: <http://pcdm.org/models#>

        select distinct ?resource ?visibilityDuringEmbargo ?visibilityAfterEmbargo ?releaseDate
        where {
            ?s fedora_model:hasModel ?model.
            ?s hydra_acl:embargoReleaseDate ?releaseDate.
            ?s hydra_acl:visibilityDuringEmbargo ?visibilityDuringEmbargo.
            ?s hydra_acl:visibilityAfterEmbargo ?visibilityAfterEmbargo.
            ?resource hydra_acl:hasEmbargo ?s.
            filter(str(?model) = "Hydra::AccessControls::Embargo")
        }
        """
        results = {
            r["resource"].value: {
                "visibility_during_embargo": r["visibilityDuringEmbargo"].value,
                "visibility_after_embargo": r["visibilityAfterEmbargo"].value,
                "release_date": r["releaseDate"].value,
                "visibility": "embargo",
            }
            for r in self.store.query(query_embargos)
        }
        return results

    # TO DO: extract parent-child relationship for nested works
    def convert_resources(
        self, data: Dict[str, Dict[str, str]]
    ) -> Iterator[Dict[str, List[str]]]:
        """Converts each resources's set of triples into a dictionary mapping Bulkrax fields to values"""
        for _id, triples in data.items():
            row = {"id": _id}
            for _, predicate, value in triples:
                # Add a value to the parents columnn for any resources that belong to a collection
                if predicate == FedoraGraph.MEMBERSHIP_CHILD:
                    row["parents"] = row.get("parents", []) + [value]
                    continue
                bulkrax_field = self.mapping.get(predicate)
                if bulkrax_field:
                    row[bulkrax_field] = row.get(bulkrax_field, []) + [value]
            # Find any parent works (which are identified on the parent resource, not the child)
            parents = self.parents.get(_id)
            if parents:
                row["parents"] = row.get("parents", []) + parents
            yield row

    def format_row(
        self, row: Dict[str, List[str]], is_fileset=False
    ) -> Dict[str, str | List[str]]:
        """Formats each row for Bulkrax, extracting resource identifiers from URI's, and combining duplicate fields using either a semicolon or a pipe."""
        for field, value in row.items():
            if field in ["id", "parents"]:
                row[field] = uri_to_id(row[field])
            if field in self.pipe_delimited:
                row[field] = "|".join(value)
            elif isinstance(row[field], list):
                row[field] = "; ".join(value)
        # Use the resource ID for the Bulkrax identifier if not already present
        row["bulkrax_identifier"] = row.get("bulrax_identifier", row["id"])
        if is_fileset:
            # We don't want to provide ID's for FileSets
            # We don't need these metadata elements for fileset import
            for key in ["id", "version", "file_uri", "last_modified"]:
                if key in row:
                    del row[key]
        return row

    def match_permissions(
        self, rows: Iterable[Dict[str, str | List[str]]]
    ) -> Iterator[Dict[str, List[str]]]:
        """Match permissions to their associated resources, selecting for the highest level of visibility."""
        for resource in rows:
            permission = self.permissions.get(resource["id"])
            if permission:
                visibility = "private"
                for group_uri in permission:
                    group_id = uri_to_id(group_uri).split("#")[-1]
                    match group_id:
                        case "public":
                            visibility = "open"
                            break
                        case "registered":
                            visibility = "restricted"
                resource["visibility"] = visibility
            yield resource

    def match_embargos(
        self, rows: Iterable[Dict[str, str | List[str]]]
    ) -> Iterator[Dict[str, List[str]]]:
        """Match embargos to their associated resources, filtering for unexpired embargos."""
        for resource in rows:
            embargo = self.embargos.get(resource["id"])
            if embargo:
                if is_active_embargo(embargo):
                    resource.update(embargo)
                # If the embargo release date is in the past, update the visibility per the embargo instructions
                else:
                    resource["visibility"] = embargo["visibility_after_embargo"]
            yield resource

    def get_file_path(self, file_set: Dict[str, str]) -> Optional[Path]:
        """Construct the path to each (binary) file for copying."""
        file_path = urlparse(file_set["file_uri"]).path
        # Not necessary to use the version information to access the latest version of the binar
        # version_path = urlparse(file_set["version"])
        file_path = Path(self.path_to_root) / f"{file_path}.binary"
        if file_path.exists():
            return file_path

    def copy_files(
        self, path_to_destination: str, files: List[Tuple[str, str]]
    ) -> List[str]:
        """Copy binary files associated with filesets to the specified destination. Renames file using filename metadata."""
        output = []
        path_to_destination = Path(path_to_destination)
        if not path_to_destination.exists():
            path_to_destination.mkdir()
        for fs in files:
            try:
                out = copy2(fs[0], path_to_destination / fs[1])
            except Exception as e:
                continue
                # TO DO: log error
            output.append(out)
        return output

    def copy_files_concurrently(
        self, path_to_destination: str, file_sets: List[Tuple[str, str]]
    ):
        with ThreadPoolExecutor() as exe:
            # copy files in batches of 10
            futures = {
                exe.submit(
                    self.copy_files, path_to_destination, file_sets[i : i + 10]
                ): i
                for i in range(0, len(file_sets), 10)
            }
            for future in as_completed(futures):
                try:
                    data = future.result()
                except Exception as e:
                    continue
                    # TO DO: log error with index of batch
                else:
                    continue
                    # TO DO: log file paths

    def prepare_import_rows(
        self, data: Dict[str, List[str]]
    ) -> Iterator[Tuple[Dict[str, str], str]]:
        """Performs mapping of resource predicates to Bulkrax fields. For filets, return the URI representing a path to the binary file object as well as the mapped and formatted Bulkrax importer row."""
        # Add permissions and embargos to works and perform mapping from Fedora predicates to Bulkrax fields
        for row in self.match_embargos(
            self.match_permissions(self.convert_resources(data))
        ):
            # Find file sets associated with this work
            file_sets_for_row = self.file_sets.get(row["id"], [])
            # format fields/values for CSV
            yield self.format_row(row), None
            # format file sets for CSV and return binary file URI
            for fs in self.match_embargos(self.match_permissions(file_sets_for_row)):
                uri = fs["file_uri"]
                yield self.format_row(fs, is_fileset=True), uri

    def prepare_import(self):
        for data in [self.collections, self.works]:
            for i, batch in enumerate(batched(data.items(), self.batch_size)):
                # Files for this batch
                files = []
                for row, uri in self.prepare_import_rows(batch):
                    # Get path for copying binary
                    if uri:
                        # field "file" is the original filename
                        files.append((self.get_file_path(uri), row["file"]))
