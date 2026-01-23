import csv
import logging
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictReader, DictWriter
from dataclasses import dataclass, field, make_dataclass
from datetime import datetime
from io import StringIO
from itertools import chain, groupby
from pathlib import Path
from shutil import copy2
from typing import Any, ClassVar, Dict, Iterable, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlparse
from zipfile import ZipFile

from more_itertools import before_and_after
from pyoxigraph import Literal, Store

logger = logging.getLogger(__name__)


class Resource:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def make_resource(
        cls, id: str, triples: Iterator, mapping: Dict[str, Tuple[str, bool]]
    ):
        """
        Helper method for creating instances of resource classes from Fedora object models
        """
        kwargs = defaultdict(list)
        for triple in triples:
            if triple["p"].value in mapping:
                # Get Bulkrax field from RDF predicate
                (field_name, multiple) = mapping[triple["p"].value]
                # Array fields
                if multiple:
                    kwargs[field_name].append(triple["o"].value)
                # Single-value fields
                else:
                    kwargs[field_name] = triple["o"].value
        # Expect the AdminSet name to be the final element in each "triple"
        # We don't need to include it in the import CSV, but it needs to be set at time of import
        admin_set = triple["adminSet"].value if triple["adminSet"] else None
        return cls(id=id, admin_set=admin_set, **kwargs)

    def format_row(self, formatter):
        return dict(
            [formatter(k, v) for k, v in self.__dict__.items() if k != "admin_set"]
        )


class Work(Resource):
    pass


class Collection(Resource):
    pass


@dataclass
class FileSet:
    """
    Dataclass for FileSet metadata. No mapping here -- predicates are captured and mapped during the Sparql query
    """

    parents: str  # Parent = work ID
    id: str
    file: str  # File name
    title: str
    file_uri: str  # URI to binary resource
    model: str = "FileSet"

    @staticmethod
    def make_fileset(triple):
        parents, id, file, file_uri = (
            triple["work"].value,
            triple["fileset"].value,
            triple["filename"].value,
            triple["file_uri"].value,
        )
        return FileSet(parents=parents, id=id, file=file, title=file, file_uri=file_uri)

    def get_file_path(self, path_to_root: str) -> Optional[Path]:
        """Construct the path to each (binary) file for copying."""
        file_path = urlparse(self.file_uri).path[1:]
        # Not necessary to use the version information to access the latest version of the binar
        # version_path = urlparse(file_set["version"])
        file_path = Path(path_to_root) / f"{file_path}.binary"
        if file_path.exists():
            return file_path
        else:
            pass

    def format_row(self, formatter):
        return dict(
            [formatter(k, v) for k, v in self.__dict__.items() if k != "file_uri"]
        )


class PermissionsMapping:
    """
    Expect each resource to have an array of permission groups.
    """

    def __init__(self):
        self.permissions_per_resource = {}

    def make_mapping(self, results: Iterator[Tuple[str, str]]):
        # Since a resource can have multiple permissions, store them as a list
        # Group by resource ID
        #  (r["agent"].value, r["resource"].value
        for k, g in groupby(results, key=lambda r: r["resource"].value):
            self.permissions_per_resource[k] = []
            for row in g:
                self.permissions_per_resource[k].append(row["agent"].value)
        return self

    def update_resource(self, resource):
        permission = self.permissions_per_resource.get(resource.id)
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
            resource.visibility = visibility
        return resource


class EmbargoMapping:
    """
    Expect each resource to have at most one embargo.
    """

    def __init__(self):
        self.embargo_per_resource = {}

    def make_mapping(self, results: Iterator):
        for r in results:
            self.embargo_per_resource[r["resource"].value] = {
                "visibility_during_embargo": r["visibilityDuringEmbargo"].value,
                "visibility_after_embargo": r["visibilityAfterEmbargo"].value,
                "release_date": r["releaseDate"].value,
                "visibility": "embargo",
            }
        return self

    def update_resource(self, resource):
        embargo = self.embargo_per_resource.get(resource.id)
        if embargo:
            if is_active_embargo(embargo):
                for field, value in embargo.items():
                    setattr(resource, field, value)
            # If the embargo release date is in the past, update the visibility per the embargo instructions
            else:
                resource.visibility = embargo["visibility_after_embargo"]
        return resource


class ParentChildMapping:
    """
    Maps child works to parents. Assumes a single work can have multiple parents.
    """

    def __init__(self):
        self.parent_child_mapping = defaultdict(list)

    def make_mapping(self, results: Iterator):
        for row in results:
            self.parent_child_mapping[row["resource"].value].append(row["parent"].value)
        return self

    def update_resource(self, resource):
        parents = self.parent_child_mapping.get(resource.id)
        if parents:
            resource.parents = parents
        return resource


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
    # MEMBERSHIP_PARENT = "http://pcdm.org/models#hasMember"

    def __init__(
        self,
        path_to_graph,
        path_to_root,
        output_path,
        path_to_mapping: str,
        models: List[str] | str,
        admin_set: Optional[str] = None,
        pipe_delimited: Optional[List[str]] = None,
        batch_size: int = 50,
    ):
        """Provide a path to an Oxigraph RDF store, a path to a mapping of RDF predicates to Bulkrax fields, and a list satisfying the predicate info:fedora/fedora-system:def/model#hasModel for the types of works to be extracted.
        The mapping should be a CSV with headers "predicate" and "bulkrax_field".
        The list of models may either be a list of strings or a comma-separated string."""
        try:
            self.store = Store.read_only(str(path_to_graph))
        except Exception as e:
            logger.error(f"Unable to load graph data from {path_to_graph}.", e)
            raise
        self.path_to_root = path_to_root
        self.output_path = Path(output_path)
        try:
            self.mapping = FedoraGraph.load_mapping(path_to_mapping)
        except Exception as e:
            logger.error(f"Unable to load mapping file from {path_to_mapping}", e)
            raise
        # Add the predicate the connects child works to parents
        if FedoraGraph.MEMBERSHIP_CHILD not in self.mapping:
            self.mapping[FedoraGraph.MEMBERSHIP_CHILD] = ("parents", True)
        if isinstance(models, str):
            self.models = models.split(",")
        else:
            self.models = models
        self.admin_set = admin_set
        self.pipe_delimited = pipe_delimited if pipe_delimited else []
        self.batch_size = batch_size
        # Fedora graph data, URI's mapped to predicates
        # Load permissions and embargo data on initialization
        self.permissions = self.get_permissions()
        self.embargos = self.get_embargos()
        self.parents = self.get_parents()

    @staticmethod
    def load_mapping(path_to_mapping: str) -> Dict[str, tuple[str, bool]]:
        with open(path_to_mapping) as f:
            reader = DictReader(f)
            mapping = [r for r in reader]
        return {
            str(row["predicate"]): (
                str(row["bulkrax_field"]),
                True if str(row["multiple"]).lower() == "true" else False,
            )
            for row in mapping
        }

    def get_resources(self, model) -> Iterator:
        """
        Returns all works, optionally limited to a given Hyrax AdminSet
        """
        admin_set_values = ""
        admin_set_criteria = ""
        if model == Collection:
            models_str = '"Collection"'
        else:
            models_str = " ".join([f'"{model}"' for model in self.models])
            if self.admin_set:
                admin_set_values = "values ?adminSet {{ {admin_set} }}".format(
                    admin_set=self.admin_set
                )
            admin_set_criteria = """?s partOf: ?a.
            ?a fedora:hasModel ?adminSetModel.
            ?a title: ?adminSet"""

        query = """
            prefix fedora: <info:fedora/fedora-system:def/model#>
            prefix pcdm: <http://pcdm.org/models#>
            prefix partOf: <http://purl.org/dc/terms/isPartOf>
            prefix title: <http://purl.org/dc/terms/title>

            select distinct ?s ?p ?o ?adminSet
            where {{
               values ?model {{ {models} }}
               values ?adminSetModel {{ "AdminSet" }}
               {admin_set_values}
               ?s ?p ?o.
               ?s fedora:hasModel ?model.
               {admin_set_criteria}
            }}
            order by ?s
        """.format(
            models=models_str,
            admin_set_values=admin_set_values,
            admin_set_criteria=admin_set_criteria,
        )

        # Return results grouped by ID
        for k, g in groupby(self.store.query(query), key=lambda r: r["s"].value):
            yield model.make_resource(id=k, triples=g, mapping=self.mapping)

    def get_filesets(self) -> Iterator[Tuple[str, FileSet]]:
        """Returns all filesets with references to parent works and file URI's."""
        if self.admin_set:
            admin_set_values = """
          values ?adminSetModel {{ "AdminSet" }}
          values ?adminSet {{ {admin_set} }}
          """.format(admin_set=self.admin_set)
            admin_set_criteria = """
          ?s partOf: ?a.
          ?a fedora:hasModel ?adminSetModel.
          ?a title: ?adminSet
          """
        else:
            admin_set_values, admin_set_criteria = "", ""
        fileset_query = """
          prefix fedora: <info:fedora/fedora-system:def/model#>
          PREFIX fedora_repo: <http://fedora.info/definitions/v4/repository#>
          prefix ns: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          prefix pcdm: <http://pcdm.org/models#>

          select distinct (?s as ?work) (?fs as ?fileset) (?fn as ?filename) (?fu as ?file_uri)
          where {{
                {admin_set_values}
                ?s pcdm:hasMember ?fs.
                ?fs ns:type ?fm.
                ?fs fedora:downloadFilename ?fn.
                ?fs pcdm:hasFile ?fu.
                ?fu ns:type <http://pcdm.org/use#OriginalFile>.
                {admin_set_criteria}
                filter(str(?fm) = "http://projecthydra.org/works/models#FileSet")
            }}
            order by ?s
      """.format(
            admin_set_values=admin_set_values, admin_set_criteria=admin_set_criteria
        )
        # group by parent work ID for batching
        for k, g in groupby(
            self.store.query(fileset_query), key=lambda r: r["work"].value
        ):
            for triple in g:
                yield FileSet.make_fileset(triple)

    def get_parents(self) -> ParentChildMapping:
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
        return ParentChildMapping().make_mapping(self.store.query(children_query))

    def get_permissions(self) -> PermissionsMapping:
        """Creates mapping of group-level permissions mapped to the resource ID's they control."""
        query = """
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
            order by ?resource
            """

        return PermissionsMapping().make_mapping(self.store.query(query))

    def get_embargos(self) -> EmbargoMapping:
        "Extracts embargo details, mapping to embargoed resource ID's."
        query = """
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
        return EmbargoMapping().make_mapping(self.store.query(query))

    def format_for_bulkrax(self, key: str, value: str | List[str]) -> Tuple[str, str]:
        """Formats each value for Bulkrax, extracting resource identifiers from URI's, and combining duplicate fields using either a semicolon or a pipe."""
        if key in ("id", "parents"):
            value = uri_to_id(value)
            if isinstance(value, list):
                value = ";".join(value)
            if key == "id":
                return "bulkrax_identifier", value
            else:
                return key, value
        elif (key in self.pipe_delimited) and isinstance(value, list):
            return key, "|".join(value)
        elif isinstance(value, list):
            return key, "; ".join(value)
        else:
            return key, value

    def copy_files(self, path_to_destination: Path, files: List[FileSet]) -> List[str]:
        """Copy binary files associated with filesets to the specified destination. Renames file using filename metadata."""
        output = []
        pd = Path(path_to_destination)
        if not pd.exists():
            pd.mkdir()
        for fs in files:
            try:
                file_path = fs.get_file_path(self.path_to_root)
                out = copy2(file_path, pd / fs.file)
            except Exception as e:
                error_msg = (
                    f"Unable to copy file {str(file_path)} to {str(pd / fs.file)}"
                )
                logger.error(error_msg, e)
                raise
            output.append(out)
        return output

    def copy_files_concurrently(
        self, path_to_destination: Path, file_sets: List[FileSet], batch_num: int
    ):
        with ThreadPoolExecutor() as exe:
            # copy files in batches of 10
            data = []
            futures = {
                exe.submit(
                    self.copy_files, path_to_destination, file_sets[i : i + 10]
                ): i
                for i in range(0, len(file_sets), 10)
            }
            for future in as_completed(futures):
                try:
                    data.extend(future.result())
                except Exception as e:
                    error_msg = f"Error copying files in batch {batch_num}"
                    logger.error(error_msg, e)
                    continue
        return data

    def prepare_import_batches(
        self,
    ) -> Iterator[Tuple[int, List[Dict[str, str]], List[Path]]]:
        """Lazily emit batches of rows for compilation into a Bulkrax csv."""

        # Using manual iterators in order to be precise about the batch size
        # Exhaust collections first, then works
        # This ensures collections will be imported first
        resource_iter = chain(self.get_resources(Collection), self.get_resources(Work))
        fileset_iter = self.get_filesets()
        batch = 0
        done = False
        collection_ids = []
        child_works = []
        child_work_filesets = []
        import_counter = Counter()
        while not done:
            rows = []
            files_to_copy = []
            (self.output_path / f"batch_{batch}").mkdir(exist_ok=True)
            while len(rows) < self.batch_size:
                resource = next(resource_iter, None)
                # If no more resources, we are done
                if not resource:
                    done = True
                    break
                # Add fields from relationships to ACL's, embargos, and parent works
                resource = self.embargos.update_resource(resource)
                resource = self.permissions.update_resource(resource)
                resource = self.parents.update_resource(resource)
                # Not terribly elegant, but we don't want to emit rows for child works before their parents, or else they might be imported in the wrong order, leading to a broken relationship
                if (
                    hasattr(resource, "parents")
                    and resource.parents
                    and any([p not in collection_ids for p in resource.parents])
                ):
                    # If it doesn't belong to a collection, then its parent would be another work
                    child_works.append(resource)
                else:
                    if resource.model == "Collection":
                        collection_ids.append(resource.id)
                    rows.append(resource.format_row(self.format_for_bulkrax))
                    for row in rows:
                        if not row["bulkrax_identifier"]:
                            print(row)
                            print(resource)
                            print(resource.format_row(self.format_for_bulkrax))
                            raise AssertionError
                import_counter[resource.model] += 1
                # Filesets should be in the same order as their parent works, ordered by work ID
                # So we take from the FileSet iterator as long as the id matches that of the current work
                # Collections don't have FileSets, when the resource is a collection, this should be None
                files, fileset_iter = before_and_after(
                    lambda x: resource.id == x.parents, fileset_iter
                )
                for file in files:
                    file = self.embargos.update_resource(file)
                    file = self.permissions.update_resource(file)
                    # Is the last child work seen the parent of this fileset? If so, emit together
                    if child_works and (child_works[-1].id in file.parents):
                        child_work_filesets.append(file)
                    else:
                        rows.append(file.format_row(self.format_for_bulkrax))
                        files_to_copy.append(file)
                    import_counter["FileSet"] += 1
            new_paths = self.copy_files_concurrently(
                self.output_path / f"batch_{batch}/files", files_to_copy, batch
            )
            yield batch, rows, new_paths
            total_msg = ", ".join(f"{k}: {v}" for k, v in import_counter.items())
            logger.info(f"Prepared batch {batch}, total works: {total_msg}")
            batch += 1
        # If done, emit any child works as a last batch, ensuring that their parents have already been imported
        if child_works:
            (self.output_path / f"batch_{batch}").mkdir(exist_ok=True)
            new_paths = self.copy_files_concurrently(
                self.output_path / f"batch_{batch}/files", child_work_filesets, batch
            )
            yield (
                batch,
                [
                    r.format_row(self.format_for_bulkrax)
                    for r in child_works + child_work_filesets
                ],
                new_paths,
            )

    def prepare_imports(self):
        # Prepare and zip import CSV and files
        for batch_id, rows, files_copied in self.prepare_import_batches():
            path_to_batch = Path(self.output_path) / f"batch_{batch_id}"
            output = StringIO()
            writer = DictWriter(
                output, fieldnames=list({k for row in rows for k in row})
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            try:
                zipfile_path = path_to_batch / f"import_{batch_id}.zip"
                with ZipFile(zipfile_path, "w") as f:
                    f.mkdir("files")
                    f.writestr(f"import{batch_id}.csv", data=output.getvalue())
                    for file in files_copied:
                        file = Path(file)
                        f.write(file, arcname=f"files/{file.name}")
                msg = f"Zip file prepared for batch {batch_id}: {str(zipfile_path)}"
                logger.info(msg)
            except Exception as e:
                error_msg = f"Error creating zipfile for batch {batch_id}"
                logger.error(error_msg, e)
                raise
