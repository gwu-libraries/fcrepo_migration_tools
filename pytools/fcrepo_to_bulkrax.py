import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictReader, DictWriter
from io import StringIO
from itertools import chain, groupby
from pathlib import Path
from shutil import copy2
from typing import Dict, Iterator, List, Optional, Tuple
from zipfile import ZipFile

from more_itertools import before_and_after
from pyoxigraph import Store

from pytools.mappings import *
from pytools.resources import *
from pytools.utils import *

logger = logging.getLogger(__name__)


class ChangeSet:
    """For updating specific fields during migration."""

    def __init__(self, change_set_path: str):
        """Expects a path to a CSV, which should contain an "id" column as well as other columns corresponding to those used in a Bulrax import. For every identifier provided, any non-null values in the row will be substituted for the values associated with that column when outputting the Bulkrax csv."""
        with open(change_set_path) as f:
            reader = DictReader(f)
            self.change_set = {r["id"]: r for r in reader}
            for _id, row in self.change_set.items():
                for k, v in row.items():
                    if not v:
                        # Remove any keys corresponding to null values
                        del self.change_set[_id][k]

    def apply_changes(self, resource: Resource | FileSet) -> Resource | FileSet:
        if changes := self.change_set.get(uri_to_id(resource.id)):
            for field, value in changes.items():
                resource.update(field, value)
        return resource


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
        change_set: Optional[str] = None,
        field_defaults: Optional[Dict[str, str]] = None,
        batch_size: int = 50,
        dry_run: bool = False,
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
        if change_set:
            self.change_set = ChangeSet(change_set)
        # if field_defaults:
        self.field_defaults = field_defaults
        self.batch_size = batch_size
        self.dry_run = dry_run
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
                admin_set_values = 'values ?adminSet {{ "{admin_set}" }}'.format(
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
            yield model.make_resource(
                id=k,
                triples=g,
                mapping=self.mapping,
                field_defaults=self.field_defaults,
            )

    def get_filesets(self) -> Iterator[FileSet]:
        """Returns all filesets with references to parent works and file URI's."""
        if self.admin_set:
            admin_set_values = """
          values ?adminSetModel {{ "AdminSet" }}
          values ?adminSet {{ "{admin_set}" }}
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
          prefix partOf: <http://purl.org/dc/terms/isPartOf>
          prefix title: <http://purl.org/dc/terms/title>

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

    def format_for_bulkrax(self, data: Dict[str, str]) -> Dict[str, str]:
        """Formats each value for Bulkrax, extracting resource identifiers from URI's, and combining duplicate fields using either a semicolon or a pipe."""
        row = data.copy()
        for key, value in data.items():
            if key in ("id", "parents"):
                value = uri_to_id(value)
                if isinstance(value, list):
                    row[key] = ";".join(value)
                elif key == "id":
                    # model_key = re.sub(
                    #    r"([a-z])([A-Z])", r"\1_\2", data["model"]
                    # ).lower()
                    row["bulkrax_identifier"] = value  # f"{model_key}s_{value}"
                else:
                    row[key] = value
            elif (key in self.pipe_delimited) and isinstance(value, list):
                row[key] = "|".join(value)
            elif isinstance(value, list):
                row[key] = "; ".join(value)
        # Exclude the original ID from the CSV for export
        del row["id"]
        return row

    def copy_files(self, path_to_destination: Path, files: List[FileSet]) -> List[str]:
        """Copy binary files associated with filesets to the specified destination. Renames file using filename metadata."""
        output = []
        pd = Path(path_to_destination)
        pd.mkdir(exist_ok=True)
        for fs in files:
            file_path = fs.get_file_path(self.path_to_root)
            if self.dry_run:
                output.append(
                    {
                        "filset_id": fs.id,
                        "path_to_binary": file_path,
                        "file_name": fs.file,
                        "found": bool(file_path),
                    }
                )
            else:
                try:
                    out = copy2(file_path, pd / fs.file)
                    output.append(out)
                except Exception as e:
                    error_msg = (
                        f"Unable to copy file {str(file_path)} to {str(pd / fs.file)}"
                    )
                    logger.error(error_msg, e)
                    continue
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
        admin_set = (self.admin_set or "").replace(" ", "_").lower()
        if self.admin_set:
            logger.info(f"Getting objects for resources in admin set {self.admin_set}")
        else:
            logger.info("Getting all objects in respository.")
        while not done:
            rows = []
            files_to_copy = []
            self.path_to_batch = Path(self.output_path) / f"batch_{admin_set}_{batch}"
            self.path_to_batch.mkdir(exist_ok=True)
            while len(rows) < self.batch_size:
                resource = next(resource_iter, None)
                # If no more resources, we are done
                if not resource:
                    done = True
                    break
                # Add fields from relationships to ACL's, embargos, and parent works
                resource = self.permissions.update_resource(resource)
                resource = self.embargos.update_resource(resource)
                resource = self.parents.update_resource(resource)
                resource = self.change_set.apply_changes(resource)
                # Not terribly elegant, but we don't want to emit rows for child works before their parents, or else they might be imported in the wrong order, leading to a broken relationship
                if any([p not in collection_ids for p in resource.parents]):
                    # If it doesn't belong to a collection, then its parent would be another work
                    child_works.append(resource)
                else:
                    if resource.model == "Collection":
                        collection_ids.append(resource.id)
                    rows.append(resource.format_row(self.format_for_bulkrax))
                import_counter[resource.model] += 1
                # Filesets should be in the same order as their parent works, ordered by work ID
                # So we take from the FileSet iterator as long as the id matches that of the current work
                # Collections don't have FileSets, when the resource is a collection, this should be None
                files, fileset_iter = before_and_after(
                    lambda x: resource.id == x.parents, fileset_iter
                )
                for file in files:
                    file = self.permissions.update_resource(file)
                    file = self.embargos.update_resource(file)
                    file = self.change_set.apply_changes(file)
                    # Is the last child work seen the parent of this fileset? If so, emit together
                    if child_works and (child_works[-1].id == file.parents):
                        child_work_filesets.append(file)
                    else:
                        rows.append(file.format_row(self.format_for_bulkrax))
                        files_to_copy.append(file)
                    import_counter["FileSet"] += 1
            new_paths = self.copy_files_concurrently(
                self.path_to_batch / "files", files_to_copy, batch
            )
            yield batch, rows, new_paths
            total_msg = ", ".join(f"{k}: {v}" for k, v in import_counter.items())
            logger.info(f"Prepared batch {batch}, total works: {total_msg}")
            batch += 1
        # If done, emit any child works as a last batch, ensuring that their parents have already been imported
        if child_works:
            self.path_to_batch = self.output_path / f"batch_{admin_set}_{batch}"
            self.path_to_batch.mkdir(exist_ok=True)
            new_paths = self.copy_files_concurrently(
                self.path_to_batch / "files", child_work_filesets, batch
            )
            yield (
                batch,
                [
                    r.format_row(self.format_for_bulkrax)
                    for r in child_works + child_work_filesets
                ],
                new_paths,
            )

    def make_csv(self, rows):
        output = StringIO()
        writer = DictWriter(output, fieldnames=list({k for row in rows for k in row}))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return output.getvalue()

    def save_zip(self, csv_data, batch_id, files):
        try:
            zipfile_path = self.output_path / f"{self.path_to_batch.name}.zip"
            with ZipFile(zipfile_path, "w") as f:
                f.mkdir("files")
                f.writestr(f"{self.path_to_batch.name}.csv", data=csv_data)
                for file in files:
                    file = Path(file)
                    f.write(file, arcname=f"files/{file.name}")
            msg = f"Zip file prepared for batch {batch_id}: {str(zipfile_path)}"
            logger.info(msg)
        except Exception as e:
            error_msg = f"Error creating zipfile for batch {batch_id}"
            logger.error(error_msg, e)
            raise
        self.cleanup_files(files)

    def cleanup_files(self, files):
        for file in files:
            if Path(file).exists():
                Path(file).unlink()
        if (self.path_to_batch / "files").exists():
            (self.path_to_batch / "files").rmdir()
        self.path_to_batch.rmdir()

    def prepare_imports(self):
        # Prepare and zip import CSV and files
        for batch_id, rows, files_copied in self.prepare_import_batches():
            if self.dry_run:
                for i, row in enumerate(rows):
                    logger.debug({"batch": batch_id, "row": i, "data": row})
                for f in files_copied:
                    logger.debug({"batch": batch_id, "file": f})
            else:
                self.save_zip(
                    csv_data=self.make_csv(rows),
                    batch_id=batch_id,
                    files=files_copied,
                )
