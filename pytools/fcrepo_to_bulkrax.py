import logging
from collections import Counter
from csv import DictReader, DictWriter
from datetime import datetime
from io import StringIO
from itertools import chain, groupby
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple
from zipfile import ZipFile

import jsonlines
from more_itertools import before_and_after
from pyoxigraph import Store

from pytools.mappings import *
from pytools.queue import ChildQueue, StaggeredQueue
from pytools.resources import *
from pytools.utils import *

logger = logging.getLogger(__name__)


class ChangeSet:
    """For updating specific fields during migration."""

    def __init__(self, change_set_path: str):
        """Expects a path to a CSV, which should contain an "id" column as well as other columns corresponding to those used in a Bulrax import. For every identifier provided, the following substitutions are allowed:
        - a non-null value: the value will be substituted for the value associated with that column when outputting the Bulkrax csv
        - the name of another Bulkrax column, surrounded by underscores (e.g., _creator_): the value from this column will be substituted for the value of the current column
        - a delete flag, __DELETE__ (note the double underscores): the value in the current column will be replaced by a null value."""
        with open(change_set_path) as f:
            reader = DictReader(f)
            self.change_set = {r["id"]: r for r in reader}

    def apply_changes(self, resource: Resource) -> Resource:
        if changes := self.change_set.get(uri_to_id(resource.id)):
            deletes = []
            for field, value in changes.items():
                if not value:
                    continue
                if not (value.startswith("_") and value.endswith("_")):
                    resource.update(field, value)
                elif (
                    value == "__DELETE__"
                ):  # Stage deletes for after all substitutions have been made
                    deletes.append(field)
                else:
                    sub_field = value[1:-1]  # removing underscores
                    sub_value = resource.data.get(sub_field)
                    if sub_value:
                        resource.update(
                            field, sub_value
                        )  # fail safe: don't overwrite with null data
            for field in deletes:
                resource.update(field, None)

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
        admin_set: str = "",
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
        self.admin_set = admin_set
        output_path = Path(output_path)
        if admin_set:
            output_path = output_path / admin_set.replace(" ", "_").lower()
        output_path.mkdir(exist_ok=True)
        self.batch_handler = BatchHandler(
            batch_size,
            self.format_for_bulkrax,
            output_path,
            path_to_root,
            dry_run,
        )
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
        self.pipe_delimited = pipe_delimited if pipe_delimited else []
        if change_set:
            self.change_set = ChangeSet(change_set)
        # if field_defaults:
        self.field_defaults = field_defaults
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
          ?a title: ?adminSet.
          """
        else:
            admin_set_values, admin_set_criteria = "", ""
        models_str = " ".join([f'"{model}"' for model in self.models])
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
                values ?model {{ {models} }}
                ?s fedora:hasModel ?model.
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
            admin_set_values=admin_set_values,
            admin_set_criteria=admin_set_criteria,
            models=models_str,
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

    def process_filesets(self, resource, has_parent):
        filesets, self.fileset_iter = before_and_after(
            lambda x: resource.id == x.parents, self.fileset_iter
        )
        for fileset in filesets:
            fileset = self.apply_attributes(fileset)
            if has_parent:
                self.works_with_parents_filesets[resource.id].append(fileset)
            else:
                self.batch_handler.add_resource(fileset, True)
            self.import_counter["FileSet"] += 1

    def process_works_with_parents(self):
        for resource in self.works_with_parents.get_children(
            parents=self.batch_handler.processed
        ):
            self.batch_handler.add_resource(resource)
            for fileset in self.works_with_parents_filesets[resource.id]:
                self.batch_handler.add_resource(fileset, True)

    def apply_attributes(self, resource: Resource | FileSet) -> Resource | FileSet:
        """Applies permissions, embargos, parent-child relationships, and changes from the change set"""
        resource = self.permissions.update_resource(resource)
        resource = self.embargos.update_resource(resource)
        resource = self.parents.update_resource(resource)
        resource = self.change_set.apply_changes(resource)
        return resource

    def prepare_import_batches(
        self,
    ) -> Iterator[BatchResult]:
        """Lazily emits batches of rows for compilation into a Bulkrax csv."""

        # Using manual iterators in order to be precise about the batch size
        # Exhaust collections first, then works
        # This ensures collections will be imported first
        self.resource_iter = chain(
            self.get_resources(Collection), self.get_resources(Work)
        )
        self.fileset_iter = self.get_filesets()
        self.works_with_parents = ChildQueue(lambda x: x.parents)
        self.works_with_parents_filesets = defaultdict(list)
        self.import_counter = Counter()
        admin_set = (self.admin_set or "").replace(" ", "_").lower()
        if self.admin_set:
            logger.info(f"Getting objects for resources in admin set {self.admin_set}")
        else:
            logger.info("Getting all objects in respository.")
        for resource in self.resource_iter:
            # Add fields from relationships to ACL's, embargos, and parent works
            resource = self.apply_attributes(resource)
            # Don't want to emit rows for child works before their parents, or else they might be imported in the wrong order, leading to a broken relationship
            has_parent = self.works_with_parents.stored(resource)
            if not has_parent:
                self.batch_handler.add_resource(resource)
            # Filesets should be in the same order as their parent works, ordered by work ID
            # So we take from the FileSet iterator as long as the id matches that of the current work
            # Collections don't have FileSets, when the resource is a collection, this should be []
            self.process_filesets(resource, has_parent)
            self.import_counter[resource.model] += 1

            batch_result = self.batch_handler.current_batch()
            if batch_result:
                yield batch_result
        #  Emit any child works  at the end, ensuring that their parents have already been imported
        # Loop until we've processed all the child works
        while self.works_with_parents.not_empty:
            self.process_works_with_parents()
        batch_result = self.batch_handler.current_batch(done=True)
        # Emitting any remaining filesets
        while batch_result:
            yield batch_result
            batch_result = self.batch_handler.current_batch()

    def log_output(self, writer, batch):
        for row in batch.rows:
            writer.write({"batch": f"{batch.batch_id}", "row": row})
        for f in batch.files_copied:
            writer.write({"batch": f"{batch.batch_id}", "file": f})

    def prepare_imports(self):
        with open(
            Path(self.batch_handler.output_path)
            / f"migration_{datetime.now().strftime('%Y-%m-%d')}.jsonl",
            "w",
        ) as f:
            with jsonlines.Writer(f) as writer:
                # Prepare and zip import CSV and files
                for batch in self.prepare_import_batches():
                    self.log_output(writer, batch)
                    if not self.batch_handler.dry_run:
                        batch.save_zip()
