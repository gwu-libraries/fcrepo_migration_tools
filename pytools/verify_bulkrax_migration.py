import asyncio
import re
import subprocess
from collections import Counter, defaultdict
from csv import DictReader
from hashlib import md5, sha256
from io import TextIOWrapper
from itertools import chain, groupby
from pathlib import Path
from shutil import rmtree
from typing import Any, Dict, Iterable, Iterator, List, Sequence
from urllib.request import urlretrieve
from zipfile import ZipFile

import polars as pl
import yaml
from aiobotocore.session import get_session
from aiohttp.web import static
from botocore.credentials import Credentials
from pyoxigraph import Literal, NamedNode, QuerySolutions, Store

from pytools.fcrepo_to_bulkrax import FedoraGraph, Work
from pytools.graph_part import GraphPart


def to_camel_case(snake_str):
    return "".join(x.capitalize() for x in snake_str.lower().split("_"))


def to_ocfl(key: str) -> str:
    """Per Fedora docs, creates OCFL identifer from the Fedora resource ID
    :param key: the URI part after the base or root URI, e.g., localhost:8080/fcrepo/rest/"""
    digest = sha256(f"info:fedora/{key}".encode("utf-8")).hexdigest()
    return f"{digest[0:3]}/{digest[3:6]}/{digest[6:9]}/{digest}"


def etag_checksum(filename, chunk_size=8 * 1024 * 1024):
    """Compute MD5 checksum for a file based on chunks (as calculated by AWS S3 on upload)"""
    md5s = []
    with open(filename, "rb") as f:
        for data in iter(lambda: f.read(chunk_size), b""):
            md5s.append(md5(data).digest())
    m = md5(b"".join(md5s))
    return "{}-{}".format(m.hexdigest(), len(md5s))


class Fedora6Repo:
    """Object to represent a Hyrax 5/Fedora 6 repository"""

    def __init__(self, path_to_metadata: str):
        """:param path_to_metadata: directory containing YAML metadata definitions for a Hyrax 5 repository."""
        self.path_to_metadata = Path(path_to_metadata)
        self.metadata = self.load_metadata_maps()

    @staticmethod
    def retrieve_metadata_maps(metadata_list: str):
        """Retrieves YAML metadata definition files from their URL's. The argument should point to a local YAML file mapping metadata types to the URL's where those definitions can be retrieved. YAML files are saved locally, in the same directory as the provided file."""
        path_to_yaml = Path(metadata_list)
        with open(path_to_yaml) as f:
            metadata_map = yaml.load(f, Loader=yaml.CLoader)
        root_dir = path_to_yaml.parents[0]
        for label, uris in metadata_map.items():
            dest_path = root_dir / label
            dest_path.mkdir(exist_ok=True)
            for uri in uris.values():
                urlretrieve(uri, dest_path / f"{uri.split('/')[-1]}")

    def load_metadata_maps(self, custom_models=True):
        """Populates metadata definitions (mapping RDF predicates to Hyrax object attributes) from those provided by the Hyrax repo and (optionally) custom repository code. Expects Hyrax standard definitions and custom definitions to reside in separate directories within the directory given as self.path_to_metadata."""
        metadata = defaultdict(dict)
        for yaml_file in self.path_to_metadata.rglob("*.yaml"):
            with open(yaml_file) as f:
                md = yaml.load(f, Loader=yaml.CLoader)
                key = yaml_file.parts[-2]  # Directory immediately above YAML file
                metadata[key][yaml_file.stem] = {}
                for k, v in md["attributes"].items():
                    req = v.get("form", {}).get("required")  # Is this field required?
                    metadata[key][yaml_file.stem][k] = {
                        "predicate": v["predicate"],  # RDF predicate
                        "optional": True if (v.get("form") and not req) else False,
                    }
        # If custom_models=True, expect two top-level keys, one corresponding to Hyrax core metadata, the other to a custom set of definitions for models used locally.
        # Update the metadata for the custom_models to reflect the full suite of Hyrax core metadata
        if custom_models:
            keys = [k for k in metadata.keys() if k != "hyrax"]
            for key in keys:
                for model in metadata[key]:
                    # Handle FileSet metadata separately
                    if model == "file_set_metadata":
                        continue
                    for k in [
                        "core_metadata",
                        "basic_metadata",
                    ]:  # Test for presence of predicates from core and basic
                        for k1, v1 in metadata["hyrax"][k].items():
                            if k1 not in metadata[key][model]:
                                metadata[key][model][k1] = (
                                    v1  # Update with missing predicates
                                )
                # Update custom FileSet definitions with Hyrax definitions
                for k, v in metadata["hyrax"]["file_set_metadata"].items():
                    if k not in metadata[key]["file_set_metadata"]:
                        metadata[key]["file_set_metadata"][k] = v
        # internal predicates
        work_internals_keys = ["admin_set_id", "id", ("internal_resource", "model")]
        metadata["work_internals"] = defaultdict(dict)
        for k in work_internals_keys:
            if isinstance(k, str):
                metadata["work_internals"][k] = metadata["hyrax"][
                    "hyrax_internal_metadata"
                ][k]
            else:
                metadata["work_internals"][k[1]] = metadata["hyrax"][
                    "hyrax_internal_metadata"
                ][k[0]]
        fileset_internals_keys = ["file_ids"]
        metadata["fileset_internals"] = defaultdict(dict)
        for k in fileset_internals_keys:
            metadata["fileset_internals"][k] = metadata["hyrax"][
                "hyrax_internal_metadata"
            ][k]
        return metadata

    @staticmethod
    def construct_object_query(models, model_predicate: str):
        """Construct SPARQL query to find resources matching a particular model."""
        models_str = " ".join([f'"{model}"' for model in models])
        return """
            select distinct ?id ?predicate ?object
            where {{
                values ?model {{ {models} }}
                ?id ?predicate ?object.
                ?id <{model_predicate}> ?model
            }}
            order by ?id
            """.format(models=models_str, model_predicate=model_predicate)

    @property
    def models(self):
        """Custom models should be defined as top-level keys under the key corresponding to local/custom (not Hyrax core or basic) metadata."""
        return [
            to_camel_case(model_key)
            for key, value in self.metadata.items()
            for model_key in value
            if (key not in ["hyrax", "work_internals", "fileset_internals"])
            and (model_key != "file_set_metadata")
        ]

    @property
    def model_predicate(self):
        """RDF predicate identifying a resource model"""
        return self.metadata["work_internals"]["model"]["predicate"]

    @property
    def works_query(self):
        return Fedora6Repo.construct_object_query(self.models, self.model_predicate)

    @property
    def file_sets_query(self):
        return Fedora6Repo.construct_object_query(
            ["Hyrax::FileSet"], self.model_predicate
        )

    @property
    def admin_sets_query(self):
        return Fedora6Repo.construct_object_query(
            ["Hyrax::AdministrativeSet"], self.model_predicate
        )

    @property
    def links_query(self):
        """SPARQL query that returns links from resources on either side of a proxyFor relation (i.e., works and filesets)"""
        return """
                prefix last: <http://www.iana.org/assignments/relation/last>
                prefix next: <http://www.iana.org/assignments/relation/next>
                prefix first: <http://www.iana.org/assignments/relation/first>
                prefix proxyFor: <http://www.openarchives.org/ore/terms/proxyFor>
                select * where {
                    {
                        select distinct ?work ?target
                        where {
                            ?work last: ?source.
                            ?source proxyFor: ?target
                        }
                    }
                    union
                    {
                        select distinct ?work ?target
                        where {
                            ?work first: ?source.
                            ?source proxyFor: ?target
                        }
                    }
                    union
                    {
                        select distinct ?work  ?target
                        where {
                            ?work next: ?source.
                            ?source proxyFor: ?target
                        }
                    }
                }
            """

    @property
    def embargo_query(self):
        """SPARQL query for return embargo values on resources"""
        return """
        prefix embargo: <http://example.com/predicate/embargo_id>
        prefix visibility_after_embargo: <http://example.com/predicate/visibility_after_embargo>
        prefix embargo_release_date: <http://example.com/predicate/embargo_release_date>
        prefix visibility_during_embargo: <http://example.com/predicate/visibility_during_embargo>

        select distinct ?resource ?embargo_release_date ?visibility_during_embargo ?visibility_after_embargo
        where {
            ?resource embargo: ?embargo.
            ?embargo visibility_after_embargo: ?visibility_after_embargo.
            ?embargo visibility_during_embargo: ?visibility_during_embargo.
            ?embargo embargo_release_date: ?embargo_release_date
        }
        """

    @property
    def acl_query(self):
        return """
        prefix acl: <http://www.w3.org/ns/auth/acl#>
        prefix model: <info:fedora/fedora-system:def/model#>
        prefix ns: <http://vocabulary.samvera.org/ns#>

        select ?resource ?mode ?agent
        where {
            ?acl ns:permissions ?p.
            ?acl acl:accessTo ?resource.
            ?p acl:mode ?mode.
            ?p acl:agent ?agent.
            ?p model:hasModel "Hyrax::Permission"
        }
        """

    def process_query(
        self, rows_iter: QuerySolutions, internals_key: str
    ) -> Iterator[Dict[str, List]]:
        """rows_iter should be an iterable of triples. This method maps each RDF predicate and object to a key-value pair based on the metadata mapping, returning one mapping per row."""
        # Create 1-d map from RDF predicates to Hyrax object attributes
        attrs = {
            k: v
            for k, v in self.metadata.items()
            if (k != "hyrax") and not (k.endswith("internals"))
        }
        predicate_map = {
            v2["predicate"]: k2 for v in attrs.values() for k2, v2 in v.items()
        }
        # Add "internal" metadata attributes
        for k, v in self.metadata[internals_key].items():
            predicate_map.update({v["predicate"]: k})
        # Group by resource ID
        for k, g in groupby(rows_iter, key=lambda row: row["id"].value):
            row = defaultdict(list)
            for triple in g:
                if triple["predicate"].value in predicate_map:
                    row[predicate_map[triple["predicate"].value]].append(
                        triple["object"].value
                    )
            if not row.get("id"):
                # Insert logging here to flag these
                row["id"] = [k]
            yield row

    def link_works_to_admin_sets(self, works, admin_sets: Iterable[Dict[str, List]]):
        # work -> bulkrax_identifier: admin_set.title
        admin_set_lookup = {row["id"][0]: row for row in admin_sets}
        return {
            row["bulkrax_identifier"][0]: admin_set_lookup.get(
                row["admin_set_id"][0], {}
            ).get("title")
            for row in works
        }

    def link_works_to_filesets(
        self, works, filesets: Iterable[Dict[str, List]], links: QuerySolutions
    ):
        """Indicates each fileset's parent work by Bulkrax identifier added to the fileset metadata."""
        work_lookup = {work["id"][0]: work for work in works}
        fileset_lookup = {fileset["id"][0]: fileset for fileset in filesets}
        for link in links:
            work = work_lookup.get(
                link["work"].value.split("#")[0]
            )  # proxyFor relations are given as hashed URIs, so we need to remove the hashed portion to access the URI of the work itself
            if work:
                fileset = fileset_lookup.get(link["target"].value)
                if fileset:
                    fileset["parents"] = work["bulkrax_identifier"]
        return list(work_lookup.values()), list(fileset_lookup.values())

    def add_embargoes(
        self, resources: List[Dict[str, List[str]]], embargoes: QuerySolutions
    ):
        """Adds embargo metadata to resources"""
        lookup = {resource["id"][0]: resource for resource in resources}
        for triple in embargoes:
            resource = lookup.get(triple["resource"].value)
            if not resource:
                continue
            for k in [
                "visibility_during_embargo",
                "visibility_after_embargo",
                "embargo_release_date",
            ]:
                resource.update({k: triple[k].value})
        return list(lookup.values())

    def add_acls(
        self, resources: List[Dict[str, List[str]]], permissions: QuerySolutions
    ):
        """Adds visibility to resources, based on the most permissive permission."""
        lookup = {resource["id"][0]: resource for resource in resources}
        for triple in permissions:
            resource = lookup.get(triple["resource"].value)
            if not resource:
                continue
            if not "visibility" in resource:
                resource["visibility"] = "restricted"
            if (
                triple["agent"].value == "group/public"
                and triple["mode"].value == "read"
            ):
                resource["visibility"] = "open"
            elif (
                triple["agent"].value == "group/registered"
                and triple["mode"].value == "read"
            ):
                resource["visibility"] = "authenticated"
        return list(lookup.values())

    def retrieve_derivatives(self, file_sets: List[Dict[str, List[str]]], graph: Store):
        """Retrieves metadata for each file resource associated with a FileSet resource"""
        for file_set in file_sets:
            files_metadata = {}
            for file_id in file_set["file_ids"]:
                result = {
                    quad.predicate.value: quad.object.value
                    for quad in graph.quads_for_pattern(NamedNode(file_id), None, None)
                }
                if (
                    result["http://vocabulary.samvera.org/ns#pcdmUse"]
                    == "http://pcdm.org/use#OriginalFile"
                ):
                    files_metadata["mime_type"] = result[
                        "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType"
                    ]
                    files_metadata["original"] = result[
                        "http://vocabulary.samvera.org/ns#fileIdentifier"
                    ]
                    files_metadata["binary_ocfl"] = to_ocfl(
                        files_metadata["original"].replace(
                            "fedora://fedora:8080/fcrepo/rest/", ""
                        )
                    )
                else:
                    files_metadata["derivatives"] = files_metadata.get(
                        "derivatives", []
                    ) + [result["http://vocabulary.samvera.org/ns#pcdmUse"]]
            file_set.update(files_metadata)
        return file_sets

    def populate_graph(self, graph: Store):
        """Given an instance of a pyoxigraph.Store (RDF graph) generated from Hyrax 5/Fedora 6 triples, creates a representation of the repository: works, filesets, collections, admin sets, their relationships, as well as embargoes and ACLs.
        TO DO: add collections, admin sets"""
        works = self.process_query(graph.query(self.works_query), "work_internals")
        file_sets = self.process_query(
            graph.query(self.file_sets_query), "fileset_internals"
        )
        links = graph.query(self.links_query)
        self.works, self.file_sets = self.link_works_to_filesets(
            works, file_sets, links
        )
        embargoes = list(graph.query(self.embargo_query))
        self.works = self.add_embargoes(self.works, embargoes)
        self.file_sets = self.add_embargoes(self.file_sets, embargoes)
        permissions = list(graph.query(self.acl_query))
        self.works = self.add_acls(self.works, permissions)
        self.file_sets = self.add_acls(self.file_sets, permissions)
        self.file_sets = self.retrieve_derivatives(self.file_sets, graph)


class DiffLog:
    skipped_fields = [
        "id",
        "admin_set_id",
        "date_uploaded",
        "date_modified",
        "depositor",
        "bulkrax_identifier",
    ]

    def __init__(self):
        self.log = []

    def log_errors(self, error: str, **options):
        """options should include paraams key and model"""
        entry = {"error": error}
        entry.update(options)
        self.log.append(entry)

    def summarize_log(self):
        summary = Counter()
        for entry in self.log:
            summary[(entry["error"], entry["key"], entry.get("model"))] += 1
        return summary


class MigrationDiff:
    """Computes diff between a Fedora 4/Hyrax 3.x repo and Fedora 6/Hyrax 5.x repo."""

    def __init__(
        self,
        f4_repo: FedoraGraph,
        f6_repo: Fedora6Repo,
        f6_graph: Store,
        out_path: Path | str,
    ):
        self.f4_repo = f4_repo
        self.f6_repo = f6_repo
        self.out_path = Path(out_path)

        self.f6_repo.populate_graph(f6_graph)

        self.f4_works = [
            self.f4_repo.embargos.update_resource(
                self.f4_repo.permissions.update_resource(resource)
            )
            for resource in self.f4_repo.get_resources(Work)
        ]

        self.f4_file_sets = [
            self.f4_repo.embargos.update_resource(
                self.f4_repo.permissions.update_resource(resource)
            )
            for resource in self.f4_repo.get_filesets()
        ]
        # Bulkrax ID serves as the map between Fedora 4 works and their migrated versions in Fedora 6
        self.ids_to_bulkrax = {
            work["bulkrax_identifier"][0]: work
            for work in self.f6_repo.works
            if work["bulkrax_identifier"]
        }
        self.diff_log = DiffLog()
        self.file_set_lookup = self._file_set_lookup()

    def _file_set_lookup(self) -> Dict[tuple[str, str], Dict[str, str]]:
        """Map file sets to their title and parent work's Bulkrax ID. (We aren't persisting the file's original identifier as its Bulkrax identifier, so matching requires this more roundabout approach."""
        file_set_lookup = {}
        for f in self.f6_repo.file_sets:
            title = f["title"][0]
            try:
                parent = f["parents"][0]
            except IndexError:
                continue
            file_set_lookup[(title, parent)] = f
        return file_set_lookup

    def match_visibility(self, key, original, migrated):
        """Handles embargoes, where the the to-be-migrated work/file set will have "embargo" for the value of the "visibility" field"""
        match key:
            case "visibility":
                if original["visibility"] == "embargo":
                    assert (
                        original["visibility_during_embargo"] == migrated["visibility"]
                    ), "Metadata Mismatch"
                else:
                    assert original["visibility"] == migrated["visibility"], (
                        "Metadata Mismatch"
                    )
                return True
            case "visibility_after_embargo" | "embargo_release_date":
                original_value = original[key]
                if key == "embargo_release_date":
                    assert migrated[key][:10] == original_value, "Metadata Mismatch"
                else:
                    assert migrated[key] == original_value, "Metadata Mismatch"
                return True

    def diff_works(self):
        """Compute difference between original and migreated works"""
        for f4_work in self.f4_works:
            model = f4_work.data["model"]
            bulkrax_id = f4_work.id.split("/")[-1]
            work = self.ids_to_bulkrax.get(bulkrax_id)
            try:
                assert work, "Matching Work Not Found"
            except AssertionError as e:
                self.diff_log.log_errors(str(e), original_id=f4_work.id, model=model)
                continue
            for field, elements in f4_work.data.items():
                try:
                    if field in DiffLog.skipped_fields:
                        continue
                    assert field in work, "Field Not Found"
                    if self.match_visibility(field, f4_work.data, work):
                        continue
                    if isinstance(elements, list):
                        if isinstance(work[field], list):
                            for item in elements:
                                if item:
                                    assert item.strip() in [
                                        elem.strip() for elem in work[field]
                                    ], "Metadata Mismatch"
                        else:
                            assert "".join(elements) == "".join(list(work[field])), (
                                "Metadata Mismatch"
                            )
                    else:
                        assert elements == "".join(list(work[field])), (
                            "Metadata Mismatch"
                        )
                except AssertionError as e:
                    work_id = work["id"][0]
                    self.diff_log.log_errors(
                        str(e),
                        original_id=f4_work.id,
                        migrated_id=work_id,
                        key=field,
                        original_value=elements,
                        migrated_value=work[field],
                        model=model,
                    )

    def diff_file_sets(self):
        """Computes differences between original and migrated file sets"""
        for f4_fs in self.f4_file_sets:
            parent_id = f4_fs.parents.split("/")[-1]
            title = f4_fs.title
            original_id = (
                f4_fs.id,
                parent_id,
                title,
            )  # include parent ID and title so we can match on Bulkrax error logs
            try:
                assert (title, parent_id) in self.file_set_lookup, (
                    "Matching FileSet Not Found"
                )
            except AssertionError as e:
                self.diff_log.log_errors(
                    str(e), original_id=original_id, model="FileSet"
                )
                continue
            fs = self.file_set_lookup[(title, parent_id)]
            for field, element in f4_fs.__dict__.items():
                try:
                    # Only checking for visibility and embargo-related fields here, but the method will return True for any other field
                    self.match_visibility(field, f4_fs.__dict__, fs)
                except AssertionError as e:
                    self.diff_log.log_errors(
                        str(e),
                        original_id=original_id,
                        migrated_id=fs["id"][0],
                        key=field,
                        migrated_value=fs[field],
                        original_value=element,
                        model="FileSet",
                    )

    def check_derivatives(self):
        """Runs a check of migrated file sets' derivatives"""
        assertions_1 = {
            "Missing Original Binary": lambda fs: "original" in fs,
            "Missing Mime Type": lambda fs: "mime_type" in fs,
            "No Derivatives": lambda fs: "derivatives" in fs,
        }
        assertions_2 = {
            "PDF Missing Extracted Text": lambda fs, derivs: (
                not (fs.get("mime_type") == "application/pdf")
                or ("http://pcdm.org/use#ExtractedText" in derivs)
            ),
            "Unexpected Number of Derivatives Found": lambda fs, derivs: (
                not (fs.get("mime_type") == "application/pdf") or (len(derivs) != 2)
            ),
            "Missing Thumbnail": lambda _, derivs: (
                "http://pcdm.org/use#ThumbnailImage" in derivs
            ),
        }
        for file_set in self.f6_repo.file_sets:
            try:
                for assertion, test in assertions_1.items():
                    try:
                        assert test(file_set), assertion
                    except AssertionError as e:
                        self.diff_log.log_errors(
                            str(e),
                            model="FileSet",
                            migrated_id=file_set["id"][0],
                            mime_type=file_set.get("mime_type"),
                            num_derivs=0,
                        )
                        raise  # If any of these tests fails, skip the rest
                for assertion, test in assertions_2.items():
                    try:
                        assert test(file_set, file_set["derivatives"]), assertion
                    except AssertionError as e:
                        self.diff_log.log_errors(
                            str(e),
                            model="FileSet",
                            migrated_id=file_set["id"][0],
                            mime_type=file_set.get("mime_type"),
                            num_derivs=Counter(file_set["derivatives"]),
                        )
            except AssertionError:
                continue

    def compare_checksums(self):
        pass

    def run_checksums(
        self,
        path_to_zips: Path | str,
        s3_checksums: Dict[str, Dict[str, str]],
        zip_file_pattern: str = r"batch.*_\d+\.zip",
    ) -> Iterator[tuple[str, List[Dict[str, str]]]]:
        """Given a path to a folder containing zipped import files, a mapping of filenames and parent works to file set metadata, and (optionally) a filename pattern, this method iterates through the zip files, updating the file set metadata with the relevant checksum, matching the method to that computed by S3."""
        path_to_zips = Path(path_to_zips)
        tmp_path = path_to_zips.parents[0] / "tmp"
        tmp_path.mkdir(exist_ok=True)
        zip_file_pattern = re.compile(zip_file_pattern)
        for zip_file in path_to_zips.glob("*.zip"):
            if not zip_file_pattern.match(zip_file.name):
                continue
            with ZipFile(zip_file) as zf:
                with zf.open(f"{zip_file.stem}.csv") as f:
                    reader = DictReader(TextIOWrapper(f))
                    rows = [r for r in reader]
                    files_to_check = [
                        {"file": r["file"], "parent": r["parents"]}
                        for r in rows
                        if r["model"] == "FileSet"
                    ]
                    checksums = []
                    for file in files_to_check:
                        fs = self.file_set_lookup.get((file["file"], file["parent"]))
                        if not fs:
                            continue
                        if not fs["binary_ocfl"]:
                            continue
                        path_to_file = zf.extract(f"files/{file['file']}", tmp_path)
                        h5_checksum = s3_checksums.get(fs["binary_ocfl"])
                        if h5_checksum:
                            if "ChecksumCRC32" in h5_checksum:
                                checksum = etag_checksum(path_to_file)
                                value = None
                            else:
                                value = h5_checksum["decoded_value"]
                                method = "CRC-64/NVME"
                                args = [
                                    "/Users/dsmith/Documents/code/rust/crc-fast-rust/target/release/checksum",
                                    "-a",
                                    method,
                                    "-f",
                                    path_to_file,
                                ]
                                checksum = subprocess.run(
                                    args, capture_output=True
                                ).stdout
                            file["h5_checksum"] = value
                            file["original_checksum"] = checksum
                            checksums.append(file)
                        Path(path_to_file).unlink()
            yield (str(zip_file), checksums)
        rmtree(tmp_path)


class S3OcflRepo:
    """Utilities for retrieving content from an S3 bucket acting as Fedora's OCFL store."""

    def __init__(
        self,
        credentials: Credentials,
        region,
        bucket,
        path_to_ocfl: str,
        graph: str | None = None,
    ):
        """:param credentials: AWS credentials with bucket access"""
        self.credentials = credentials
        self.bucket = bucket
        self.path_to_ocfl = path_to_ocfl
        self.region = region
        self.loop = asyncio.get_event_loop()  # For running async tasks
        if graph:
            self.graph = Store(graph)
        else:
            self.graph = None

    def prepare_repo(self, inventory_key, download_path: str):
        self.download_path = Path(download_path)
        inventory_path = self.loop.run_until_complete(
            self.download_inventory(inventory_key, download_path)
        )
        self.filter_inventory(inventory_path)
        downloaded_objects = self.loop.run_until_complete(
            self.download_nt_objects(
                self.rdf_df.rows(named=True), self.download_path / "rdf"
            )
        )
        self.graph = self.checksums = self.loop.run_until_complete(
            self.get_object_checksums(self.originals.rows(named=True))
        )
        errors = [obj for obj in downloaded_objects if isinstance(obj, Exception)] + [
            obj for obj in self.checksums if isinstance(obj, Exception)
        ]
        # TO DO: log errors
        if not self.graph:
            g = GraphPart(
                dirs=[self.download_path / "rdf"],
                store=str(self.download_path / "hyrax-5-migrated"),
            )
            g.walk()
        return self

    async def download_inventory(self, inventory_key, download_path: str) -> Path:
        """Downloads file from S3 at the provided key."""
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=self.region,
            aws_secret_access_key=self.credentials.secret_key,
            aws_access_key_id=self.credentials.access_key,
        ) as client:
            target = Path(download_path) / Path(inventory_key).name
            await self.fetch_object(client, inventory_key, target)
        return target

    async def fetch_checksum(self, client, key):
        response = await client.get_object_attributes(
            Bucket=self.bucket, Key=key, ObjectAttributes=["Checksum"]
        )
        return {"key": key, "checksum": response["Checksum"]}

    async def fetch_object(self, client, key, filepath):
        """:param client: from abiobocore.get_session.session.create_client
        :param key: key to S3 object
        :param filepath: path to file for saving locally
        Returns True unless an exception is raised
        """
        response = await client.get_object(Bucket=self.bucket, Key=key)
        async with response["Body"] as stream:
            with open(filepath, "wb") as f:
                f.write(await stream.read())
        return True

    async def download_nt_objects(
        self, resources: list[dict[str, Any]], download_path: Path
    ):
        """:param resources: should contain a key column as well as a key_base column, which will be used to name the .nt file locally (and contains only the resource's full OCFL identifier"""
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=self.region,
            aws_secret_access_key=self.credentials.secret_key,
            aws_access_key_id=self.credentials.access_key,
        ) as client:
            tasks = asyncio.gather(
                *[
                    self.fetch_object(
                        client,
                        key=resource["key"],
                        filepath=download_path / f"{resource['key_base']}.nt",
                    )
                    for resource in resources
                ],
                return_exceptions=True,
            )
            return await tasks

    async def get_object_checksums(
        self, resources: list[dict[str, Any]]
    ) -> list[dict[str, Any] | BaseException]:
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=self.region,
            aws_secret_access_key=self.credentials.secret_key,
            aws_access_key_id=self.credentials.access_key,
        ) as client:
            tasks = asyncio.gather(
                *[
                    self.fetch_checksum(client, key=resource["key"])
                    for resource in resources
                ],
                return_exceptions=True,
            )
            return await tasks

    def filter_inventory(self, path_to_inventory: Path):
        """Loads and filters a parquet inventory from S3, producing two version: one retaining only the latest versions of all .nt files, the other retaining only files whose keys end with the string "original" (referring to original binaries)."""
        df = pl.read_parquet(path_to_inventory)
        # Filter the pola.rs DataFrame, retaining only .nt files, and unpacking the version label into its own column
        # The key_base column contains the unique OCLF ID for that resource (irrespective of version)
        # Plan is as follows:
        # filter on keys ending in .nt
        # extract the version and OCFL ID from the key string
        # unpack extracted data (from struct)
        # create column to store version, case to int (for correct sorting)
        # group by OCFL ID
        # sort by version within each group
        # take the first element of each sorted group -> latest version by ID
        self.rdf_df = (
            df.filter(pl.col("key").str.ends_with(".nt"))
            .with_columns(
                pl.col("key"),
                pl.col("key")
                .str.extract_groups(
                    ".+/(?<key_base>[a-z0-9]+)/v(?<version>[0-9]+)/content/.+"
                )
                .alias("key_struct"),
            )
            .unnest("key_struct")
            .with_columns(pl.col("version").cast(pl.Int32))
            .group_by("key_base")
            .agg(pl.all().sort_by("version", descending=True).first())
        )
        # Filter on binary originals
        self.originals = df.filter(pl.col("key").str.ends_with("original"))
        # Check that we're not dealing with multiple versions
        # If so, this logic needs to be more complicated
        assert self.originals.filter(pl.col("key").str.contains("v2")).is_empty()
