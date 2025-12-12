import csv
import json
import re
from collections import defaultdict
from csv import DictReader
from datetime import datetime
from itertools import groupby
from pathlib import Path
from typing import Dict, Iterator, List, Self, Tuple

import requests
from pyoxigraph import Store


def uri_to_id(uri: str) -> str:
    return uri.split("/")[-1]


def is_active_embargo(record):
    return (
        datetime.fromisoformat(record["release_date"]).replace(tzinfo=None)
        >= datetime.now()
    )


class FedoraGraph:
    MEMBERSHIP_PREDICATE = "http://pcdm.org/models#memberOf"

    def __init__(
        self,
        path_to_graph,
        path_to_mapping: str,
        models,
        pipe_delimited: List[str] | str,
    ):
        """Provide a path to an Oxigraph RDF store, a path to a mapping of RDF predicates to Bulkrax fields, and a list satisfying the predicate info:fedora/fedora-system:def/model#hasModel for the types of works to be extracted.
        The mapping should be a CSV with headers "predicate" and "bulkrax_field".
        The list of models may either be a list of strings or a comma-separated string."""
        self.store = Store.read_only(path_to_graph)
        with open(path_to_mapping) as f:
            reader = DictReader(f)
            mapping = [r for r in reader]
        self.mapping = {row["predicate"]: row["bulkrax_field"] for row in mapping}
        if isinstance(models, str):
            self.models = models.split(",")
        else:
            self.models = models
        self.pipe_delimited = pipe_delimited
        # Fedora graph data, URI's mapped to predicates
        # Load all data on initialization
        self.collections = self.get_resources()
        self.works = self.get_resources(self.models)
        self.file_sets = self.get_file_sets()
        self.permissions = self.get_group_permissions()
        self.embargos = self.get_embargos()

    def get_resources(self, models: List[str] = None) -> Self:
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

    def get_file_sets(self) -> List[Dict[str, str]]:
        """Returns all filesets with references to parent works."""
        query = """
            prefix fedora: <info:fedora/fedora-system:def/model#>
            prefix ns: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            prefix pcdm: <http://pcdm.org/models#>

            select (?s as ?work) (?fs as ?fileset) (?fn as ?filename) (?fu as ?file_uri)
            where {
                ?s pcdm:hasMember ?fs.
                ?fs ns:type ?fm.
                ?fs fedora:downloadFilename ?fn.
                ?fs pcdm:hasFile ?fu.
                ?fu ns:type <http://pcdm.org/use#OriginalFile>
                filter(str(?fm) = "http://projecthydra.org/works/models#FileSet")
            }
            """
        return [
            {
                "parent": uri_to_id(r["work"].value),
                "id": uri_to_id(r["fileset"].value),
                "file": r["filename"].value,  # Bulkrax CSV fields
                "title": r["filename"].value,  # Bulkrax CSV fields
                "file_url": r["file_uri"].value,
            }
            for r in self.store.query(query)
        ]

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
            key = uri_to_id(k)
            permissions_per_resource[key] = []
            for row in g:
                permissions_per_resource[key].append(row[0])
        return permissions_per_resource

    def get_embargos(self) -> Dict[Dict[str, str]]:
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
            uri_to_id(r["resource"].value): {
                "visibility_during_embargo": r["visibilityDuringEmbargo"].value,
                "visibility_after_embargo": r["visibilityAfterEmbargo"].value,
                "release_date": r["releaseDate"].value,
            }
            for r in self.store.query(query_embargos)
        }
        return results

    # TO DO: extract parent-child relationship for nested works
    def convert_resources(self) -> List[Dict[str, List[str]]]:
        """Converts each resources's set of triples into a dictionary mapping Bulkrax fields to values"""
        rows = []
        for uri, triples in self.data.items():
            row = {"id": uri_to_id(uri)}
            for _, predicate, value in triples:
                # Add a value to the parents columnn for any resources that belong to a collection
                if predicate == FedoraGraph.MEMBERSHIP_PREDICATE:
                    row["parents"] = row.get("parents", []) + [uri_to_id(value)]
                    continue
                bulkrax_field = self.mapping.get(predicate)
                if bulkrax_field:
                    row[bulkrax_field] = row.get(bulkrax_field, []) + [value]
            rows.append(row)
            return rows

    def format_row(self, row: Dict[str, List[str]], is_fileset=False) -> Dict[str, str]:
        """Formats each row for Bulkrax, combining duplicate fields using either a semicolon or a pipe."""
        for field, value in row.items():
            if field == "id":
                continue
            if field in self.pipe_delimited:
                row[field] = "|".join(value)
            else:
                row[field] = "; ".join(value)
        # Use the resource ID for the Bulkrax identifier if not already present
        row["bulkrax_identifier"] = row.get("bulrax_identifier", row["id"])
        # We don't want to provide ID's for FileSets
        if is_fileset:
            del row["id"]
        return row

    def match_permissions(
        self, rows: List[Dict[str, List[str]]]
    ) -> List[Dict[str, List[str]]]:
        """Match permissions to their associated resources, selecting for the highest level of visibility."""
        for resource in rows:
            permission = self.permissions.get(resource["id"])
            if permission:
                visibility = "private"
                for group_uri in permission:
                    group_id = uri_to_id(group_uri).split("#")[-1]
                    match group_id:
                        case "public":
                            visibility = "public"
                            break
                        case "registered":
                            visibility = "restricted"
                resource["visiblity"] = visibility
        return rows

    def match_embargos(
        self, rows: List[Dict[str, List[str]]]
    ) -> List[Dict[str, List[str]]]:
        """Match embargos to their associated resources, filtering for unexpired embargos."""
        for resource in rows:
            embargo = self.embargos.get(resource["id"])
            if embargo:
                if is_active_embargo(embargo):
                    resource.update(embargo)
                # If the embargo release date is in the past, update the visibility per the embargo instructions
                else:
                    resource["visibility"] = embargo["visibility_after_embargo"]
        return rows

    def prepare_importer_rows(self) -> Iterator[Dict[str, str]]:
        for data in [self.collections, self.works]:
            rows = self.convert_resources(data)
            rows = self.match_permissions(rows)
            rows = self.match_embargos(rows)
            for row in rows:
                yield self.format_row(row)
        file_sets = self.match_permissions(self.file_sets)
        file_sets = self.match_embargos(file_sets)
        for fs in file_sets:
            yield self.format_row(fs)
