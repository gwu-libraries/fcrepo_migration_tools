import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple
from urllib.parse import urlparse
from uuid import uuid1

from pytools.utils import uri_to_id


class Resource:
    def __init__(self, id, admin_set, field_defaults=None, **kwargs):
        self.id = id
        self.admin_set = admin_set
        self.field_defaults = field_defaults
        self.data = {}
        for key, value in kwargs.items():
            self.data[key] = value

    @classmethod
    def make_resource(
        cls,
        id: str,
        triples: Iterator,
        mapping: Dict[str, Tuple[str, bool]],
        field_defaults=None,
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
        return cls(id=id, admin_set=admin_set, field_defaults=field_defaults, **kwargs)

    def update(self, field, value):
        # Used for setting visibility and embargoes on the resource, which happens after init
        self.data[field] = value

    @property
    def parents(self):
        return self.data.get("parents", [])

    @property
    def model(self):
        return self.data["model"]

    def format_row(self, formatter):
        # Add the resource ID to the field/value pairs before formatting
        self.data["id"] = self.id
        return formatter(self.data)


class Work(Resource):
    pass


class Collection(Resource):
    def format_row(self, formatter):
        # Can't have the creator field blank when importing collections
        if not self.data.get("creator"):
            self.data["creator"] = self.field_defaults["creator"]
        return super().format_row(formatter)


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
    whitespace = re.compile(r"\s+")

    @staticmethod
    def make_fileset(triple):
        parents, id, file, file_uri = (
            triple["work"].value,
            triple["fileset"].value,
            triple["filename"].value,
            triple["file_uri"].value,
        )
        return FileSet(
            parents=parents,
            id=id,
            file=f"{uri_to_id(id)}_{re.sub(FileSet.whitespace, '', file)}",  # Remove white spaces in file name and prefix with ID
            title=file,
            file_uri=file_uri,
        )

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

    def update(self, field, value):
        # For updating visibility and embargo attributes, post-init
        setattr(self, field, value)

    def format_row(self, formatter):
        row = formatter({k: v for k, v in self.__dict__.items() if k != "file_uri"})
        # Don't re-use legacy ID for Bulkrax ID for filesets
        row["bulkrax_identifier"] = str(uuid1())
        return row
