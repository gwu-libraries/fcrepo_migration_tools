from itertools import groupby
from typing import Iterator, Tuple

from pytools.resources import *
from pytools.utils import convert_date, is_active_embargo, uri_to_id


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

    def update_resource(self, resource: Resource | FileSet) -> Resource | FileSet:
        permission = self.permissions_per_resource.get(resource.id)
        if permission:
            visibility = "restricted"
            for group_uri in permission:
                group_id = uri_to_id(group_uri).split("#")[-1]
                match group_id:
                    case "public":
                        visibility = "open"
                        break
                    case "registered":
                        visibility = "authenticated"
            resource.update("visibility", visibility)
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
                "embargo_release_date": r["releaseDate"].value,
                "visibility": "embargo",
            }
        return self

    def update_resource(self, resource: Resource | FileSet) -> Resource | FileSet:
        embargo = self.embargo_per_resource.get(resource.id)
        if embargo:
            if is_active_embargo(embargo):
                embargo["embargo_release_date"] = convert_date(
                    embargo["embargo_release_date"]
                )
                for k, v in embargo.items():
                    resource.update(k, v)
            # If the embargo release date is in the past, update the visibility per the embargo instructions
            else:
                resource.update("visibility", embargo["visibility_after_embargo"])
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

    def update_resource(self, resource: Resource | FileSet) -> Resource | FileSet:
        parents = self.parent_child_mapping.get(resource.id)
        if parents:
            resource.update("parents", parents)
        return resource
