from itertools import islice
from os import listdir
from zipfile import ZipFile

import pytest
from pytools.fcrepo_to_bulkrax import FedoraGraph


def id_to_uri(_id):
    return f"http://localhost:8984/rest/prod/{_id[:2]}/{_id[2:4]}/{_id[4:6]}/{_id[6:8]}/{_id}"


@pytest.fixture(scope="session")
def fcrepo_graph(tmp_path_factory):
    path_to_graph = tmp_path_factory.mktemp("fcrepo_graph")
    with ZipFile("./tests/fcrepo-graph.zip") as zf:
        zf.extractall(path=path_to_graph)
    return path_to_graph


@pytest.fixture(scope="session")
def fcrepo_export(tmp_path_factory):
    path_to_export = tmp_path_factory.mktemp("fcrepo_export")
    with ZipFile("./tests/fedora-4.7.5-export.zip") as zf:
        zf.extractall(path=path_to_export)
    return path_to_export


@pytest.fixture(scope="session")
def output_path(tmp_path_factory):
    return tmp_path_factory.mktemp("bulkrax_output")


@pytest.fixture(scope="session")
def fedora_graph_obj(fcrepo_graph, fcrepo_export, output_path):
    fg = FedoraGraph(
        path_to_graph=fcrepo_graph / "fcrepo-graph",
        path_to_root=fcrepo_export,
        path_to_mapping="./fedora_bulkrax_mapping.csv",
        output_path=output_path,
        models="GwWork,GwEtd,GwJournalIssue",
        pipe_delimited="license,rights_statement,doi,related_url",
    )
    return fg


@pytest.fixture(scope="session")
def works(fedora_graph_obj):
    return sorted(
        [r for r in fedora_graph_obj.convert_resources(fedora_graph_obj.works)],
        key=lambda x: x["date_uploaded"],
    )


@pytest.fixture()
def collection_ids():
    return ["j67313767", "j6731377h"]


@pytest.fixture()
def work_ids():
    return ["k643b116n", "v979v304g", "6t053f96k", "3197xm04j", "3j333224f"]


@pytest.fixture()
def single_values():
    return [
        ("title", "Work 1"),
        ("creator", "Author, Work 1"),
        ("contributor", "Contributor, Work 1"),
        ("publisher", "Publisher, Work 1"),
        ("description", "Work 1 \r\nWork 1\r\nWork 1"),
        ("language", "en"),
        ("identifier", "work_1"),
        ("doi", "https://doi.org/10.4079/work.1"),
        ("date_created", "2001"),
        ("resource_type", "Article"),
        ("rights_statement", "http://rightsstatements.org/vocab/InC/1.0/"),
        ("license", "http://creativecommons.org/licenses/by/4.0/"),
        ("model", "GwWork"),
        (
            "gw_affiliation",
            "ACCESS Center for Advancement of Research in Distance Education",
        ),
    ]


@pytest.fixture()
def multi_values():
    return [("keyword", ["Keyword 1", "Keyword 2"])]


@pytest.fixture()
def parents_children():
    return {
        "j67313767": ["rf55z768s", "9s1616164", "7w62f8209"],
        "j6731377h": ["3197xm04j", "zw12z528p"],
        "v979v304g": ["k643b116n"],
    }


@pytest.fixture()
def permissions():
    return {
        "cj82k728n": "restricted",
        "05741r680": "restricted",
        "2v23vt362": "restricted",
        "t722h8817": "private",
        "n296wz12m": "private",
        "j6731377h": "restricted",
        "6q182k12h": "restricted",
    }


@pytest.fixture()
def embargos():
    return {
        "2v23vt362": ("restricted", "2026-12-17T00:00:00+00:00Z", "open"),
        "t722h8817": ("private", "2026-12-31T00:00:00+00:00Z", "open"),
    }


@pytest.fixture(scope="session")
def bulkrax_rows(fedora_graph_obj):
    data = fedora_graph_obj.collections
    data.update(fedora_graph_obj.works)
    return [r for r in fedora_graph_obj.prepare_import_rows(data)]


def test_load_resources(fedora_graph_obj, collection_ids, work_ids):
    for attr, resource in [("collections", collection_ids), ("works", work_ids)]:
        keys = [k.split("/")[-1] for k in getattr(fedora_graph_obj, attr).keys()]
        for _id in resource:
            assert _id in keys
    assert len(fedora_graph_obj.works) == 17


def test_convert_a_work(works, single_values, multi_values):
    work = works[0]
    for field, value in single_values:
        assert work[field] == [value]
    for field, value in multi_values:
        assert work[field] == value


def test_memberships(fedora_graph_obj, parents_children):
    resources = fedora_graph_obj.convert_resources(fedora_graph_obj.works)
    resources = [fedora_graph_obj.format_row(resource) for resource in resources]
    resources = {resource["id"]: resource for resource in resources}
    for parent, children in parents_children.items():
        for child in children:
            assert child in resources
            assert parent in resources[child]["parents"]


def test_permissions(bulkrax_rows, permissions):
    count = 0
    for r, _ in bulkrax_rows:
        if r["bulkrax_identifier"] in permissions:
            count += 1
            assert r["visibility"] == permissions[r["bulkrax_identifier"]]
        else:
            assert r["visibility"] == "open"

    assert count == len(permissions)


def test_embargos(bulkrax_rows, embargos):
    for row, _ in bulkrax_rows:
        if embargo := embargos.get(row["bulkrax_identifier"]):
            assert row["visibility"] == "embargo"
            assert (
                row["visibility_during_embargo"],
                row["release_date"],
                row["visibility_after_embargo"],
            ) == embargo


def test_prepare_import_rows(bulkrax_rows):
    # Total number of entries
    assert (len(bulkrax_rows)) == 39
    # Each entry has a unique identifier
    assert len(bulkrax_rows) == len({r[0]["bulkrax_identifier"] for r in bulkrax_rows})
    # Number of files
    assert len([r for r in bulkrax_rows if r[0].get("file")]) == 20
