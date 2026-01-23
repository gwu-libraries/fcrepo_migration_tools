from itertools import chain, islice
from os import listdir
from pathlib import Path
from re import L
from zipfile import ZipFile

import pytest
from more_itertools import before_and_after
from pyoxigraph import Literal, NamedNode
from pytools.fcrepo_to_bulkrax import (
    EmbargoMapping,
    FedoraGraph,
    FileSet,
    ParentChildMapping,
    PermissionsMapping,
)


def take(n, iterable):
    "Return first n items of the iterable as a list."
    return list(islice(iterable, n))


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
    return Path(path_to_export) / "fedora-4.7.5-export"


@pytest.fixture(scope="session")
def output_path(tmp_path_factory):
    return tmp_path_factory.mktemp("bulkrax_output")


@pytest.fixture()
def a_fileset_id():
    return "http://localhost:8984/rest/prod/0r/96/73/72/0r967372b"


@pytest.fixture()
def a_work_id():
    return "http://localhost:8984/rest/prod/6t/05/3f/96/6t053f96k"


@pytest.fixture()
def a_file_uri():
    return "http://localhost:8984/rest/prod/0r/96/73/72/0r967372b/files/970d6269-194c-4448-ab23-37aa0027ffe3"


@pytest.fixture()
def a_filename():
    return "TestWordDoc.doc"


@pytest.fixture()
def a_fileset_result(a_fileset_id, a_work_id, a_file_uri, a_filename):
    return {
        "fileset": NamedNode(a_fileset_id),
        "filename": Literal(a_filename),
        "file_uri": NamedNode(a_file_uri),
        "work": NamedNode(a_work_id),
    }


@pytest.fixture()
def a_collection_id():
    return "http://localhost:8984/rest/prod/j6/73/13/76/j67313767"


@pytest.fixture()
def a_collection_result():
    return [
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://schema.org/keywords"),
            "o": Literal("Keyword Collection 1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode(
                "http://fedora.info/definitions/v4/repository#lastModifiedBy"
            ),
            "o": Literal("bypassAdmin"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://fedora.info/definitions/v4/repository#hasParent"),
            "o": NamedNode("http://localhost:8984/rest/prod"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/elements/1.1/creator"),
            "o": Literal("Author, Collection 1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/terms/alternative"),
            "o": Literal("Collection 1 Alt Title"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/elements/1.1/description"),
            "o": Literal("Collection 1 Text"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "o": NamedNode("http://projecthydra.org/works/models#Collection"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "o": NamedNode("http://www.w3.org/ns/ldp#RDFSource"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "o": NamedNode("http://fedora.info/definitions/v4/repository#Container"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "o": NamedNode("http://www.w3.org/ns/ldp#Container"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "o": NamedNode("http://pcdm.org/models#Collection"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            "o": NamedNode("http://fedora.info/definitions/v4/repository#Resource"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/elements/1.1/subject"),
            "o": Literal("Subject 1 Collection 1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/elements/1.1/subject"),
            "o": Literal("Subject 2 Collection 1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/2000/01/rdf-schema#seeAlso"),
            "o": Literal("https://library.gwu.edu/collection1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/elements/1.1/publisher"),
            "o": Literal("Publisher, Collection 1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/terms/title"),
            "o": Literal("Collection 1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/terms/identifier"),
            "o": Literal("coll_1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://schema.org/additionalType"),
            "o": Literal("gid://scholarspace/Hyrax::CollectionType/2"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/terms/created"),
            "o": Literal("2025"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://fedora.info/definitions/v4/repository#createdBy"),
            "o": Literal("bypassAdmin"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/elements/1.1/contributor"),
            "o": Literal("Contributor, Collection 1"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("info:fedora/fedora-system:def/model#hasModel"),
            "o": Literal("Collection"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://id.loc.gov/vocabulary/relators/dpt"),
            "o": Literal("admin@example.com"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/terms/type"),
            "o": Literal("Journal"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/elements/1.1/language"),
            "o": Literal("en"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://purl.org/dc/terms/license"),
            "o": Literal("http://creativecommons.org/publicdomain/zero/1.0/"),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
        {
            "s": NamedNode("http://localhost:8984/rest/prod/j6/73/13/76/j67313767"),
            "p": NamedNode("http://www.w3.org/ns/auth/acl#accessControl"),
            "o": NamedNode(
                "http://localhost:8984/rest/prod/cb/7e/9b/e6/cb7e9be6-76a9-474c-bca9-2feb927ec3b4"
            ),
            "adminSet": NamedNode(
                "http://localhost:8984/rest/prod/ad/mi/n_/se/admin_set/default"
            ),
        },
    ]


@pytest.fixture(scope="session")
def graph(fcrepo_graph, fcrepo_export, output_path):
    fg = FedoraGraph(
        path_to_graph=fcrepo_graph / "fcrepo-graph",
        path_to_root=fcrepo_export,
        path_to_mapping="./fedora_bulkrax_mapping.csv",
        output_path=output_path,
        models="GwWork,GwEtd,GwJournalIssue",
        pipe_delimited="license,rights_statement,doi,related_url",
        batch_size=5,
    )
    return fg


@pytest.fixture()
def collection_ids():
    return ["j67313767", "j6731377h"]


@pytest.fixture()
def work_ids():
    return [
        "02870v844",
        "765371328",
        "k643b116n",
        "3197xm04j",
        "rf55z768s",
        "v979v304g",
        "44558d285",
        "3j3332260",
        "jq085j963",
        "05741r680",
        "zw12z528p",
        "7w62f8209",
        "cj82k728n",
        "6t053f96k",
        "3j333225q",
        "9s1616164",
        "3j333224f",
    ]


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
        "6q182k12h": "restricted",
        "8336h188j": "private",
    }


@pytest.fixture()
def embargos():
    return {
        "2v23vt362": ("authenticated", "2026-12-17T00:00:00Z", "open"),
        "t722h8817": ("restricted", "2026-12-31T00:00:00Z", "open"),
        "05741r680": ("authenticated", "2026-12-17T00:00:00Z", "open"),
    }


@pytest.fixture()
def works(graph):
    return list(graph.get_resources(graph.Work))


@pytest.fixture()
def collections(graph):
    return list(graph.get_resources(graph.Collection))


@pytest.fixture()
def filesets(graph):
    return list(graph.get_filesets())


def test_fileset(a_fileset_result, a_fileset_id, a_work_id, a_file_uri, a_filename):
    fs = FileSet.make_fileset(a_fileset_result)
    assert fs.parents == a_work_id
    assert fs.id == a_fileset_id
    assert fs.file_uri == a_file_uri
    assert fs.title == a_filename
    assert fs.file == a_filename


def test_collection(graph, a_collection_id, a_collection_result):
    collection = graph.Collection.make_resource(a_collection_id, a_collection_result)
    assert collection.id == "http://localhost:8984/rest/prod/j6/73/13/76/j67313767"
    assert collection.keyword == ["Keyword Collection 1"]
    assert collection.depositor == "admin@example.com"
    assert collection.title == ["Collection 1"]
    assert collection.creator == ["Author, Collection 1"]
    assert collection.date_created == ["2025"]
    assert collection.subject == ["Subject 1 Collection 1", "Subject 2 Collection 1"]
    assert collection.model == "Collection"


def test_load_resources(works, collections, collection_ids, work_ids):
    assert len(collections) == 2
    assert len(works) == 17
    for work in works:
        assert work.id.split("/")[-1] in work_ids
    for collection in collections:
        assert collection.id.split("/")[-1] in collection_ids


def test_value_types(works, single_values, multi_values):
    work = works[14]
    for field, value in single_values:
        assert getattr(work, field) == [value]
    for field, value in multi_values:
        assert getattr(work, field) == value


def test_memberships(graph, works, parents_children):
    works = [graph.parents.update_resource(work) for work in works]
    work_dict = {work.id.split("/")[-1]: work for work in works}
    for parent, children in parents_children.items():
        for child in children:
            assert child in work_dict
            assert parent in [p.split("/")[-1] for p in work_dict[child].parents]


def test_permissions(graph, works, filesets, permissions):
    count = 0
    works = [graph.permissions.update_resource(work) for work in works]
    filesets = [graph.permissions.update_resource(fileset) for fileset in filesets]
    for resource in works + filesets:
        r_id = resource.id.split("/")[-1]
        if r_id in permissions:
            count += 1
            assert resource.visibility == permissions[r_id]
        else:
            assert resource.visibility == "open"

    assert count == len(permissions)


def test_embargos(graph, works, filesets, embargos):
    works = [graph.permissions.update_resource(work) for work in works]
    filesets = [graph.permissions.update_resource(fileset) for fileset in filesets]
    works = [graph.embargos.update_resource(work) for work in works]
    filesets = [graph.embargos.update_resource(fileset) for fileset in filesets]

    for resource in works + filesets:
        r_id = resource.id.split("/")[-1]
        if r_id in embargos:
            assert resource.visibility == "embargo"
            assert (
                resource.visibility_during_embargo,
                resource.release_date,
                resource.visibility_after_embargo,
            ) == embargos[r_id]
        else:
            assert resource.visibility != "embargo"


def test_ordering(works, filesets, collections):
    work = works[0]
    fileset = filesets[0]
    assert work.id == fileset.parents


def test_bulkrax_rows(graph, output_path):
    rows_iter = graph.prepare_import_batches()
    _, batch_1, copied_files = next(rows_iter)
    # Should emit collections first
    assert {r["model"] for r in batch_1[:2]} == {"Collection"}
    # Each batch should contain all files for the works in that batch
    filenames = []
    batch_ids = []
    for batch in chain([[0, batch_1[2:], []]], rows_iter):
        rows = batch[1]
        batch_ids.append(batch[0])
        copied_files.extend(batch[2])
        work_ids = [
            row["bulkrax_identifier"] for row in rows if row["model"] != "FileSet"
        ]
        file_parents = [row["parents"] for row in rows if row["model"] == "FileSet"]
        for f_p in file_parents:
            assert f_p in work_ids
        filenames.extend([row["file"] for row in rows if row["model"] == "FileSet"])
    for file in filenames:
        assert file in [f.name for f in copied_files]
    assert len(filenames) == len(copied_files)
