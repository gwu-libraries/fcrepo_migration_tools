from collections import defaultdict

import pytest

from pytools.utils import BatchHandler


@pytest.fixture()
def resource():
    class Resource:
        def __init__(self, num):
            self.id = num

        def format_row(self, formatter):
            return {"id": self.id, "type": "Resource"}

    return Resource


@pytest.fixture()
def resources(resource):
    return [resource(i) for i in range(50)]


@pytest.fixture()
def fileset(resources):
    class FileSet:
        def __init__(self, num, parents):
            self.id = num
            self.name = f"FileSet_{self.id}"
            self.parents = parents

        def format_row(self, formatter):
            return {"id": self.id, "name": self.name}

    return FileSet


@pytest.fixture()
def filesets(fileset, resources):
    return [fileset(i, p.id) for (i, p) in zip(range(50, 100), resources)]


@pytest.fixture()
def handler():
    def _copy_files_mock(batch_id, files):
        return [(batch_id, fs.name) for fs in files]

    handler = BatchHandler(
        batch_size=5, formatter=lambda x: x, output_path="./", path_to_root=""
    )
    handler._copy_files = _copy_files_mock
    return handler


def test_batches(handler, filesets, resources):
    # Add one resource/fileset
    handler.add_resource(resources[0])
    handler.add_resource(filesets[0], True)
    assert not handler.current_batch(), "Should not emit a batch yet"
    # Add resources enough to make one batch
    for i, resource in enumerate(resources[1:5]):
        handler.add_resource(resource)
        handler.add_resource(filesets[i + 1], True)
    br = handler.current_batch()
    # Used all rows in this batch
    assert {b["id"] for b in br.rows} == {0, 1, 2, 3, 4, 50, 51, 52, 53, 54}, (
        "Should have included all resources and filesets in this batch"
    )
    assert handler.resources == [], "Internal list of rows should be empty"

    batches = []
    files_copied = []
    # Add remaining resources
    for resource, fileset in zip(resources[5:], filesets[5:]):
        handler.add_resource(resource)
        handler.add_resource(fileset, True)
        br = handler.current_batch()
        if br:
            batches.append(br.rows)
            files_copied.extend(br.files_copied)
    assert len(batches) == 9, "Should have batched all rows"
    assert len(files_copied) == 45, "Should have copied all files"


def test_batches_done(handler, filesets, resources):
    # Add a few more works + files
    for resource in resources[:4]:
        handler.add_resource(resource)
    for fileset in filesets[:4]:
        handler.add_resource(fileset, True)
    br = handler.current_batch(done=True)
    assert len(br.rows) == 8, "Should have returned all rows"
    assert len(br.files_copied) == 4, "Should have copied all files"
    # Add more files, beyond the batch size
    for fileset in filesets[4:13]:
        handler.add_resource(fileset, True)
    # Expect two batches
    br = handler.current_batch()
    assert len(br.rows) == 5, "Should not release more rows than max batch size"
    assert len(br.files_copied) == 5, "Should not copy more files than max batch size"
    for fileset in filesets[4:9]:
        assert fileset.name in [f[1] for f in br.files_copied], (
            "Should contain all filesets from released batch"
        )
    # Batch 2
    br = handler.current_batch()
    assert len(br.rows) == 4, "Should release remainder"
    for fileset in filesets[9:13]:
        assert fileset.name in [f[1] for f in br.files_copied], (
            "Should contain all filesets from released batch"
        )
