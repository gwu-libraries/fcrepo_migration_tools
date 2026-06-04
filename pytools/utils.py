import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictWriter
from datetime import datetime
from io import StringIO
from pathlib import Path
from shutil import copy2
from typing import List
from uuid import uuid1
from zipfile import ZipFile

from pytools.queue import StaggeredQueue

logger = logging.getLogger(__name__)


def uri_to_id(uri: str | List[str]):
    if isinstance(uri, list):
        return [uri_to_id(element) for element in uri]
    return uri.split("/")[-1]


def convert_date(date_str: str) -> str:
    # Format date without timestamp for Bulkrax
    return datetime.fromisoformat(date_str).replace(tzinfo=None).strftime("%Y-%m-%d")


def is_active_embargo(record) -> bool:
    return (
        datetime.fromisoformat(record["embargo_release_date"]).replace(tzinfo=None)
        >= datetime.now()
    )


class BatchResult:
    def __init__(self, batch_id, batch, files_copied, batch_handler):
        self.batch_id = batch_id
        self.rows = batch
        self.files_copied = files_copied
        self.batch_handler = batch_handler

    def make_csv(self):
        output = StringIO()
        writer = DictWriter(
            output, fieldnames=list({k for row in self.rows for k in row})
        )
        writer.writeheader()
        for row in self.rows:
            writer.writerow(row)
        return output.getvalue()

    def cleanup_files(self, path_to_batch):
        for file in self.files_copied:
            if Path(file).exists():
                Path(file).unlink()
        if (path_to_batch / "files").exists():
            (path_to_batch / "files").rmdir()
        path_to_batch.rmdir()

    def save_zip(self):
        try:
            path_to_batch = (
                Path(self.batch_handler.output_path) / f"batch_{self.batch_id}"
            )
            path_to_batch.mkdir(exist_ok=True)
            zipfile_path = self.batch_handler.output_path / f"{path_to_batch.name}.zip"
            with ZipFile(zipfile_path, "w") as f:
                f.mkdir("files")
                f.writestr(f"{path_to_batch.name}.csv", data=self.make_csv())
                for file in self.files_copied:
                    file = Path(file)
                    f.write(file, arcname=f"files/{file.name}")
            self.cleanup_files(path_to_batch)

            msg = f"Zip file prepared for batch {self.batch_id}: {str(zipfile_path)}"
            logger.info(msg)
        except Exception as e:
            error_msg = f"Error creating zipfile for batch {self.batch_id}"
            logger.error(error_msg, e)
            raise


class BatchHandler:
    """Handles preparation of batches of data for CSV output"""

    def __init__(self, batch_size, formatter, output_path, path_to_root, dry_run=False):
        self.batch_size = batch_size
        self.formatter = formatter
        self.output_path = Path(output_path)
        self.path_to_root = path_to_root
        self.dry_run = dry_run

        self.resources = []
        self.files_staging = []  # next batch of files to copy
        self.processed = set()
        self.done = False
        self.fileset_queue = StaggeredQueue(
            lambda x: x.parents
        )  # Ensures that filesets are not released such that more than one per batch has the same parent

    def current_batch(self, done=False):
        self.done = self.done or done
        batch_id, rows = next(self)
        if batch_id:
            files_copied = self.copy_files(batch_id, True)
            return BatchResult(batch_id, rows, files_copied, self)

    def __next__(self):
        # If we're not done, only provide rows if we have enough to make a batch
        if not self.done and len(self.resources) < self.batch_size:
            return None, None
        # Remove this batch from the available rows
        rows = []
        for resource in self.resources[: self.batch_size]:
            rows.append(resource.format_row(self.formatter))
        self.resources = self.resources[len(rows) :]
        for fileset in self.fileset_queue.take(self.batch_size):
            # Move file to staging
            self.files_staging.append(fileset)
            rows.append(fileset.format_row(self.formatter))
        if rows:
            batch_id = uuid1()
            return batch_id, rows
        return None, None

    def add_resource(self, resource, is_fileset=False):
        """Expects resource to have format_row method, which is called with the formatted passed in on initialization"""
        if not is_fileset:
            self.resources.append(resource)
            self.processed.add(resource.id)
        else:
            self.fileset_queue.add(resource)

    def copy_files(self, batch_id, concurrently=False):
        if concurrently:
            result = self.copy_files_concurrently(batch_id, self.files_staging)
        else:
            result = self._copy_files(batch_id, self.files_staging)
        self.files_staging = []
        return result

    def _copy_files(self, batch_id, files):
        """Copy binary files associated with filesets to the specified destination. Renames file using filename metadata."""
        output = []
        pd = self.output_path / f"batch_{batch_id}/files"
        pd.mkdir(parents=True, exist_ok=True)
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

    def copy_files_concurrently(self, batch_id, file_sets):
        with ThreadPoolExecutor() as exe:
            # copy files in batches of 10
            data = []
            futures = {
                exe.submit(self._copy_files, batch_id, file_sets[i : i + 10]): i
                for i in range(0, len(file_sets), 10)
            }
            for future in as_completed(futures):
                try:
                    data.extend(future.result())
                except Exception as e:
                    error_msg = f"Error copying files in batch {batch_id}"
                    logger.error(error_msg, e)
                    continue
        return data
