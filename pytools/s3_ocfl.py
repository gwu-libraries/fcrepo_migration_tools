import asyncio
from pathlib import Path
from typing import Any

import polars as pl
from aiobotocore.session import get_session
from botocore.credentials import Credentials
from pyoxigraph import Store

from pytools.graph_part import GraphPart


class S3OcflRepo:
    """Utilities for retrieving content from an S3 bucket acting as Fedora's OCFL store."""

    def __init__(
        self,
        credentials: Credentials,
        region,
        bucket,
        path_to_ocfl: str,
        path_to_graph: str | None = None,
    ):
        """:param credentials: AWS credentials with bucket access
        region: AWS region
        bucket: S3 bucket
        path_to_ocfl: to root of repo in s3 bucket, e.g., fedora-ocfl-hyrax-5/data/ocfl-root
        graph: path to pyoxigraph.Store instance (if not provided, one will be created)
        """
        self.credentials = credentials
        self.bucket = bucket
        self.path_to_ocfl = path_to_ocfl
        self.region = region
        self.path_to_graph = path_to_graph

        self.loop = asyncio.get_event_loop()  # For running async tasks

    def prepare_repo(self, inventory_key, download_path: str):
        """
        1) Retrieves an inventory (Parquet) file listing all the OCFL objects by key
        2) Filters the inventory into two parts:
            a) .nt files containing RDF metadata
            b) /original binary files
        3) Retrieves checksums for all binaries
        4) If an instance of a pyoxigraph.Store does not yet exist, downloads all .nt objects (not binaries) and populates one locally
        """
        self.download_path = Path(download_path)
        inventory_path = self.loop.run_until_complete(
            self.download_inventory(inventory_key, download_path)
        )
        self.filter_inventory(inventory_path)

        self.checksums = self.loop.run_until_complete(
            self.get_object_checksums(self.originals.rows(named=True))
        )
        self.checksum_errors = [
            obj for obj in self.checksums if isinstance(obj, Exception)
        ]
        # TO DO: log errors
        if not self.path_to_graph:
            downloaded_objects = self.loop.run_until_complete(
                self.download_nt_objects(
                    self.rdf_df.rows(named=True), self.download_path / "rdf"
                )
            )
            self.download_errors = [
                obj for obj in downloaded_objects if isinstance(obj, Exception)
            ]
            g = GraphPart(
                dirs=[self.download_path / "rdf"],
                store=str(self.download_path / "hyrax-5-migrated"),
            )
            g.walk()
            self.path_to_graph = str(self.download_path / "hyrax-5-migrated")
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
            df.filter(
                pl.col("key").str.ends_with(".nt")
                & pl.col("key").str.starts_with(self.path_to_ocfl)
            )
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
        self.originals = df.filter(
            pl.col("key").str.starts_with(self.path_to_ocfl)
            & pl.col("key").str.ends_with("original")
        )
        # Check that we're not dealing with multiple versions
        # If so, this logic needs to be more complicated
        assert self.originals.filter(pl.col("key").str.contains("v2")).is_empty()
