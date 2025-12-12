import requests
from requests import HTTPError
import click
import logging
from pathlib import Path
from typing import List
from pyoxigraph import Store, parse, serialize, NamedNode, RdfFormat
import subprocess

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


class GraphPart:
    def __init__(self, dirs: List[str | Path], store: str):
        self.dirs = [Path(d) for d in dirs]
        self.g = Store(store)

    def add_nodes(self, ttl: str | Path):
        logging.info(f"Adding {ttl} to graph.")
        self.g.bulk_load(path=ttl)

    def walk(self):
        for d in self.dirs:
            for p in d.rglob("*"):
                if p.is_file() and (p.name.endswith(".ttl") or p.name.endswith(".nt")):
                    self.add_nodes(p)

    def parse_list(self, p_list: List[Path | str]):
        for p in p_list:
            if str(p).endswith(".ttl"):
                self.add_nodes(p)


def delete_object(session, uri):
    try:
        r = session.delete(uri)
        if r.status_code != 204:
            r.raise_for_status()
    except HTTPError:
        logging.error(f"Error deleting {uri}: {r.text}")


@click.group()
def main():
    pass


@main.command()
@click.option(
    "--remote-path", help="Path to OCFL root directory on the remove, to sync FROM."
)
@click.option(
    "--local-path", default="./", help="Path on the local machine, to sync TO "
)
@click.option("--rsync", default="rsync", help="Local path to rsync command.")
def rsync_ocfl(remote_path, local_path, rsync):
    # -rvzWP -f'+ *.nt' -f'+ */'   -f'- *'   --dry-run  aws-gwss-migrate:/data/ocfl-root ./data
    # filter for all .nt files, all directories, exclude all binaries
    command = [
        rsync,
        "-rvzWP",
        "-f",
        "+ *.nt",
        "-f",
        "+ */",
        "-f",
        "- *",
        #        "--dry-run",
        remote_path,
        local_path,
    ]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode == 0:
        print("Rsync completed successfully.")


@main.command()
@click.option("--ttl", help="Path to TTL file to modify.")
def remove_audits(ttl):
    g = parse(path=ttl)
    # Derive prefixes from original TTL file
    with open(ttl) as f:
        rdf = f.read()
    prefixes = [r for r in rdf.split("\n") if r.startswith("@prefix")]
    prefix_dict = {}
    for prefix in prefixes:
        p = prefix.split()
        prefix_dict[p[1][:-1]] = p[2][1:-1]
    # Remove every child except the prod object
    keep = [
        node
        for node in g
        if not (
            node.predicate == NamedNode("http://www.w3.org/ns/ldp#contains")
            and node.object != NamedNode("http://localhost:8984/rest/prod")
        )
    ]
    # Save modified graph
    serialize(input=keep, output=ttl, format=RdfFormat.TURTLE, prefixes=prefix_dict)


@main.command()
@click.option("--root", help="Root path to repository files.")
@click.option("--output", help="Path for saving RDF store.")
def parse_graph(root: str, output: str):
    g = GraphPart([root], output)
    g.walk()
    logging.info("Saving graph.")


@main.command()
@click.option(
    "--objects",
    help="Path to text file containing list of objects to remove, one URI per line.",
)
def remove_orphans(objects):
    with open(objects) as f:
        uris = [r.strip() for r in f]
    session = requests.Session()
    for uri in uris:
        if uri:
            # Replace the localhost with the base URI of the host network
            uri = uri.replace("localhost", "127.0.0.1")
            logging.info(f"Deleting object {uri}")
            delete_object(session, uri)


if __name__ == "__main__":
    main()
