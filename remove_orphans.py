import requests
from requests import HTTPError
import click
import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def delete_object(session, uri):
    try:
        r = session.delete(uri)
        if r.status_code != 204:
            r.raise_for_status()
    except HTTPError:
        logging.error(f"Error deleting {uri}: {r.text}")


@click.command()
@click.option(
    "--objects",
    help="Path to CSV containing list of objects to remove, one URI per line.",
)
def remove_orphans(objects):
    with open(objects) as f:
        uris = [r.strip() for r in f]
    session = requests.Session()
    for uri in uris:
        if uri:
            logging.info(f"Deleting object {uri}")
            delete_object(session, uri)


if __name__ == "__main__":
    remove_orphans()
