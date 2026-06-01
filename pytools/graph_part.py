import logging
from pathlib import Path
from typing import List

from pyoxigraph import Store

logger = logging.getLogger(__name__)


class GraphPart:
    def __init__(self, dirs: List[str | Path], store: str):
        self.dirs = [Path(d) for d in dirs]
        self.g = Store(store)

    def add_nodes(self, ttl: str | Path):
        logger.info(f"Adding {ttl} to graph.")
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
