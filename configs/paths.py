import os
from pydantic import BaseModel


class DataPaths(BaseModel):
    base_path: str = './data/processed/'
    directional_rels: str = 'directional_rels_raw.txt'
    bidirectional_rels: str = 'bidirectional_rels_raw.txt'
    attribute_rels: str = 'attribute_rels_raw.txt'
    directional_rels_procd: str = 'directional_rels_procd.pkl'
    bidirectional_rels_procd: str = 'bidirectional_rels_procd.pkl'
    attribute_rels_procd: str = 'attribute_rels_procd.pkl'
    concat_relationships_procd: str = 'relations.txt'
    nodes_raw: str = 'nodes_raw.txt'
    nodes_procd: str = 'nodes.txt'
    nodes_header: str = 'nodes_header.txt'
    relationships_header: str = 'relations_header.txt'

    def get_full_path(self, file_name: str) -> str:
        return os.path.join(self.base_path, file_name)
