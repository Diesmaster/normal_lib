import json
from pathlib import Path
from copy import deepcopy


class ConfigReader:
    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.config = self._load_config()

    def _load_config(self):
        if not self.file_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.file_path}")
        if not self.file_path.suffix == ".json":
            raise ValueError("Config file must be a .json file")

        with open(self.file_path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON: {e}") from e

    def get_config(self):
        ##
        config = {}
        for collection in self.config.get('collections', []):
            config[collection['name']] = deepcopy(collection)

            config[collection['name']]['fields'] = {}

            for field in collection.get('fields', []):
                config[collection['name']]['fields'][field['name']] = field 

        return config

    def get_collections(self):
        return self.config.get("collections", [])

    def get_fields_for_collection(self, collection_name):
        for collection in self.get_collections():
            if collection.get("name") == collection_name:
                return collection.get("fields", [])
        return []


