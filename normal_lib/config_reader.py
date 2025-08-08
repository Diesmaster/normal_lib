import json
from pathlib import Path
from copy import deepcopy
from normal_lib.config import Config


class ConfigReader:
    def __init__(self, file_path):
        self.docIdAttrName = Config.docIdAttrName 
        self.file_path = Path(file_path)
        self.config = self._load_config()
        self.config_dict = self.get_config()


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

    def parse_string_link(self, link):
        """
        Splits a string like 'users.username' into ('users', 'username')
        """
        if not isinstance(link, str) or '.' not in link:
            raise ValueError("Link must be a string in 'collection.attribute' format.")
        collection, attribute = link.split('.', 1)
        return collection, attribute

    def generate_string_link(self, collection, attribute):
        """
        Combines collection and attribute into a string like 'users.username'
        """
        if not collection or not attribute:
            raise ValueError("Both collection and attribute must be provided.")
        return f"{collection}.{attribute}"

    def generate_refId(self, coll):
        return f"{coll}{self.docIdAttrName}"


    def compile_refs(self, config):

        ret_config = deepcopy(config)

        for key in config:
            for field in config[key]['fields']:
                if 'link' in config[key]['fields'][field] and 'idRef' in config[key]['fields'][field]:
                    my_link = self.generate_string_link(key, field)
                    refId = self.generate_refId(key)

                    for link in config[key]['fields'][field]['link']:
                        col, attr = self.parse_string_link(link)

                        if not 'link' in config[col]['fields'][attr]:
                            ret_config[col]['fields'][attr]['link'] = []

                        if not 'refId' in config[col]['fields'][attr]:
                            ret_config[col]['fields'][attr]['idRef'] = []


                        if my_link not in ret_config[col]['fields'][attr]['link']:
                            ret_config[col]['fields'][attr]['link'].append(my_link)
                            ret_config[col]['fields'][attr]['idRef'].append(refId) 
                            
                            if not 'indpended' in ret_config[col]['fields'][attr]:
                                ret_config[col]['fields'][attr]['indpended'] = True
    
                            if not 'origin' in ret_config[col]['fields'][attr]:
                                ret_config[col]['fields'][attr]['origin'] = True


                elif ('link' in config[key]['fields'][field]) ^ ('idRef' in config[key]['fields'][field]):
                    raise ValueError(f"Only link or refId present in field: {field}, in collection: {key}")

        return ret_config

    def get_config(self):
        ##
        config = {}
        for collection in self.config.get('collections', []):
            config[collection['name']] = deepcopy(collection)

            config[collection['name']]['fields'] = {}

            for field in collection.get('fields', []):
                config[collection['name']]['fields'][field['name']] = field 

        config = self.compile_refs(config) 

        return config

    def get_collections(self):
        return self.config.get("collections", [])

    def get_fields_for_collection(self, collection_name):
        for collection in self.get_collections():
            if collection.get("name") == collection_name:
                return collection.get("fields", [])
        return []


