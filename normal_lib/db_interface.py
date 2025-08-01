from normal_lib.validator import Validator, ValidationError

class DBInterface:
    def __init__(self, db_impl, config: dict):
        """
        db_impl: an instance of something like MongoDB
        config: full config dict from ConfigReader.get_config()
        """
        self.db = db_impl
        self.config = config

    def add(self, collection_name, document):
        return self.db.add(collection_name, document)

    def delete(self, collection_name, doc_id):
        return self.db.delete(collection_name, doc_id)

    def modify(self, collection_name, doc_id, updates: dict):
        return self.db.modify(collection_name, doc_id, updates)

    def _get_collection_config(self, collection_name):
        for collection in self.config.get("collections", []):
            if collection.get("name") == collection_name:
                return collection.get("fields", [])
        raise ValueError(f"Collection '{collection_name}' not found in config")

