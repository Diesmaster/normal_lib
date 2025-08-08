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

    def add_element_to_array(self, collection_name, doc_id, array_field, element, unique: bool = False):
        """
        Add an element to an array field in a document.

        If unique=True, no duplicates will be added.
        """
        return self.db.add_element_to_array(collection_name, doc_id, array_field, element, unique)

    def _get_collection_config(self, collection_name):
        for collection in self.config.get("collections", []):
            if collection.get("name") == collection_name:
                return collection.get("fields", [])
        raise ValueError(f"Collection '{collection_name}' not found in config")

    def get(self, collection_name, query=None):
        return self.db.get(collection_name, query)

