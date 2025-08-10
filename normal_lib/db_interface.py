from normal_lib.validator import Validator, ValidationError

class DBInterface:
    def __init__(self, db_impl, config: dict):
        """
        db_impl: an instance of something like MongoDB
        config: full config dict from ConfigReader.get_config()
        """
        self.db = db_impl
        self.config = config

    def add(self, collection_name, document, doc_id=None):
        return self.db.add(collection_name, document, doc_id)

    def delete(self, collection_name, doc_id):
        return self.db.delete(collection_name, doc_id)

    def modify(self, collection_name, doc_id, updates: dict):
        return self.db.modify(collection_name, doc_id, updates)

    def add_element_to_array(self, collection_name, doc_id, array_field, element,  unique: bool = False):
        """
        Add an element to an array field in a document.

        If unique=True, no duplicates will be added.
        """
        return self.db.add_element_to_array(collection_name, doc_id, array_field, element,  unique)

    def remove_element_from_array(self, collection_name, doc_id, array_field, element):
        """
        Remove an element from an array field in a document.
        """
        return self.db.remove_element_from_array(collection_name, doc_id, array_field, element)

    def _get_collection_config(self, collection_name):
        for collection in self.config.get("collections", []):
            if collection.get("name") == collection_name:
                return collection.get("fields", [])
        raise ValueError(f"Collection '{collection_name}' not found in config")

    def get(self, collection_name, query=None):
        return self.db.get(collection_name, query)

    def get_by_id(self, collection_name, doc_id):
        """
        Retrieve a single document by its _id.
        """
        return self.db.get_by_id(collection_name, doc_id)

    def get_flat_tree(self, collection_name, root_id, children_field, include_root=True):
        """
        Retrieve a flat list of the root and all descendants from a tree structure,
        walking only by the `children_field`.
        """
        return self.db.get_tree_flat(
            collection_name=collection_name,
            root_id=root_id,
            children_field=children_field,
            include_root=include_root
        )

