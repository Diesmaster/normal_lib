from normal_lib.validator import Validator, ValidationError

class ClassFuncs:
    def __init__(self, db_interface, config, collection_name):
        """
        db_interface: instance of DBInterface
        config: full config dict from ConfigReader.get_config()
        collection_name: e.g., "users"
        """
        self.db = db_interface
        self.config = config
        self.collection_name = collection_name

        # Extract field config for this collection and initialize validator
        self.fields_config = self._get_fields_config()
        print("fields: ")
        print(self.fields_config)
        self.validator = Validator({"fields": self.fields_config})

    def _get_fields_config(self):
        
        if self.collection_name in self.config:
            return self.config[self.collection_name].get("fields", {})
        
        raise ValueError(f"Collection '{self.collection_name}' not found in config")

    def add(self, document):
        """Add after validating."""
        try:
            self.validator.validate(document)
            print("we get here?")
        except ValidationError as e:
            raise ValueError(f"Validation failed:\n{e}")
        print("yes yes")
        return self.db.add(self.collection_name, document)

    def delete(self, doc_id):
        """Default delete method — will be replaced or extended later."""
        return self.db.delete(self.collection_name, doc_id)

    def modify(self, doc_id, updates):
        """Default modify method — will be replaced or extended later."""
        return self.db.modify(self.collection_name, doc_id, updates)

