from normal_lib.config_reader import ConfigReader
from normal_lib.db_interface import DBInterface
from normal_lib.class_funcs import ClassFuncs


class Normalizer:
    def __init__(self, db_driver, config_path):
        """
        db_driver: MongoDB instance or similar
        config_path: path to a .json config file
        """
        self.config_reader = ConfigReader(config_path)
        self.config = self.config_reader.get_config()
        self.interface = DBInterface(db_driver, self.config)
        self._collection_classes = self._init_class_funcs()

    def _init_class_funcs(self):
        result = {}
        print(self.config)
        for col_name in self.config.keys():
            print(f"col: {col_name}")
            #name = self.config.get(col_name)
            result[col_name] = ClassFuncs(
                db_interface=self.interface,
                config=self.config,
                collection_name=col_name
            )
        return result

    def get_classes(self):
        """
        Returns a dict of collection_name: ClassFuncs instance
        """
        return self._collection_classes

    def add(self, collection_name, document):
        return self.interface.add(collection_name, document)

    def delete(self, collection_name, doc_id):
        return self.interface.delete(collection_name, doc_id)

    def modify(self, collection_name, doc_id, updates):
        return self.interface.modify(collection_name, doc_id, updates)

