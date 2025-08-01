from pymongo import MongoClient
from bson.objectid import ObjectId

class MongoDriver:
    def __init__(self, uri="mongodb://localhost:27017", db_name="mydb"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def add(self, collection_name, document):
        collection = self.db[collection_name]
        result = collection.insert_one(document)
        return str(result.inserted_id)

    def delete(self, collection_name, doc_id):
        collection = self.db[collection_name]
        result = collection.delete_one({"_id": ObjectId(doc_id)})
        return result.deleted_count > 0

    def modify(self, collection_name, doc_id, updates: dict):
        collection = self.db[collection_name]
        result = collection.update_one(
            {"_id": ObjectId(doc_id)},
            {"$set": updates}
        )
        return result.modified_count > 0

