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

    def add_element_to_array(
        self,
        collection_name: str,
        doc_id: str,
        array_field: str,
        element,
        unique: bool = False
    ) -> bool:
        collection = self.db[collection_name]
        oid = ObjectId(doc_id)

        # 1) Ensure the field is an array (initialize to [] if missing/null/non-array)
        doc = collection.find_one({"_id": oid}, {array_field: 1})
        if not doc or not isinstance(doc.get(array_field), list):
            collection.update_one({"_id": oid}, {"$set": {array_field: []}})

        # 2) Now safely add the element
        op = {"$addToSet": {array_field: element}} if unique else {"$push": {array_field: element}}
        result = collection.update_one({"_id": oid}, op)
        return result.modified_count > 0

    def get(self, collection_name, query=None):
        """
        Retrieve documents from a collection. Defaults to all documents.
        Returns a list of dicts with _id converted to string.
        """
        collection = self.db[collection_name]
        if query is None:
            query = {}
        docs = list(collection.find(query))
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs
