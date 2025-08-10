from pymongo import MongoClient
from bson.objectid import ObjectId

class MongoDriver:
    def __init__(self, uri="mongodb://localhost:27017", db_name="mydb"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def add(self, collection_name, document, doc_id=None):
        collection = self.db[collection_name]

        # If a custom doc_id is provided, store it explicitly
        if doc_id is not None:
            document["_id"] = ObjectId(doc_id) if ObjectId.is_valid(str(doc_id)) else str(doc_id)

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
        
        oid = ObjectId(doc_id) if ObjectId.is_valid(str(doc_id)) else str(doc_id)

        # 1) Ensure the field is an array (initialize to [] if missing/null/non-array)
        doc = collection.find_one({"_id": oid}) #, {array_field: 1}

        if not doc or not isinstance(doc.get(array_field), list):
            collection.update_one({"_id": oid}, {"$set": {array_field: []}})

        

        # 2) Now safely add the element
        op = {"$addToSet": {array_field: element}} if unique else {"$push": {array_field: element}}
        result = collection.update_one({"_id": oid}, op)
        return result.modified_count > 0

    def remove_element_from_array(
        self,
        collection_name: str,
        doc_id: str,
        array_field: str,
        element
    ) -> bool:
        """
        Removes an element from an array field in a document.
        Returns True if the document was modified.
        """
        collection = self.db[collection_name]
        oid = ObjectId(doc_id) if ObjectId.is_valid(str(doc_id)) else str(doc_id)

        # Only try to pull if the field is a list
        doc = collection.find_one({"_id": oid}, {array_field: 1})
        if not doc or not isinstance(doc.get(array_field), list):
            return False  # No change because it's not an array

        result = collection.update_one({"_id": oid}, {"$pull": {array_field: element}})
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


    def get_by_id(self, collection_name, doc_id):
        """
        Retrieve a single document by its _id.
        Automatically handles both ObjectId and string IDs.
        Returns the document as a dict with _id converted to string, or None if not found.
        """
        collection = self.db[collection_name]
        oid = ObjectId(doc_id) if ObjectId.is_valid(str(doc_id)) else str(doc_id)
        doc = collection.find_one({"_id": oid})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def get_tree_flat(self, collection_name: str, root_id, children_field: str, include_root: bool = True):
        """
        Return a flat (preorder) list of the root + all descendants by walking `children_field`.
        Ignores any parent field.
        """
        if not children_field:
            raise ValueError("children_field is required")

        collection = self.db[collection_name]
        root_key = ObjectId(root_id) if ObjectId.is_valid(str(root_id)) else str(root_id)

        # Ensure root exists
        root = collection.find_one({"_id": root_key})
        if not root:
            return []

        # Pull all descendants via children_field
        pipeline = [
            {"$match": {"_id": root_key}},
            {"$graphLookup": {
                "from": collection_name,
                "startWith": {
                    "$map": {
                        "input": f"${children_field}",
                        "as": "cid",
                        "in": {"$toObjectId": "$$cid"}  # will error if not valid 24-hex
                    }
                },
                "connectFromField": children_field,      # not actually used since startWith is expression
                "connectToField": "_id",
                "as": "descendants"
            }}
        ]      

        agg = list(collection.aggregate(pipeline))
        if not agg:
            return []

        root_doc = agg[0]
        descendants = root_doc.get("descendants", [])
        for d in [root_doc, *descendants]:
            d.pop("descendants", None)

        # Build id map with string ids and in-memory child links from children_field
        id_map = {}
        for d in [root_doc, *descendants]:
            copy = dict(d)
            copy["_id"] = str(copy["_id"])
            copy.setdefault("children", [])
            id_map[copy["_id"]] = copy

        def to_str_id(x):  # normalize ids to string
            return x if isinstance(x, str) else str(x)

        for node in id_map.values():
            for cid in (node.get(children_field) or []):
                child = id_map.get(to_str_id(cid))
                if child:
                    node["children"].append(child)

        # Flatten preorder (optionally skip root)
        root_node = id_map[str(root_doc["_id"])]
        flat = []
        stack = [root_node]
        while stack:
            node = stack.pop()
            if include_root or node is not root_node:
                out = dict(node)
                out.pop("children", None)
                flat.append(out)
            if node.get("children"):
                stack.extend(reversed(node["children"]))
        return flat
