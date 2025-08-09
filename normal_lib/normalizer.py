from normal_lib.config_reader import ConfigReader
from normal_lib.db_interface import DBInterface
from normal_lib.class_funcs import ClassFuncs
from normal_lib.config import Config 
from bson.objectid import ObjectId


class Normalizer:
    def __init__(self, db_driver, config_path):
        """
        db_driver: MongoDB instance or similar
        config_path: path to a .json config file
        """
        self.ref_dict = {}
        self.config_reader = ConfigReader(config_path)
        self.config = self.config_reader.get_config()
        print(f"conv: {self.config}")

        self.interface = DBInterface(db_driver, self.config)
        self._collection_classes = self._init_class_funcs()
        self.create_adds()


    def substring_until_dot(self, s: str) -> str:
        return s.split('.', 1)[0]

    def substring_from_dot(self, s: str) -> str:
        return s.split('.', 1)[1]


    def _init_class_funcs(self):
        result = {}
        for col_name in self.config.keys():
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

    
    def create_adds(self):
        for coll in self.config:
            this_coll_refs = {}
            for attr in self.config[coll]['fields']:
                if 'link' in self.config[coll]['fields'][attr]:
                    for link in self.config[coll]['fields'][attr]['link']:
                        linkedCol = self.substring_until_dot(link)

                        refs = [
                            ref
                            for ref in self.config[coll]['fields'][attr]['idRef']
                            if not ref.endswith(f".{Config.docIdAttrName}")
                        ]

                        if not linkedCol in this_coll_refs and not len(refs) == 0:
                            this_coll_refs[linkedCol] = refs 

            self.ref_dict[coll] = {}
            self.ref_dict[coll] = this_coll_refs 


            print(f"final col links: {this_coll_refs}")

       
    def gen_add(self,  collection_name, document, doc_id=None):
        doc_id = self.add(collection_name, document, doc_id)
       
        print("gen add is being called")
        print(f"ref dict: {self.ref_dict}")


        for ref_key in self.ref_dict[collection_name]:
            

            for ref_attr in self.ref_dict[collection_name][ref_key]:

                print("we get here")


                ref_doc_id = document[ref_attr]

                print(f"ref docid: {ref_doc_id}")

                array_field = f"{collection_name}{Config.docIdAttrName}"

                unique = True

                self.add_element_to_array(ref_key, ref_doc_id, array_field, doc_id, unique)
                
        return doc_id       

    def gen_modify(self, collection_name, doc_id, updates):

        res = self.modify(collection_name, doc_id, updates)
       
        res = [res]

        gathered_updates = {}

        doc = {}

        for field in updates:
            if 'link' in self.config[collection_name]['fields'][field] and 'idRef' in self.config[collection_name]['fields'][field]:
                
                if doc == {}:
                    doc = self.get_by_id(collection_name, doc_id)[0]

                for i in range(0, len(self.config[collection_name]['fields'][field]['link'])): 
                    link = self.config[collection_name]['fields'][field]['link'][i]
                    idRef = self.config[collection_name]['fields'][field]['idRef'][i]
                    
                    linked_coll = self.substring_until_dot(link)

                    if not linked_coll in gathered_updates:
                        gathered_updates[linked_coll] = {}

                    target_field = self.substring_from_dot(link)
                    value = updates[field]

                    target_doc_ids = doc[idRef]

                    for target_doc_id in target_doc_ids:
                        if not target_doc_id in gathered_updates[linked_coll]:            
                            gathered_updates[linked_coll][target_doc_id] = {}

                        gathered_updates[linked_coll][target_doc_id][target_field] = value 

        for col in gathered_updates:
            for target_doc_id in gathered_updates[col]:
                res.append(self.modify(col, target_doc_id, gathered_updates[col][target_doc_id]))

        return res

    def gen_delete(self, collection_name, doc_id):
        ## when to remove ref
        ## when to delete doc that refs?

        ## if independed == True remove the data 
        ## if independed == False remove the doc

        doc = {}
        updates = {}
        deletes = {}
        all_links = {}


        for field in self.config[collection_name]['fields']:
            if 'link' in self.config[collection_name]['fields'][field] and 'idRef' in self.config[collection_name]['fields'][field] and 'origin' in self.config[collection_name]['fields'][field]:                
                
                
                if doc == {}:
                    doc = self.get_by_id(collection_name, doc_id)[0]

 
                for i in range(0, len(self.config[collection_name]['fields'][field]['link'])): 
                    link = self.config[collection_name]['fields'][field]['link'][i]
                    idRef = self.config[collection_name]['fields'][field]['idRef'][i]
                         
                    linked_coll = self.substring_until_dot(link)
                    target_field = self.substring_from_dot(link)

                    if not linked_coll in all_links:
                        all_links[linked_coll] = {}

                    if not idRef in all_links[linked_coll]:
                        all_links[linked_coll][idRef] = True


                    if self.config[collection_name]['fields'][field]['origin'] == False:
                        continue

                    if not linked_coll in updates:
                        updates[linked_coll] = {}



                    if self.config[linked_coll]['fields'][target_field]['independed'] == True:
                        
                        if not linked_coll in updates:
                            updates[linked_coll] = {}

                        target_doc_ids = doc[idRef]

                        for target_doc_id in target_doc_ids:
                            if not target_doc_id in updates[linked_coll]:            
                                updates[linked_coll][target_doc_id] = {}

                            #if not isinstance(updates[linked_coll][target_doc_id][target_field], List):
                            updates[linked_coll][target_doc_id][target_field] = None
                            ref_fields = self.config[linked_coll]['fields'][target_field]['idRef']     
                            for ref_field in ref_fields:
                                updates[linked_coll][target_doc_id][ref_field] = None
                            


                            #elif isinstance(updates[linked_coll][target_doc_id][target_field], List):
                            #    pass
                            ## we need to remove things from the List
   


                    elif self.config[linked_coll]['fields'][target_field]['independed'] == False:
                        
                        target_doc_ids = doc[idRef]
                        if not linked_coll in deletes:
                            deletes[linked_coll] = {}


                        for target_doc_id in target_doc_ids:
                            deletes[linked_coll][target_doc_id] = True

                    else:    
                        return "exception TODO"   

        res = []

        for col in updates:
            for target_doc_id in updates[col]:
                res.append(self.modify(col, target_doc_id, updates[col][target_doc_id]))


        for col in deletes:
            for target_doc_id in deletes[col]:
                if deletes[col][target_doc_id] == True:
                    res.append(self.delete(col, target_doc_id))

        auto_key = f"{collection_name}{Config.docIdAttrName}"

        for col in all_links:
            ids = set()

            for ref_id in all_links[col]:
                if isinstance(doc[ref_id], list):
                    for target_doc_id in doc[ref_id]:
                        ids.add(target_doc_id)
                else:
                    ids.add(doc[ref_id])

            for target_doc_id in ids:
                res.append(self.remove_element_from_array(col, target_doc_id, auto_key, doc_id))

        res.append(self.delete(collection_name, doc_id))

        return res


    def add(self, collection_name, document, doc_id=None):
        return self.interface.add(collection_name, document, doc_id)

    def delete(self, collection_name, doc_id):
        return self.interface.delete(collection_name, doc_id)

    def modify(self, collection_name, doc_id, updates):
        return self.interface.modify(collection_name, doc_id, updates)


    def add_element_to_array(self, collection_name, doc_id, array_field, element, unique: bool = False):
        """
        Add an element to an array field in a document.

        If unique=True, no duplicates will be added.
        """
        return self.interface.add_element_to_array(collection_name, doc_id, array_field, element, unique)

    def remove_element_from_array(self, collection_name, doc_id, array_field, element):
        """
        Remove an element from an array field in a document.
        """
        return self.interface.remove_element_from_array(collection_name, doc_id, array_field, element)


    def get_by_id(self, collection_name, doc_id):                           
        return self.get(collection_name, {"_id": ObjectId(doc_id)})

    def get(self, collection_name, query=None):
        return self.interface.get(collection_name, query)

