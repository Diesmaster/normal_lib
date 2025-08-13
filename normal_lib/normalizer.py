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
        split = s.split('.', 1)
        if len(split) > 1:
            return split[1]
        else:
            return ''


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
                if 'link' in self.config[coll]['fields'][attr] and 'idRef' in self.config[coll]['fields'][attr]:
                    for link in self.config[coll]['fields'][attr]['link']:
                        linkedCol = self.substring_until_dot(link)

                        refs = [
                            ref
                            for ref in self.config[coll]['fields'][attr]['idRef']
                            if not ref.endswith(f"{Config.docIdAttrName}")
                        ]

                        if not linkedCol in this_coll_refs and not len(refs) == 0:
                            this_coll_refs[linkedCol] = refs 

            self.ref_dict[coll] = {}
            self.ref_dict[coll]['refs'] = this_coll_refs 
            
            
            if 'inits' not in self.ref_dict[coll]:
                self.ref_dict[coll]['inits'] = []

            if 'inits' in self.config[coll]:
                self.ref_dict[coll]['inits'] = self.ref_dict[coll]['inits'] + self.config[coll]['inits']  
       
            print(f"REF_DICT: {self.ref_dict}")


    ## HOW TO DEAL WITH ARRAYS???

    ## works bc of reference
    def find_path(self, target_dict, path):
        while not self.substring_from_dot(path) == '': 
            walk = self.substring_until_dot(path)
            target_dict = target_dict[walk]
            path = self.substring_from_dot(path)
           
            if isinstance(target_dict, list):
                return target_dict, path

        return target_dict, path

    def init_dict(self, target_dict, original_doc, link, name):
        ret_dict, key = self.find_path(original_doc, self.substring_from_dot(link))
        set_value = ret_dict[key]

        ret_dict, key = self.find_path(target_dict, name)
        if not isinstance(ret_dict, list):
            ret_dict[key] = set_value

    def gen_init_doc(self, original_col, target_col, original_doc):
        init_doc = {}
        for field_key in self.config[target_col]['fields']:
            field = self.config[target_col]['fields'][field_key]
            if 'link' in field:
                for link in field['link']:
                    print(f"Link: {link}")
                    if self.substring_until_dot(link) == original_col:
                        if self.substring_from_dot(field['name']) == '':
                            if self.substring_from_dot(link) == '' and 'array' in field['type']:
                                init_doc[field['name']] = []
                                continue
                            if self.substring_from_dot(link) == '' and 'json' in field['type']:
                                init_doc[field['name']] = {}
                            else:
                                attr = self.substring_from_dot(link)
                                init_doc[field['name']] = original_doc[attr] 
                        else:
                            self.init_dict(init_doc, original_doc, link, field['name'])


                        """
                        print("no sucess? {original_doc}")
                        ## deal with hyrarchical init
                        ret_dict, key = self.find_path(original_doc, self.substring_from_dot(link))
                        set_value = ret_dict[key]

                        print("we get here: {init_doc}")

                        ret_dict, key = self.find_path(init_doc, field['name'])
                        if isinstance(ret_dict, list):
                       
                            print("we get here: {ret_dict}")
                            if len(ret_dict) > 0:
                                if not key in ret_dict[-1]:  
                                    ret_dict[-1][key] = set_value
                            
                                else:
                                    ret_dict.append({})
                                    ret_dict[-1][key] = set_value
                            else:
                                ret_dict.append({})
                                ret_dict[-1][key] = set_value

                        print(f"ret_dict: {ret_dict}")
                        """
                    else:
                        init_doc[self.config[target_col]['fields'][field]['name']] = None
        return init_doc


    def get_init_doc_id(self, doc, doc_id, attr):
        if attr == 'docId':
            return doc_id
        else:
            return doc[attr] ## later fix to get value for hyrarchical stuff


    def gen_add(self,  collection_name, document, doc_id=None):
        
        doc_id = self.add(collection_name, document, doc_id)

        ## add refs
        for ref_key in self.ref_dict[collection_name]['refs']:
            for ref_attr in self.ref_dict[collection_name]['refs'][ref_key]:

                ref_doc_id = document[ref_attr]

                array_field = f"{collection_name}{Config.docIdAttrName}"

                unique = True
                
                if not isinstance(ref_doc_id, list):
                    self.add_element_to_array(ref_key, ref_doc_id, array_field, doc_id, unique)
                else:
                    for el_ref_doc_id in ref_doc_id:
                        res = self.add_element_to_array(ref_key, el_ref_doc_id, array_field, doc_id, unique)

        ## init extensions
        for init in self.ref_dict[collection_name]['inits']:
            doc_id_string = self.config[init]['docId']
            ref_coll = self.substring_until_dot(doc_id_string)
            attr = self.substring_from_dot(doc_id_string)
           
            if ref_coll == collection_name:
                init_doc_id = self.get_init_doc_id(document, doc_id, attr)
                doc = self.gen_init_doc(collection_name, init, document)
                self.add(init, doc, init_doc_id)


        ## fix init refs
        for init in self.ref_dict[collection_name]['inits']:
            elements_to_update = {}

            for field_key in self.config[init]['fields']:
                field = self.config[init]['fields'][field_key]
               
                if 'link' in field:
                    
                    if field['link'] == collection_name and 'array' in field['type']:
                        elements_to_update[field['name']] = {}
                    
                    elif not self.substring_from_dot(field['name']) == '':
                        target_field = self.substring_until_dot(field['name'])

                        if target_field not in elements_to_update:
                            elements_to_update[target_field] = {}

                        attr_name = self.substring_from_dot(field['name'])

                        for link in field['link']:
                            ret_dict, key = self.find_path(document, self.substring_from_dot(link))
                            set_value = ret_dict[key]

                            elements_to_update[target_field][attr_name] = set_value 

            for update in elements_to_update:
                
                path = self.config[init]['docId'] 
                target_doc_id = document[self.substring_from_dot(path)] 
                
                key = f"{collection_name}DocId"

                elements_to_update[update][key] = doc_id

                unique = True
                self.add_element_to_array(init, target_doc_id, update, elements_to_update[update], unique)

        return doc_id       

    def gen_modify(self, collection_name, doc_id, updates):

        res = self.modify(collection_name, doc_id, updates)
       
        res = [res]

        gathered_updates = {}

        doc = {}

        for field in updates:
            if 'link' in self.config[collection_name]['fields'][field] and 'idRef' in self.config[collection_name]['fields'][field]:
                
                if doc == {}:
                    doc = self.get_by_id(collection_name, doc_id) 

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
                    doc = self.get_by_id(collection_name, doc_id)
 
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
                        
                        if 'docId' in self.config[linked_coll]:
                            if collection_name == self.config[linked_coll]['docId']:
                                target_doc_ids = [doc_id]
                            else:
                                ## TODO
                                pass
                        else:
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
                if ref_id in doc:
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

    def get_by_id(self, collection_name, doc_id):
        """
        Retrieve a single document by its _id.
        """
        return self.interface.get_by_id(collection_name, doc_id)

    def get_flat_tree(self, collection_name, root_id, children_field, include_root=True):
        """
        Retrieve a flat list of documents starting from root_id following the children_field.
        """
        return self.interface.get_flat_tree(
            collection_name=collection_name,
            root_id=root_id,
            children_field=children_field,
            include_root=include_root
        )
