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
        self.add_dict = {}
        self.delete_dict = {}
        self.config_reader = ConfigReader(config_path)
        self.config = self.config_reader.get_config()
        print(f"conv: {self.config}")

        self.interface = DBInterface(db_driver, self.config)
        self._collection_classes = self._init_class_funcs()
        self.create_adds()

        ## if col inits always delete doc
        ## otherwise check
        self.create_deletes()


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
        
        for col in self.config:
            self.add_dict[col] = {}
            self.add_dict[col]['inits'] = {}
            self.add_dict[col]['updates'] = {}
            ## prepare inits
            if 'inits' in self.config[col]:
                for init_col in self.config[col]['inits']:

                    self.add_dict[col]['inits'][init_col] = {}
                   
                    ## get the idRef
                    doc_id_string = self.config[init_col]['docId']
                    ref_coll = self.substring_until_dot(doc_id_string)
                    attr = self.substring_from_dot(doc_id_string)
           
                    if not ref_coll == col:
                        attr = self.find_my_link([doc_id_string], col)      
       
                    self.add_dict[col]['inits'][init_col]['idRef'] = attr 

        ## arr refs refs
        for col in self.config:
            for field_key in self.config[col]['fields']:
                if 'link' in self.config[col]['fields'][field_key]:
                    links = self.config[col]['fields'][field_key]['link']
                    
                    for link in links:
                        linked_col = self.substring_until_dot(link)
                        linked_attr = self.substring_from_dot(link)
                        
                        name = self.config[col]['fields'][field_key]['name']
                        types = self.config[col]['fields'][field_key]['type']

                        ## extends coll link
                        if linked_attr == '' and "array" in types:
                            if col not in self.add_dict[linked_col]['updates']: 
                                self.add_dict[linked_col]['updates'][col] = {}
                            
                            idRef = self.config[col]['fields'][field_key]['revIdRef']

                            if name not in self.add_dict[linked_col]['updates'][col]: 
                                self.add_dict[linked_col]['updates'][col][name] = {}

                            
                            self.add_dict[linked_col]['updates'][col][name]['type'] = types
                            self.add_dict[linked_col]['updates'][col][name]['idRef'] = idRef

                        elif not self.substring_from_dot(name) == '':
                            ## if normal link don't do anything
                            
                            parent_attr = self.substring_until_dot(name)

                            if "array" in self.config[col]['fields'][parent_attr]['type']:
                                if name not in self.add_dict[linked_col]['updates'][col]:
                                    self.add_dict[linked_col]['updates'][col][name] = {}

                                self.add_dict[linked_col]['updates'][col][name]['type'] = types

                                idRef = self.config[col]['fields'][parent_attr]['revIdRef']
                                self.add_dict[linked_col]['updates'][col][name]['idRef'] = idRef
                                self.add_dict[linked_col]['updates'][col][name]['link'] = links 

                        elif 'idRef' in self.config[col]['fields'][field_key] and self.config[col]['fields'][field_key]['origin'] == False:
                                
                                linkedCol = self.substring_until_dot(link)

                                refs = [
                                    ref
                                    for ref in self.config[col]['fields'][field_key]['idRef']
                                    if not ref.endswith(f"{Config.docIdAttrName}")
                                ]

                                target_key =  f"{col}{Config.docIdAttrName}"

                                if linkedCol not in self.add_dict[col]['updates']:
                                    self.add_dict[col]['updates'][linkedCol] = {}
                                
                                if target_key not in self.add_dict[col]['updates'][linkedCol]:

                                    self.add_dict[col]['updates'][linkedCol][target_key] = {}
                                    self.add_dict[col]['updates'][linkedCol][target_key]['type'] = ["array"]
                                    self.add_dict[col]['updates'][linkedCol][target_key]['idRef'] = refs
                                    self.add_dict[col]['updates'][linkedCol][target_key]['link'] = ["docId"] 
                                    

       
                                

                            ## list attr's that need updating
                                



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
                    if self.substring_from_dot(link) == '' and 'array' in field['type']:
                        init_doc[field['name']] = []
                        continue

                    if self.substring_from_dot(link) == '' and 'json' in field['type']:
                        init_doc[field['name']] = {}

                    if self.substring_until_dot(link) == original_col:
                        if self.substring_from_dot(field['name']) == '':
                            attr = self.substring_from_dot(link)
                            init_doc[field['name']] = original_doc[attr] 

                        else:
                            self.init_dict(init_doc, original_doc, link, field['name'])

                    else:
                        if self.substring_from_dot(field['name']) == '':
                            init_doc[field['name']] = None
        return init_doc


    def get_init_doc_id(self, doc, doc_id, attr):
        if attr == 'docId':
            return doc_id
        else:
            return doc[attr] ## later fix to get value for hyrarchical stuff


    def gen_add(self,  collection_name, document, doc_id=None):
       

        print(f"adds dict: {self.add_dict}")

        doc_id = self.add(collection_name, document, doc_id)


        ## inits:
        for init in self.add_dict[collection_name]['inits']:
            doc_id_string = self.config[init]['docId']
            ref_coll = self.substring_until_dot(doc_id_string)
            attr = self.substring_from_dot(doc_id_string)
           
            if not ref_coll == collection_name:
                attr = self.find_my_link([doc_id_string], collection_name)      
       
            init_doc_id = self.get_init_doc_id(document, doc_id, attr)
            doc = self.gen_init_doc(collection_name, init, document)
           
            if (test := self.get_by_id(init, init_doc_id)) == None:
                self.add(init, doc, init_doc_id)


        ## can only update arrays fill with the correct values
        updates = {}
        for target_col in self.add_dict[collection_name]['updates']:
            updates[target_col] = {}
            for target_attr in self.add_dict[collection_name]['updates'][target_col]:
                idRefs = self.add_dict[collection_name]['updates'][target_col][target_attr]["idRef"]
                
                if 'link' in self.add_dict[collection_name]['updates'][target_col][target_attr]:
                    links = self.add_dict[collection_name]['updates'][target_col][target_attr]["link"]

                    for idRef in idRefs:
                         
                        target_doc_id = self.get_init_doc_id(document, doc_id, idRef)
                        
                        if target_doc_id not in updates[target_col]:
                            updates[target_col][target_doc_id] = {} 
                        
                        for link in links:
                            this_attr = self.substring_from_dot(link)
                            if self.substring_from_dot(target_attr) == '':
                                if this_attr == '':
                                    updates[target_col][target_doc_id][target_attr] = doc_id 
                                else:
                                    updates[target_col][target_doc_id][target_attr] = document[this_attr] 
                            else:
                                parent = self.substring_until_dot(target_attr)
                                kid = self.substring_from_dot(target_attr)

                                if parent not in updates[target_col][target_doc_id]:
                                    updates[target_col][target_doc_id][parent] = {}
                                    key = f"{collection_name}DocId"
                                    updates[target_col][target_doc_id][parent][key] = doc_id
                                    

                                updates[target_col][target_doc_id][parent][kid] = document[this_attr]

        unique = True

        for collection_name in updates:
            for target_doc_id in updates[collection_name]:
                update = updates[collection_name][target_doc_id]
                for array_field in update:
                    self.add_element_to_array(collection_name, target_doc_id, array_field, update[array_field], unique)


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

                    if not isinstance(doc[idRef], list):
                        target_doc_ids = [target_doc_ids]

                    for target_doc_id in target_doc_ids:
                        if not target_doc_id in gathered_updates[linked_coll]:            
                            gathered_updates[linked_coll][target_doc_id] = {}

                        gathered_updates[linked_coll][target_doc_id][target_field] = value 


        for col in gathered_updates:
            for target_doc_id in gathered_updates[col]:
                add = False
                for key in gathered_updates[col][target_doc_id].keys():
                    if not self.substring_from_dot(key) == '':
                        if 'array' in self.config[col]['fields'][self.substring_until_dot(key)]['type']:
                            add = True
                            break
                    else:
                        break
                
                if add == True:
                    gathered_updates[col][target_doc_id]['find'] = {f'{collection_name}DocId': doc_id}

                res.append(self.modify(col, target_doc_id, gathered_updates[col][target_doc_id]))

        return res

    def init_delete_dict(self, target_col, target_attr, col):
        if not target_col in self.delete_dict:
            self.delete_dict[target_col] = {}

        if not col in self.delete_dict[target_col]:
            self.delete_dict[target_col][col] = {}

        if not target_attr in self.delete_dict[target_col][col]:
            self.delete_dict[target_col][col][target_attr] = {}



    def find_my_link(self, link: list, this_col):
        
        from_col = ''
        from_attr = ''
        stop = False
        ret_link = None


        while stop == False:
            for el_link in link:
                
                ## find the real origin:
                from_col = self.substring_until_dot(el_link)
                from_attr = self.substring_from_dot(el_link)

                if from_col == this_col:
                    
                    if from_attr == '':
                        ret_link = 'docId'
                    else:
                        ret_link = from_attr


                    stop = True
                    break

                if from_attr == '':
                    stop = True
                    break

                from_field = self.config[from_col]['fields'][from_attr]

                if not 'link' in from_field:
                    stop = True
                    break
                else:
                    link = from_field['link']
                    

        return ret_link




    def create_deletes(self):

        print(self.delete_dict)

        ###
        ## deletes dict should have a delete and an update subfield for every col
        ###


        for col in self.config:


            self.delete_dict[col] = {}
            self.delete_dict[col]['deletes'] = {}
            self.delete_dict[col]['updates'] = {}

            if 'inits' in self.config[col]:
                
                self.delete_dict[col]['deletes'] = {}

                for init in self.config[col]['inits']:
                    self.delete_dict[col]['deletes'][init] = {}
                    
                    search = [self.config[init]['docId']]

                    link = self.find_my_link(search, col)
                    if link == None:
                        return 'err'

                    self.delete_dict[col]['deletes'][init]['idRef'] = [link]
                    self.delete_dict[col]['deletes'][init]['independed'] = False
                    self.delete_dict[col]['deletes'][init]['origin'] = False


            
        for col in self.config:
            for field_name in self.config[col]['fields']:
                field = self.config[col]['fields'][field_name]

                if 'link' in field: 
                    if field['origin'] == False:
                        ## handle independed field case
                        if field['independed'] == False: 
                            ## now handle nested vs unested
                            substring = self.substring_until_dot(field['name'])
                            
                            
                            if not self.substring_from_dot(field['name']) == '':
                                if 'array' in self.config[col]['fields'][substring]['type']:

                                    ## this is nested arr stuff
                                    for link in field['link']:
                                    
                                        target_col = self.substring_until_dot(link)
                                        target_attr = self.substring_from_dot(link)
                                       
                                        if target_attr == '':
                                            target_attr = 'docId'
                                            
                                        
                                        revId = self.config[col]['fields'][substring]['revIdRef']

                                        
                                        if not col in self.delete_dict[target_col]['updates']: 
                                            self.delete_dict[target_col]['updates'][col] = {}
                            
                                        if not substring in self.delete_dict[target_col]['updates'][col]:
                                            self.delete_dict[target_col]['updates'][col][substring] = {}


                                        else:
                                            continue

                                        self.delete_dict[target_col]['updates'][col][substring]['idRef'] = revId 
                                        self.delete_dict[target_col]['updates'][col][substring]['find_in_attr'] = field['idRef']
                                        
                                        self.delete_dict[target_col]['updates'][col][substring]['find_element'] = 'docId'
                                        self.delete_dict[target_col]['updates'][col][substring]['independed'] = False
                                        self.delete_dict[target_col]['updates'][col][substring]['origin'] = False

                                    continue 

                                    
                            
                            for link in field['link']:
                                
                                target_col = self.substring_until_dot(link)
                                target_attr = self.substring_from_dot(link)
                               
                                if target_attr == '':
                                    target_attr = ['docId']

                                if 'revIdRef' in field:
                                    target_attr = field['revIdRef']

                                self.delete_dict[target_col]['deletes'][col] = {}
                                self.delete_dict[target_col]['deletes'][col]['idRef'] = target_attr
                                self.delete_dict[target_col]['deletes'][col]['independed'] = False
                                self.delete_dict[target_col]['deletes'][col]['origin'] = False
                        
                        elif field['independed'] == True:
                           
                            for link in field['link']:
                                if not self.substring_from_dot(link) == '':
                                    if not col in self.delete_dict[target_col]['updates']: 
                                        self.delete_dict[target_col]['updates'][col] = {}
                                    
                                    if field['name'] not in self.delete_dict[target_col]['updates'][col]:
                                        self.delete_dict[target_col]['updates'][col][field['name']] = {}
                                   
                                    idRefKey = f"{col}DocIds"

                                    self.delete_dict[target_col]['updates'][col][field['name']]['independed'] = True
                                    self.delete_dict[target_col]['updates'][col][field['name']]['idRef'] = [idRefKey] 
                                    self.delete_dict[target_col]['updates'][col][field['name']]['origin'] = False

                            
                                    
    def gen_delete(self, collection_name, doc_id):
        doc = {}
        update_field = {}
        update_array = {} 
        deletes = {}
        all_links = {}

        print(f"#####")
        print(f"delete dict: {self.delete_dict}")
        print(f"#####")

        deletes[collection_name] = {}

        deletes[collection_name][doc_id] = True    

        effect = self.delete_dict[collection_name]

        doc = self.get_by_id(collection_name, doc_id)

        print(f"eff: {effect}")

        for delete_col in effect['deletes']:
            print(f"del: {delete_col}")
            
            deletes[delete_col] = {}

            for idRef in effect['deletes'][delete_col]['idRef']:
                target_ids = self.get_init_doc_id(doc, doc_id, idRef)
                
                if not isinstance(target_ids, list):
                    target_ids = [target_ids]

                for target_id in target_ids:
                    deletes[delete_col][target_id] = True



        for update_col in effect['updates']:
            for update_attr in effect['updates'][update_col]:

                if 'find_element' in effect['updates'][update_col][update_attr]:
                    
                    if update_col not in update_array:
                        update_array[update_col] = {}


                    effecting = effect['updates'][update_col][update_attr] 
                    
                    target_doc_ids = []
                    for idRef in effecting['idRef']:
                        
                        target = doc[idRef]

                        if not isinstance(target, list):
                            target = [target]

                        target_doc_ids = target_doc_ids + target

                
                    for target_doc_id in target_doc_ids:
                        
                        if target_doc_id not in update_array[update_col]:
                            update_array[update_col][target_doc_id] = {}
                        
                        if update_attr not in update_array[update_col][target_doc_id]:
                            update_array[update_col][target_doc_id][update_attr] = {}
                        
                         
                        target_value = self.get_init_doc_id(doc, doc_id, effecting['find_element']) 

                        update_array[update_col][target_doc_id][update_attr]['find_element'] = target_value 

                        update_array[update_col][target_doc_id][update_attr]['find_in_attr'] = effecting['find_in_attr']

                else:
                    pass


        print(f"arr updates {update_array}")
        print(f"field updates {update_field}")
        print(f"deletes {deletes}")

        rets = []

        for update_col in update_array:
            for update_doc_id in update_array[update_col]:
                for update_field in update_array[update_col][update_doc_id]:
                    element = update_array[update_col][update_doc_id][update_field]['find_element']
                    element_attr = update_array[update_col][update_doc_id][update_field]['find_in_attr']

                    condition = {}

                    for attr in element_attr:
                        name_attr = self.substring_from_dot(attr)
                        condition[name_attr] = element
                   
                    rets.append(self.remove_element_from_array(update_col, update_doc_id, update_field, condition))

        for delete_col in deletes:
            for delete_doc_id in deletes[delete_col]:
                
                rets.append(self.delete(delete_col, delete_doc_id))

        return rets

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
