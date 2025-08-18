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
       

        print(f"adds dict: {self.ref_dict}")
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


        ## can only update arrays
        updates = {}
        for target_col in self.add_dict[collection_name]['updates']:
            updates[target_col] = {}
            for target_attr in self.add_dict[collection_name]['updates'][target_col]:
                print(target_attr)
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


        print(f"#####")
        print(f"updates: \n {updates} ")
        print(f"#####")

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
        for col in self.config:
            for field_name in self.config[col]['fields']:
                field = self.config[col]['fields'][field_name]

                if 'link' in field: 
                    if field['origin'] == False:
                        ## init other lib
                        for link in field['link']:
                            
                            target_col = self.substring_until_dot(link)
                            target_attr = self.substring_from_dot(link)
                            name = field['name']
                            
                            
                            self.init_delete_dict(target_col, name, col)

                            self.delete_dict[target_col][col][name]['delete'] = True
                            self.delete_dict[target_col][col][name]['independed'] = field['independed'] 
                            self.delete_dict[target_col][col][name]['type'] = field['type']
                            

                            if 'docId' not in self.config[col]:
                                self.delete_dict[target_col][col][name]['idRef'] = f'{col}{Config.docIdAttrName}' 
                            else:

                                search = [self.config[col]['docId']]

                                link = self.find_my_link(search, target_col)
                                if link == None:
                                    return 'err'

                                self.delete_dict[target_col][col][name]['idRef'] = link 


            if 'docId' in self.config[col]:
                
                paths = [self.config[col]['docId']]

                from_col = ''
                from_attr = ''
                refId = ''
                stop = False

                while stop == False: 
                    for path in paths:
                        ## find the real origin:
                        from_col = self.substring_until_dot(path)
                        from_attr = self.substring_from_dot(path)

                        if from_attr == '':
                            stop = True
                            break
    
                        from_field = self.config[from_col]['fields'][from_attr]

                        if 'refId' in from_field:
                            refId = from_field[refId]

                        elif 'revIdRef' in from_field:
                            refId = from_field['revIdRef']

                        if not 'link' in from_field:
                            stop = True
                            break
                        else:
                            paths = from_field['link']
                            print(paths)


                if not from_attr == '': 

                    self.init_delete_dict(from_col, from_attr, col)
                    self.delete_dict[from_col][col][from_attr] = {}                

                    self.delete_dict[from_col][col][from_attr]['delete'] = True
                    self.delete_dict[from_col][col][from_attr]['independed'] = self.config[col][from_attr]['independed'] 
                    self.delete_dict[from_col][col]['refId'] = refId 

                else:
                    if from_col not in self.delete_dict:
                        self.delete_dict[from_col] = {}

                    if col not in self.delete_dict[from_col]:
                        self.delete_dict[from_col][col] = {}

                    if 'docId' not in self.delete_dict[from_col][col]:
                        self.delete_dict[from_col][col]['docId'] = {} 

                    self.delete_dict[from_col][col]['docId']['delete'] = True 
                    self.delete_dict[from_col][col]['docId']['independed'] = False 
                    
                    self.delete_dict[from_col][col]['docId']['refId'] = refId 

    def gen_delete(self, collection_name, doc_id):
        doc = {}
        updates = {}
        deletes = {}
        all_links = {}

        print(f"#####")
        print(f"delete dict: {self.delete_dict}")
        print(f"#####")

        deletes[collection_name] = {}
        deletes[collection_name][doc_id] = {}
        deletes[collection_name][doc_id]['delete'] = True
        deletes[collection_name]['idRefs'] = []

        if doc == {}:
            doc = self.get_by_id(collection_name, doc_id)
 

        ## to fix need to fix init logic first

        for col in self.delete_dict[collection_name]:
            
            if col not in deletes:
                deletes[col] = {}
                deletes[col]['idRefs'] = []

            for attr_key in self.delete_dict[collection_name][col]:
                
                del_attr = self.delete_dict[collection_name][col][attr_key]
                
                if not isinstance(del_attr, dict):
                    continue

                if del_attr['independed'] == False:
                    target_docIds = []

                    if attr_key == 'docId':
                        if 'docId' not in deletes[col]['idRefs']:
                            target_docIds.append(doc_id)
                            deletes[col]['idRefs'].append('docId')
                        
                    else:
                        refId_key = del_attr['idRef']

                        if refId_key not in deletes[col]['idRefs']:
                            
                            deletes[col]['idRefs'].append(refId_key)

                            temp_ids = doc[refId_key]   

                            if isinstance(temp_ids, str):
                                target_docIds.append(temp_ids)
                            elif isinstance(temp_ids, list):
                                target_docIds = target_docIds + temp_ids

                    for docId in target_docIds:
                        if docId not in deletes[col]:
                            deletes[col][docId] = {}

                        deletes[col][docId]['delete'] = True


        print(deletes)
        ## actual deletes:
        for col in deletes:
            for target_attr in deletes[col]:
                if not isinstance(deletes[col][target_attr], dict):
                    continue

                self.delete(col, target_attr)

        """
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
                            


                            # elif isinstance(updates[linked_coll][target_doc_id][target_field], List):
                            #    pass
                            # we need to remove things from the List
   


                    elif self.config[linked_coll]['fields'][target_field]['independed'] == False:
                        
                        target_doc_ids = doc[idRef]

                        if not linked_coll in deletes:
                            deletes[linked_coll] = {}

                        for target_doc_id in target_doc_ids:
                            deletes[linked_coll][target_doc_id] = True

                    else:    
                        return "exception TODO"   

        for col in self.config:
            if 'docId' in self.config[col]:
                check_col = self.substring_until_dot(self.config[col]['docId'])
                check_attr = self.substring_from_dot(self.config[col]['docId'])

                if not check_attr == 'docId':
                    if 'link' in self.config[check_col]['fields'][check_attr]:
                        if collection_name in self.config[check_col]['fields'][check_attr]['link']:
                            
                            if col not in deletes:
                                deletes[col] = {}

                            for idRef in self.config[check_col]['fields'][check_attr]['revIdRef']:
                                if idRef == 'docId':
                                    deletes[col][doc_id] = True
                                else:
                                    deletes[col][doc[idRef]] = True

        res = []

        for col in updates:
            for target_doc_id in updates[col]:
                res.append(self.modify(col, target_doc_id, updates[col][target_doc_id]))


        ### how to deal with deletes??? user gets deleted -> the houses of user get deleted -> myHouses need to be deleted too 

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
        """

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
