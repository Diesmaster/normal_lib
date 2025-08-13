import json
from bson.objectid import ObjectId
from normal_lib.config_reader import ConfigReader
from normal_lib.validator import Validator, ValidationError
from normal_lib.db_drivers.mongo_driver import MongoDriver
from normal_lib.normalizer import Normalizer

if __name__ == "__main__":
    # Load the config
    reader = ConfigReader("example_configs/my_houses.json")

    print("🔍 Full Config:")
    config = reader.get_config()
    print(json.dumps(config, indent=2))

    # Get fields config for 'users' collection
    users_config = reader.get_fields_for_collection("users")
    validator = Validator({"fields": users_config})

    # Load user data from JSON file
    with open("example_objects/user.json", "r", encoding="utf-8") as f:
        user_data = json.load(f)

    # Load user data from JSON file
    with open("example_objects/house.json", "r", encoding="utf-8") as f:
        house_data = json.load(f)


    # Validate the user document manually (optional pre-check)
    try:
        validator.validate(user_data)
        print("✅ User document is valid (manual validator)!")
    except ValidationError as e:
        print("❌ Manual Validation failed:")
        print(e)

    print("\n🚀 Initializing Normalizer...")
    db = MongoDriver(db_name="testdb")
    normalizer = Normalizer(db_driver=db, config_path="example_configs/my_houses.json")

    print("\n📦 ClassFuncs per collection:")
    class_map = normalizer.get_classes()

    for name, class_func in class_map.items():
        print(f"  • {name}: {class_func}")

    print("\n➕ Attempting to insert user via ClassFuncs...")

    user_id = ""
    house_id = ""

     
    #try:
    users_class = class_map["users"]
    user_id = users_class.add(user_data)
    print(f"✅ Inserted user with ID: {user_id}")
    #except Exception as e:
    #    print("❌ Failed to insert user:")
    #    print(e)


    #try:
    users_class = class_map["houses"]
    house_data['ownedByDocId'] = user_id 
    house_id = normalizer.gen_add("houses", house_data)  
    print(f"✅ Inserted house with ID: {house_id}")
    #except Exception as e:
    #    print("❌ Failed to insert house:")
    #    print(e)

    print("\n📄 Inserted user document:")
    print(normalizer.get("users", {"_id": ObjectId(user_id)}))

    print("\n📄 Inserted house document:")
    print(normalizer.get("houses", {"_id": ObjectId(house_id)}))

    print("\n auto my houses:")
    print(normalizer.get("myHouses", {"_id":ObjectId(user_id)}))

    updates = {"address":"Den Haag, Jl. Raya, 1"}

    normalizer.gen_modify("houses", house_id, updates)

    print("\n📄 Inserted user document:")
    print(normalizer.get("users", {"_id": ObjectId(user_id)}))

    print("\n📄 Inserted house document:")
    print(normalizer.get("houses", {"_id": ObjectId(house_id)}))

    print("\n Inserted myHouses document: ")
    print(normalizer.get("myHouses", {"_id":ObjectId(user_id)}))


    #try:
    normalizer.gen_delete("users", user_id) 
    #except Exception as e:
    #    print(f"err: {e}")

    print("\n📄 deleted user document:")
    print(normalizer.get("users", {"_id": ObjectId(user_id)}))

    print("\n📄 deleted house document:")
    print(normalizer.get("houses", {"_id": ObjectId(house_id)}))


