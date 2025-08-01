import json
from normal_lib.config_reader import ConfigReader
from normal_lib.validator import Validator, ValidationError
from normal_lib.db_drivers.mongo_driver import MongoDriver
from normal_lib.normalizer import Normalizer

if __name__ == "__main__":
    # Load the config
    reader = ConfigReader("example_configs/easy_config.json")

    print("🔍 Full Config:")
    config = reader.get_config()
    print(json.dumps(config, indent=2))

    # Get fields config for 'users' collection
    users_config = reader.get_fields_for_collection("users")
    validator = Validator({"fields": users_config})

    # Load user data from JSON file
    with open("example_objects/user.json", "r", encoding="utf-8") as f:
        user_data = json.load(f)

    # Validate the user document manually (optional pre-check)
    try:
        validator.validate(user_data)
        print("✅ User document is valid (manual validator)!")
    except ValidationError as e:
        print("❌ Manual Validation failed:")
        print(e)

    print("\n🚀 Initializing Normalizer...")
    db = MongoDriver(db_name="testdb")
    normalizer = Normalizer(db_driver=db, config_path="example_configs/easy_config.json")

    print("\n📦 ClassFuncs per collection:")
    class_map = normalizer.get_classes()

    for name, class_func in class_map.items():
        print(f"  • {name}: {class_func}")

    print("\n➕ Attempting to insert user via ClassFuncs...")

    try:
        users_class = class_map["users"]
        doc_id = users_class.add(user_data)
        print(f"✅ Inserted user with ID: {doc_id}")
    except Exception as e:
        print("❌ Failed to insert user:")
        print(e)

