from datetime import datetime
from types import NoneType 


class ValidationError(Exception):
    pass

class Validator:
    TYPE_MAPPING = {
        "string": str,
        "integer": int,
        "float": float,
        "boolean": bool,
        "array": list,
        "date": str,  # we'll validate ISO 8601 manually
        "dictionairy": dict,
        "json": dict,
        "*": object,
        "none": NoneType
    }

    def __init__(self, collection_config):
        """
        collection_config = {
            "fields": [ {...}, {...} ] OR { "fieldname": {...}, ... }
        }
        """
        raw_fields = collection_config["fields"]

        if isinstance(raw_fields, dict):
            # dict format
            self.fields_dict = raw_fields
            self.fields = [{"name": name, **details} for name, details in raw_fields.items()]
        else:
            # list format
            self.fields = raw_fields
            self.fields_dict = {field["name"]: field for field in raw_fields}

    def validate(self, document: dict):
        errors = []

        doc_keys = set(document.keys())
        expected_keys = set(self.fields_dict.keys())

        # Missing fields
        for key in expected_keys - doc_keys:
            errors.append(f"Missing required field: '{key}'")

        # Extra fields
        for key in doc_keys - expected_keys:
            errors.append(f"Unexpected field: '{key}' is not defined in schema")

        # Type checks
        for name, field in self.fields_dict.items():
            if name not in document:
                continue

            expected_types = field.get("type", [])
            value = document[name]

            if not self._is_valid_type(value, expected_types):
                errors.append(
                    f"Invalid type for field '{name}': expected {expected_types}, got {type(value).__name__}"
                )

        if errors:
            raise ValidationError("\n".join(errors))

        return True

    def _is_valid_type(self, value, expected_types):
        if "notNone" in expected_types and value is None:
            return False

        if "*" in expected_types and (value is not None or "notNone" not in expected_types):
            return True

        for expected in expected_types:
            if expected == "notNone":
                if value is None:
                    return False

            elif expected == "date":
                if isinstance(value, str) and self._is_iso_date(value):
                    return True

            elif expected in self.TYPE_MAPPING:
                if isinstance(value, self.TYPE_MAPPING[expected]):
                    return True

        return False

    def _is_iso_date(self, string):
        try:
            datetime.fromisoformat(string.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False

