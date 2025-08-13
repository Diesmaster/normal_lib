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
        "date": str,
        "dictionairy": dict,
        "json": dict,
        "*": object,
        "none": NoneType,
    }

    def __init__(self, collection_config):
        raw_fields = collection_config["fields"]

        if isinstance(raw_fields, dict):
            self.fields_dict = raw_fields
            self.fields = [{"name": name, **details} for name, details in raw_fields.items()]
        else:
            self.fields = raw_fields
            self.fields_dict = {field["name"]: field for field in raw_fields}

    def get_attr(self, doc, name):
        """
        Retrieves nested values using dot notation.
        Returns a single value or list of values (if any segment is a list).
        """
        keys = name.split('.')
        return self._resolve_attr(doc, keys)

    def _resolve_attr(self, current, keys):
        if not keys:
            return current

        key = keys[0]
        rest_keys = keys[1:]

        if isinstance(current, list) and not rest_keys == []:
            results = []
            for item in current:
                try:
                    resolved = self._resolve_attr(item, keys)
                    if isinstance(resolved, list):
                        results.extend(resolved)
                    else:
                        results.append(resolved)
                except KeyError:
                    pass  # optionally keep or skip missing
            return results

        if not isinstance(current, dict):
            raise KeyError(f"Expected dict while resolving '{'.'.join(keys)}', got {type(current).__name__}")

        if key not in current:
            raise KeyError(f"Missing key '{key}' while resolving '{'.'.join(keys)}'")

        return self._resolve_attr(current[key], rest_keys)

    def validate(self, document: dict):
        errors = []

        for name, field in self.fields_dict.items():
            expected_types = field.get("type", [])

            try:
                value = self.get_attr(document, name)
            except KeyError as e:
                errors.append(f"Missing required field '{name}': {e}")
                continue

            is_dict = False
            if isinstance(value, list):
                for element in value:
                    if isinstance(element, dict):
                        is_dict == True


            if is_dict == True:
                for v in value:
                    if not self._is_valid_type(v, expected_types):
                        errors.append(
                            f"Invalid type for field '{name}': expected {expected_types}, got {type(v).__name__}"
                        )
            else:
                if not self._is_valid_type(value, expected_types):
                    errors.append(
                        f"Invalid type for field '{name}': expected {expected_types}, got {type(v).__name__}"
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

