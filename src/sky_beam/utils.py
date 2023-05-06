import ast
import json


def try_key(d, key, default=None):
    """Try to get a key from a dictionary, returning a default value if it doesn't exist."""
    if d is None:
        return default

    try:
        return d[key]
    except KeyError:
        return default


def try_key_all(
    d: dict,
    main_key: str = "",
) -> dict:
    """Try to get all keys from a main_key dictionary, returning an empty dictionary if
    it doesn't exist.
    """
    if d is None:
        return {}

    return _extract_keys(d, main_key)


def _extract_keys(
    d: dict,
    prefix_alias: str,
) -> dict:
    """Extract keys from a dictionary."""
    return_dict = {}

    for key in d.keys():
        if prefix_alias == "":
            alias = key
        else:
            alias = f"{prefix_alias}_{key}"

        if isinstance(d[key], dict) or (isinstance(d[key], str) and _is_json(d[key])):
            if isinstance(d[key], str):
                use_key = ast.literal_eval(d[key])
            else:
                use_key = d[key]

            return_dict = {
                **return_dict,
                **_extract_keys(use_key, alias),
            }
        else:
            return_dict[alias] = d[key]

    return return_dict


def _is_json(x: str) -> bool:
    """Check if a string is a valid JSON with ast.literal_eval."""
    try:
        if x[0] != "{":
            return False

        return isinstance(ast.literal_eval(x), dict)
    except (ValueError, SyntaxError, IndexError):
        return False
