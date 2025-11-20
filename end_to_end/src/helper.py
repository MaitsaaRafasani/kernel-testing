import re
import json, yaml
import templates

vars = templates.vars

def hex_to_bytes(hex_str):
    return bytes.fromhex(hex_str)

def bytes_to_str(bytes_msg):
    return bytes.hex(bytes_msg)

def replace_variables(obj):
    """Recursively replace ${var} placeholders using vars dict."""
    if isinstance(obj, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: str(vars.get(m.group(1), m.group(0))), obj)
    elif isinstance(obj, list):
        return [replace_variables(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: replace_variables(v) for k, v in obj.items()}
    return obj

def expand_templates(obj):
    """Recursively expand objects containing $template."""
    if isinstance(obj, dict) and '$template' in obj:
        template_name = obj["$template"]
        args = [replace_variables(a) for a in obj.get("args", [])]
        expected_failed = obj.get("expected_failed", False)

        if not hasattr(templates, template_name):
            raise ValueError(f"Unknown template: {template_name}")

        func = getattr(templates, template_name)
        expanded = func(*args)

        # If the template result is a dict, attach expected_failed
        if isinstance(expanded, dict) and expected_failed:
            expanded["expected_failed"] = expected_failed

        return expanded

    elif isinstance(obj, list):
        return [expand_templates(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: expand_templates(v) for k, v in obj.items()}
    return obj

def load_payload(filename):
    """Load and expand test payloads from JSON or YAML file."""
    with open(filename, encoding="utf-8") as f:
        if filename.endswith((".yaml", ".yml")):
            payload = yaml.safe_load(f)
        else:
            payload = json.load(f)

    payload = replace_variables(payload)
    return expand_templates(payload)

def split_hex(type, msg):
    if type == 'concox':
        end_bit = "0d0a"
        messages = []
        current = ""

        for i in range(0, len(msg), 2):
            current += msg[i:i+2]
            if current.endswith(end_bit):
                messages.append(current)
                current = ""
        return messages
    else:
        return [msg]