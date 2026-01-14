def create_emit_object(key: str, type: str, description: str) -> str:
    return (
        f'    "{key}": {{\n'
        f'        "type": "{type}",\n'
        f'        "description": "{description}"\n'
        f'    }},'
    )