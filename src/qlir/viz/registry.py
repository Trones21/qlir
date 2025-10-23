REGISTRY = {}

def register(name: str):
    def deco(fn):
        REGISTRY[name] = fn
        return fn
    return deco


def get(name: str):
    return REGISTRY.get(name)
