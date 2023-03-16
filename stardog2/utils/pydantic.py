import inspect
from functools import wraps
import re

from pydantic import validate_arguments

p = re.compile(r"^(?:typing.)?Union\[([^,]+), str]$")


def sd_validate_arguments(f, global_vars):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    sig = inspect.signature(f)

    wrapper.__signature__ = sig
    for n, v in f.__annotations__.items():
        c = p.match(str(v))

        if c:
            keys = c.group(1).split(".")
            try:
                wrapper.__annotations__[n] = global_vars[keys[-1]]
            except KeyError:
                node = globals()
                for key in keys:
                    node = node[key] if isinstance(node, dict) else getattr(node, key)

                wrapper.__annotations__[n] = node

        else:
            wrapper.__annotations__[n] = v

    return validate_arguments(wrapper)
