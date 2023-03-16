import inspect
from functools import wraps
import re
from typing import List, Union

from pydantic import validate_arguments

union2_p = re.compile(r"^(?:typing.)?Union\[([^,]+),\s*(?:dict|str)\]$")
union3_p = re.compile(r"^(?:typing.)?Union\[([^,]+),\s*([^,]+),\s*(?:dict|str)\]$")
list2_p = re.compile(r"^(?:typing.)?List\[(?:typing.)?Union\[([^,]+),]\s*str\]\]$")


def sd_validate_arguments(f, global_vars):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    def get_object(keylist):
        try:
            return global_vars[keylist[-1]]
        except KeyError:
            node = globals()
            for key in keys:
                node = node[key] if isinstance(node, dict) else getattr(node, key)

            return

    sig = inspect.signature(f)

    wrapper.__signature__ = sig
    for n, v in f.__annotations__.items():
        if n == "return":
            wrapper.__annotations__[n] = v
        else:
            str_v = str(v)
            c = union2_p.match(str_v)

            if c:
                keys = c.group(1).split(".")
                wrapper.__annotations__[n] = get_object(keys)

            else:
                c = list2_p.match(str_v)
                if c:
                    keys = c.group(1).split(".")
                    wrapper.__annotations__[n] = List[get_object(keys)]
                else:
                    c = union3_p.match(str_v)
                    if c:
                        keys1 = c.group(1).split(".")
                        keys2 = c.group(2).split(".")
                        x = Union[get_object(keys1), get_object(keys2)]
                        wrapper.__annotations__[n] = Union[
                            get_object(keys1), get_object(keys2)
                        ]
                    else:
                        wrapper.__annotations__[n] = v

    return validate_arguments(wrapper)
