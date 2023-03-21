from typing import Union

from stardog2._database import _Database
from stardog2.resource import Database


class Namespace:
    def __init__(self, prefix, iri):
        self.prefix = prefix
        self.iri = iri


class Namespaces(list):
    def __init__(self, db: Union[str, Database]):
        self.__db = _Database(db) if isinstance(db, str) else db
        super().__init__()
        prefix_index = {}
        iri_index = {}

        def conv(n: str):
            data = n.split("=")
            return Namespace(data[0], data[1])

        self.extend(list(map(conv, self.__db.get_option("database.namespaces"))))

        for idx, ns in enumerate(self):
            prefix_index[ns.prefix] = idx
            iri_index[ns.iri] = idx

        self.__iri_index = iri_index
        self.__prefix_index = prefix_index

    def find_iri(self, iri: str):
        return self.__iri_index[iri]

    def find_prefix(self, prefix: str):
        return self.__prefix_index[prefix]

    def save(self):
        pass
