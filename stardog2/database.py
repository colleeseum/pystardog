from typing import List, Tuple, Union

from pydantic import validate_arguments

from stardog2 import QueryManager
from stardog2._database import _Database
from stardog2.content import GraphContent
from stardog2.database_options import (
    AllDatabaseOptions,
)
from stardog2.resource import ResourceType, Resource


class Database(_Database, QueryManager):
    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        db_name: str,
        options: AllDatabaseOptions = None,
        contents: Union[List[GraphContent], List[Tuple[GraphContent, str]]] = None,
        copy_to_server: bool = False,
        create: bool = False,
        use_cache: bool = False,
    ):
        super().__init__(
            db_name, options, contents, copy_to_server, create, use_cache, False
        )


class DatabaseManager(Resource):
    def __init__(self, use_cache: bool = False):
        super().__init__("")
        self._resource_type = ResourceType.DATABASE
        self.__use_cache = use_cache

    def list_dict(self, with_options: bool = False) -> dict:
        if with_options:
            return self.client().get("/admin/databases/options").json()["databases"]
        else:
            return self.client().get("/admin/databases").json()["databases"]

    # noinspection PyMethodOverriding
    def exists(self, db_name):
        return db_name in self.list_dict()

    def list(self) -> List[Database]:
        """Retrieves all databases.

        Returns:
          list[Database]: A list of database objects
        """

        dbs = self.list_dict(True)

        dbs_o = []
        for db in dbs:
            valid_keys = AllDatabaseOptions.get_options()

            db_name = db["database.name"]

            for key, value in list(db.items()):
                if key not in valid_keys:
                    del db[key]

            dbs_o.append(Database(db_name, db, use_cache=self.__use_cache))

        return dbs_o

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def create(
        self,
        db_name: str,
        options: AllDatabaseOptions = None,
        contents: Union[List[GraphContent], List[Tuple[GraphContent, str]]] = None,
        copy_to_server: bool = False,
        use_cache: bool = False,
    ) -> Database:
        """Creates a new database."""

        return Database(db_name, options, contents, copy_to_server, True, use_cache)
