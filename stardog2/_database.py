import contextlib
import json
from typing import List, Tuple, Union, Iterable

import contextlib2
from pydantic import validate_arguments

from stardog2.content import GraphContent, GraphContentType
from stardog2.database_options import (
    AllDatabaseOptions,
    MutableDatabaseOptions,
    DatabaseOptionEnum,
    OfflineDatabaseOptions,
    OnlineDatabaseOptions,
)
from stardog2.resource import ResourceType, Resource, Database
from stardog2.utils.pydantic import sd_validate_arguments


def custom_validate_arguments(f):
    return sd_validate_arguments(f, globals())


class _Database(Resource):
    """A Database object

    Args:
        db_name: The database to associate the QueryManager with

    Raises:
        StardogException: [404] 0D0DU2: Database '{name}' does not exist.
    """

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        db_name: str,
        options: AllDatabaseOptions = None,
        contents: Union[List[GraphContent], List[Tuple[GraphContent, str]]] = None,
        copy_to_server: bool = False,
        create: bool = False,
        use_cache: bool = False,
        read_only: bool = True,
    ):
        super().__init__(db_name)
        self._resource_type = ResourceType.DATABASE
        self.__use_cache = use_cache
        self.__options: AllDatabaseOptions = options
        self.__read_only = read_only

        if create:
            fmetas = []
            params = []

            if contents is None:
                contents = []

            with contextlib2.ExitStack() as stack:
                for content in contents:
                    context = content[1] if isinstance(content, tuple) else None
                    content = content[0] if isinstance(content, tuple) else content

                    # we will be opening references to many sources in a
                    # single call use a stack manager to make sure they
                    # all get properly closed at the end
                    data = stack.enter_context(content.data())
                    fname = content.name
                    fmeta = {"filename": fname}
                    if context:
                        fmeta["context"] = context

                    fmetas.append(fmeta)
                    params.append(
                        (
                            fname,
                            (
                                fname,
                                data,
                                content.content_type,
                                {"Content-Encoding": content.content_encoding},
                            ),
                        )
                    )

                meta = {
                    "dbname": db_name,
                    "options": options.dict(exclude_none=True, by_alias=True)
                    if options is not None
                    else {},
                    "files": fmetas,
                    "copyToServer": copy_to_server,
                }

                params.append(("root", (None, json.dumps(meta), "application/json")))

                self.client().post("/admin/databases", files=params)

    @property
    def use_cache(self, flag: bool = None):
        if flag is not None:
            self.__use_cache = flag

            if not flag:
                self.__options = None  # invalidate current cache

        return self.__use_cache

    def invalidate_cache(self):
        self.__options = None

    @property
    def __admin_path(self):
        return f"/admin/databases/{self.name}"

    def drop(self):
        """Delete a resource"""
        self.client().delete(self.__admin_path)

    def delete(self):
        self.drop()

    @custom_validate_arguments
    def get_options(self, options: List[Union[DatabaseOptionEnum, str]] = None) -> dict:
        """Get the value of specific metadata options for a database

        Args:
          options  Database option names

        Returns:
          dict: Database options

        Examples
            >>> db = Database("db")
            >>> db.get_options([DatabaseOptionEnum.SEARCH_ENABLED, DatabaseOptionEnum.SPATIAL_ENABLED]) # recommended

            >>> # useful when automating
            >>> # noinspection PyTypeChecker
            >>> db.get_options(['search.enabled', 'spatial.enabled'])
        """

        if options is None:
            if self.__use_cache:
                return self.__options.dict(exclude_none=True, by_alias=True)
            else:
                return self.client().get(self.__admin_path + "/options").json()
        else:
            meta = dict([(x, None) for x in options])
            return self.client().put(self.__admin_path + "/options", json=meta).json()

    @custom_validate_arguments
    def get_option(self, options: Union[DatabaseOptionEnum, str]):
        return self.get_options([options])[options]

    @custom_validate_arguments
    def set_options(self, options: Union[MutableDatabaseOptions, dict]) -> None:
        """Sets database options.

        The database must be offline.

        Args:
          options (dict): Database options

        Raises:
            StardogException: [500] 000012 Cannot change configuration while the database {name} is online
            ValidationError: {number of} validation error for SetOptions

        Examples
            >>> db = Database("db")
            >>> opts = OnlineDatabaseOptions(spatial_result_limit=20000)
            >>> db.set_options(opts)

            >>> opts = OfflineDatabaseOptions(search_enabled=True)
            >>> db.set_options(opts)

            >>> # useful when automating
            >>> db.set_options({"spatial.result.limit": 10000}) # not recommended
        """

        self.client().post(
            self.__admin_path + "/options",
            json=options.dict(exclude_none=True, by_alias=True),
        )

    def namespaces(self):
        pass

    def optimize(self):
        """Optimizes a database."""
        self.client().put(self.__admin_path + "/optimize")

    def verify(self):
        """verifies a database."""
        self.client().post(self.__admin_path + "/verify")

    def repair(self):
        """Attempt to recover a corrupted database.

        The database must be offline.
        """
        r = self.client().post(self.__admin_path + "/repair")
        return r.status_code == 200

    def backup(self, to: str = None):
        """Create a backup of a database on the server.

        Args:
          to (string, optional): specify a path on the server to store
            the backup

        See Also:
          https://www.stardog.com/docs/#_backup_a_database
        """
        params = {"to": to} if to else {}
        self.client().put(self.__admin_path + "/backup", params=params)

    def online(self):
        """Sets a database to online state."""
        self.client().put(self.__admin_path + "/online")

    def offline(self):
        """Sets a database to offline state."""
        self.client().put(self.__admin_path + "/offline")

    def copy(self, to: str):
        """Makes a copy of this database under another name.

        The database must be offline.

        Args:
          to: Name of the new database to be created

        Returns:
          Database: The new Database
        """
        self.client().put(self.__admin_path + "/copy", params={"to": to})
        return Database(to)

    @custom_validate_arguments
    def export(
        self,
        content_type: Union[GraphContentType, str] = GraphContentType.TURTLE,
        stream: bool = False,
        chunk_size: int = 10240,
        graph_uri: str = None,
    ) -> Iterable[str] | str:
        """Exports the contents of the database.

        Args:
          content_type: RDF content type. Defaults to 'text/turtle'
          stream: Chunk results? Defaults to False
          chunk_size: Number of bytes to read per chunk when streaming. Defaults to 10240
          graph_uri: Named graph to export

        Returns:
          str: If stream = False

        Returns:
          gen: If stream = True

        Examples:
          no streaming

          >>> mydb = Database("dbtest")
          >>> contents = mydb.export()

          streaming

          >>> with mydb.export(stream=True) as mystream:
          >>>      contents = ''.join(mystream)
        """

        @contextlib.contextmanager
        def _nextcontext(r):
            yield next(r)

        def _export():
            with self.client().get(
                "/export",
                headers={"Accept": content_type},
                params={"graph-uri": graph_uri},
                stream=stream,
            ) as r:
                yield r.iter_content(chunk_size=chunk_size) if stream else r.content

        db = _export()
        return _nextcontext(db) if stream else next(db)
