from __future__ import annotations

import inspect
from enum import Enum
from functools import partial
from typing import ForwardRef, List, Union, TypeVar

import inflection as inflection
from pydantic import validate_arguments
from stardog2.connector.client import Client
from stardog2.utils.smart_enum import SmartEnum

T = TypeVar("T")

Entity = ForwardRef("Entity")
Database = ForwardRef("Database")
StoredQuery = ForwardRef("StoredQuery")

Resources = Union[Entity, Database, StoredQuery]


class mixedmethod(object):
    """This decorator mutates a function defined in a class into a 'mixed' class and instance method.

    Usage:

        class Spam:

            @mixedmethod
            def egg(self, cls, *args, **kwargs):
                if self is None:
                    pass # executed if egg was called as a class method (eg. Spam.egg())
                else:
                    pass # executed if egg was called as an instance method (eg. instance.egg())

    The decorated methods need 2 implicit arguments: self and cls, the former being None when
    there is no instance in the call. This follows the same rule as __get__ methods in python's
    descriptor protocol.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        return partial(self.func, instance, cls)


class Base:
    _client: Client = None

    def __init__(self):
        self.__local_client: Client = None

    @mixedmethod
    def set_client(self, cls, client: Client) -> None:
        """

        Args:
            client: This is the HTTP client to used for future actions.


        When idempotent is True, the API will ignore exception such as
            when creating
                0D0DE2 - Database exists
                SE0RE2 - Role, User exists
                000IA2 - VirtualGraph exists
            when dropping/deleting
                0D0DU2 - Database Unknown
                SERNF2 - Role, User Does not exist

        """

        if self is None:
            cls._client = client
        else:
            self.__local_client = client

    @mixedmethod
    def set_idempotent(self, cls, value: bool):

        if self is None:
            cls._client.idempotent = value
        else:
            if self.__local_client is not None:
                self.__local_client.idempotent: value
            else:
                cls._client.idempotent = value

    @mixedmethod
    def client(self, cls):
        client = None

        if self is None:
            client = cls._client
        else:
            if self.__local_client is not None:
                client = self.__local_client
            else:
                client = cls._client

        if client is None:
            client = Client()

        return client


class Resource(Base):
    def __init__(self, name: str):
        self._name = name
        self._resource_type = None
        super().__init__()

    @property
    def name(self):
        return self._name

    @property
    def resource_type(self):
        return self._resource_type

    @property
    def _path(self):
        short_cls = inflection.underscore(type(self).__name__) + "s"
        return f"/admin/{short_cls}/{self.name}"

    @classmethod
    def _list(cls) -> List[str]:
        short_cls = inflection.underscore(cls.__name__) + "s"
        r = cls.client().get(f"/admin/{short_cls}")
        return r.json()[short_cls]

    def delete(self):
        """Delete a resource"""
        self.client().delete(self._path)

    def exists(self):
        cls = type(self)
        resources = cls._list()
        return self.name in resources

    def add_access(self, entity: Entity, action: GrantType) -> T:
        # noinspection PyUnresolvedReferences
        """
        Give the specified access to the specified entity (Role, User)

        Args:
            entity: The User or Role object to add the access too.
            action: The type of access to give

        Return: resource

        Examples:
            >>> resource.add_access(role,'GRANT' )

            >>> resource.add_access(user,'GRANT' )
        """

        entity.add_permission(action, self.resource_type.value)

        return self


class Request(SmartEnum):
    GET = ("get",)
    POST = "post"
    DELETE = "delete"
    PUT = "put"


class InputType(SmartEnum):
    DELIMITED = "DELIMITED"
    JSON = "JSON"


class Syntax(SmartEnum):
    R2RML = "R2RML"
    SMS2 = "SMS2"


class ResourceType(SmartEnum):
    USER = "user"
    ROLE = "role"
    NAMED_GRAPH = "named-graph"
    VIRTUAL_GRAPH = "virtual-graph"
    DATASOURCE = "data-source"
    DATABASE = "db"
    DATABASE_METADATA = "metadata"
    DBMS_ADMIN = "dbms-admin"
    SERVER_ADMIN = "admin"
    ICV_CONSTRAINT = "icv-constraints"
    SENSITIVE_PROPERTIES = "sensitive-properties"
    STORED_QUERY = "stored-query"
    TRANSACTION = "sub-resource-transaction"
    ALL = "*"


RESOURCE_SHORT_TYPE = {
    ResourceType.USER: "user",
    ResourceType.ROLE: "role",
    ResourceType.VIRTUAL_GRAPH: "virtual_graph",
    ResourceType.DATASOURCE: "datasource",
    ResourceType.DATABASE: "database",
    ResourceType.STORED_QUERY: "stored_query",
}

GRANT_MAP = {
    ResourceType.USER: ["create", "read", "write", "delete", "execute"],
    ResourceType.ROLE: ["create", "read", "delete"],
    ResourceType.NAMED_GRAPH: ["read", "write", "grant", "revoke"],
    ResourceType.VIRTUAL_GRAPH: ["create", "read", "delete", "grant", "revoke"],
    ResourceType.DATASOURCE: ["create", "read", "write", "delete", "grant", "revoke"],
    ResourceType.DATABASE: ["create", "read", "write", "delete", "grant", "revoke"],
    ResourceType.DATABASE_METADATA: ["read", "write", "grant", "revoke"],
    ResourceType.DBMS_ADMIN: ["read", "write", "grant", "revoke", "execute"],
    ResourceType.SERVER_ADMIN: ["grant", "revoke", "execute"],
    ResourceType.ICV_CONSTRAINT: ["read", "write", "grant", "revoke"],
    ResourceType.SENSITIVE_PROPERTIES: ["read"],
    ResourceType.STORED_QUERY: ["create"],
}


class GrantType(Enum):
    CREATE = "create"
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    GRANT = "grant"
    REVOKE = "revoke"
    EXECUTE = "execute"
    ALL = "all"

    @classmethod
    @validate_arguments
    def has_value(cls, value: str, resource_type: ResourceType) -> bool:
        if value == "all":
            return True

        # noinspection all
        m = cls.list(resource_type)

        return value in m

    @classmethod
    @validate_arguments
    def get(cls, value, resource_type: ResourceType):
        for key in cls:
            if key.value == value and value in GRANT_MAP[resource_type.value]:
                return key

        valid = cls.list()

        raise Exception(f"Value {value} is not valid, valid values: {valid} ")

    @classmethod
    @validate_arguments
    def list(cls, resource_type: ResourceType) -> List[str]:
        return GRANT_MAP[resource_type]
