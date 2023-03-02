from unittest.mock import MagicMock

from stardog2.connector.client import Client
from stardog2.resource import Base


class Test:
    @classmethod
    def get_connection_details(cls):
        return {
            "endpoint": "http://localhost:5820",
            "username": "admin",
            "password": "admin",
        }

    @classmethod
    def setup_class(cls):
        Base.set_client(Client(**cls.get_connection_details()))

    @property
    def db_name(self) -> str:
        return "dbtest"

    @classmethod
    def teardown_method(cls):
        Base.set_idempotent(False)

    @staticmethod
    def check_call(mock_method: MagicMock, url: str, arguments: dict | str = None):

        if mock_method.call_args[0][0] != url:
            return False

        if arguments is not None:
            keys = mock_method.call_args[1].keys()

            if "data" in keys:
                if mock_method.call_args[1]["data"] != arguments:
                    return False
            else:
                if mock_method.call_args[1]["params"] != arguments:
                    return False

        return True
