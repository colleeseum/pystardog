import re

import pytest
from pydantic import ValidationError

from stardog2 import GraphFile
from stardog2.connector import client
from stardog2.content import SelectContentType
from stardog2.database import DatabaseManager, Database
from stardog2.namespaces import Namespaces
from stardog2.database_options import (
    OnlineDatabaseOptions,
    AllDatabaseOptions,
    DatabaseOptionEnum,
    OfflineDatabaseOptions,
)
from stardog2.exceptions import StardogException


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
        Database.set_client(client.Client(**cls.get_connection_details()))

    @classmethod
    def teardown_method(cls):
        Database.set_idempotent(False)

        db = Database("dbtest")

        # noinspection PyUnusedLocal
        try:
            db.drop()
        except StardogException as e:
            pass


class TestDatabaseManager(Test):
    def test_create_list_exists_drop(self):
        dbm = DatabaseManager()
        dbs = dbm.list()
        assert len(dbs) == 1

        db = dbm.create("dbtest")
        dbs = dbm.list()
        assert len(dbs) == 2
        assert dbm.exists("dbtest")

        db.drop()
        dbs = dbm.list()
        assert len(dbs) == 1

    def test_override(self):
        options = AllDatabaseOptions(auto_schema_reasoning=True)

        dbm = DatabaseManager()
        dbm.create("dbtest", options)

        assert dbm.exists("dbtest")

    def test_bulk_create(self):
        dbm = DatabaseManager()

        db = dbm.create(
            "dbtest",
            None,
            [
                (GraphFile("../data/example.ttl"), "urn:ns1"),
                (GraphFile("../data/starwars.ttl"), "urn:ns2"),
            ],
        )

        res = db.select(
            "SELECT DISTINCT ?g { GRAPH ?g { ?s ?p ?o }}",
            content_type=SelectContentType.SPARQL_JSON,
        )
        # j = options.json()
        assert res["results"]["bindings"] == [
            {"g": {"type": "uri", "value": "urn:ns1"}},
            {"g": {"type": "uri", "value": "urn:ns2"}},
        ]

    def test_get_options(self):
        db = Database("catalog")
        options = db.get_options()

        assert isinstance(options, dict)

    def test_get_options_subset(self):
        db = Database("catalog")
        options = db.get_options(
            [DatabaseOptionEnum.SEARCH_ENABLED, DatabaseOptionEnum.SPATIAL_ENABLED]
        )

        assert isinstance(options, dict)
        keys = options.keys()

        assert "spatial.enabled" in keys
        assert "search.enabled" in keys

    def test_set_options(self):
        db = DatabaseManager().create("dbtest")
        options = db.get_options([DatabaseOptionEnum.SPATIAL_RESULT_LIMIT])
        assert db.get_option(DatabaseOptionEnum.SPATIAL_RESULT_LIMIT) == 10000

        new_options = OnlineDatabaseOptions(spatial_result_limit=10001)
        db.set_options(new_options)
        assert db.get_option(DatabaseOptionEnum.SPATIAL_RESULT_LIMIT) == 10001

        db.set_options({"spatial.result.limit": 10000})
        assert db.get_option("spatial.result.limit")

        with pytest.raises(ValidationError) as e:
            db.set_options({"bad": 10000})

        assert not db.get_option("spatial.enabled")

        new_options = OfflineDatabaseOptions(spatial_enabled=True)
        with pytest.raises(
            StardogException, match=re.escape("Cannot change configuration")
        ) as e:
            db.set_options(new_options)

        assert e.value.stardog_code == "000012"

        db.offline()
        db.set_options(new_options)
        assert db.get_option("spatial.enabled")
        db.online()

    def test_repair(self):
        db = Database("catalog")
        db.repair()  # throws exception unless successul.

    def test_optimize(self):
        db = Database("catalog")
        db.optimize()  # throws exception unless successful

    def test_verify(self):
        db = Database("catalog")
        db.verify()  # throws exception unless successful

    def test_backup(self):
        db = Database("catalog")
        db.backup()  # throws exception unless successful

        db.backup("/tmp/backup")


class TestNamespace(Test):
    def test_manager_from_string(self):
        nss = Namespaces("catalog")
        assert len(nss) == 6

    def test_manager_from_db(self):
        nss = Namespaces(Database("catalog"))
        assert len(nss) == 6
