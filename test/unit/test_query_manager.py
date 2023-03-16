import re
from types import MethodType
from unittest import mock

import pytest

from stardog2 import QueryManager
from stardog2.connector.client import Client
from stardog2.content import (
    SelectContentType,
    AskContentType,
    GraphContentType,
    GraphRaw,
)
from stardog2.exceptions import StardogException
from stardog2.query_manager import Transaction
from test.unit.base import Test


class TestQueryManager(Test):
    def test_name(self):
        db = QueryManager("test")
        assert db.name == "test"
        assert db.db_name == "test"

    def test_begin_tx(self):
        mock_response = mock.Mock()
        mock_response.status_code = 201
        mock_response.text = "9e04a8fb-896a-4db6-8cda-94251cbe1387"
        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("dbtest")
            tx = qm.begin_tx()

            assert tx.id == "9e04a8fb-896a-4db6-8cda-94251cbe1387"
            assert mock_method.called
            assert mock_method.call_args_list[0].args[0] == "/dbtest/transaction/begin"

    def test_begin_with_tx(self):
        mock_response = mock.Mock()
        mock_response.status_code = 201
        mock_response.text = "9e04a8fb-896a-4db6-8cda-94251cbe1387"
        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("dbtest")
            tx = qm.begin_tx("9e04a8fb-896a-4db6-8cda-94251cbe1387")

            assert tx.id == "9e04a8fb-896a-4db6-8cda-94251cbe1387"
            assert mock_method.called
            assert (
                mock_method.call_args_list[0].args[0]
                == "/dbtest/transaction/begin/9e04a8fb-896a-4db6-8cda-94251cbe1387"
            )

    def test_explain_with_default(self):
        text = "# reused\nFrom local\nFrom named local named\nProjection(?p, ?o) [#1]\n`─ Scan[SPOC](<https://example.org/subject>, ?p, ?o) [#1]\n"
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = text

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("dbtest")
            plan = qm.explain("select * { <https://example.org/subject> ?p ?o}")
            assert self.check_call(
                mock_method,
                "/dbtest/explain",
                {
                    "query": "select * { <https://example.org/subject> ?p ?o}",
                    "reasoning": False,
                    "profile": False,
                    "verbose": False,
                    "baseURI": None,
                },
            )
            assert text == plan

    def test_explain_with_override(self):
        text = """Profiling results:
Query executed in 68 ms and returned 0 result(s)
Total used memory: 320K
Pre-execution time: 15 ms (22.1%)
Post-processing time: 0 ms (0.0%)

prefix : <http://api.stardog.com/>
prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix stardog: <tag:stardog:api:>

Distinct [#24], memory: {total=256K (80.0%); max=224K}, results: 0, wall time: 2 ms (2.9%)
`─ Projection(?p, ?o) [#24], results: 0, wall time: 0 ms (0.0%)
   `─ Property(http://www.w3.org/ns/Resource, ?p, ?o), memory: {total=64K (20.0%)}, results: 0, wall time: 50 ms (73.5%)
"""

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = text

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("dbtest")
            plan = qm.explain(
                "select * { <subject> ?p ?o}",
                reasoning=True,
                profile=True,
                verbose=True,
                base_uri="https://example.org/",
            )
            assert self.check_call(
                mock_method,
                "/dbtest/explain",
                {
                    "query": "select * { <subject> ?p ?o}",
                    "reasoning": True,
                    "profile": True,
                    "verbose": True,
                    "baseURI": "https://example.org/",
                },
            )
            assert text == plan

    def test_select_defaults(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return "json"

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = "content"
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.select("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == "json"

    def test_select_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.select(
                "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                content_type=SelectContentType.SPARQL_XML,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=False,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": False,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == b"content"

    def test_select_text_rather_enum(self):
        # noinspection PyUnusedLocal

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.select(
                "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                content_type="application/sparql-results+xml",
            )
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == b"content"

    def test_ask_default(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"true"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.ask(
                "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }"
            )
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res

    def test_ask_override(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return "json"

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = "content"
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.ask(
                "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                content_type=AskContentType.SPARQL_JSON,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=False,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": False,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == "json"

    def test_ask_xml(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.ask(
                "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                content_type=AskContentType.SPARQL_XML,
            )
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == b"content"

    def test_graph_default(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.graph("DESCRIBE <http://www.w3.org/ns/r2rml#class>")
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == b"content"

    def test_graph_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.graph(
                "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
                content_type=GraphContentType.TRIG,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=False,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )

            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": False,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == b"content"

    def test_paths_defaults(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return "json"

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = "content"
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.select(
                "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type"
            )
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == "json"

    def test_paths_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.paths(
                "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
                content_type=SelectContentType.SPARQL_XML,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=False,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )
            assert self.check_call(
                mock_method,
                "/catalog/query",
                {
                    "query": "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": False,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == b"content"

    #############################
    #
    # Test update
    #
    #############################

    def test_update_default(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            qm.update("INSERT { :subj :pred :obj }")
            assert self.check_call(
                mock_method,
                "/catalog/update",
                {
                    "query": "INSERT { :subj :pred :obj }",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

    def test_update_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            qm.update(
                "INSERT { :subj :pred :obj }",
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                use_namespaces=False,
                graph_uri="https://graph-uri.com",
                using_graph_uri="https://using-graph-uri.com",
                default_graph_uri="https://default-uri.com",
                using_named_graph_uri="https://using-named-graph-uri.com",
                named_graph_uri="https://named-graph-uri.com",
                insert_graph_uri="https://insert-graph-uri.com",
                remove_graph_uri="https://remove-graph-uri.com",
            )
            assert self.check_call(
                mock_method,
                "/catalog/update",
                {
                    "query": "INSERT { :subj :pred :obj }",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "useNamespaces": False,
                    "graph-uri": "https://graph-uri.com",
                    "using-graph-uri": "https://using-graph-uri.com",
                    "default-graph-uri": "https://default-uri.com",
                    "using-named-graph-uri": "https://using-named-graph-uri.com",
                    "named-graph-uri": "https://named-graph-uri.com",
                    "insert-graph-uri": "https://insert-graph-uri.com",
                    "remove-graph-uri": "https://remove-graph-uri.com",
                    "reasoning": True,
                },
            )

    #############################
    #
    # Test is_consistent, inference and inconsistencies
    #
    #############################

    def test_is_consistent_db(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "true"

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")
            res = qm.is_consistent()

            assert self.check_call(mock_method, "/catalog/reasoning/consistency")
            assert res

    def test_is_consistent_ng(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "true"

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")
            res = qm.is_consistent("https://example.org/ng")

            assert self.check_call(
                mock_method,
                "/catalog/reasoning/consistency",
                {"graph-uri": "https://example.org/ng"},
            )
            assert res

    def test_explain_inference(self):
        payload = {
            "proofs": [
                {
                    "status": "INFERRED",
                    "expression": "@prefix : \\u003chttp://api.stardog.com/\\u003e .\\n@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .\\n@prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .\\n@prefix xsd: \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .\\n@prefix owl: \\u003chttp://www.w3.org/2002/07/owl#\\u003e .\\n@prefix stardog: \\u003ctag:stardog:api:\\u003e .\\n\\n\\u003chttp://www.w3.org/ns/r2rml#PredicateMap\\u003e rdfs:subClassOf \\u003chttp://www.w3.org/ns/r2rml#TermMap\\u003e .",
                    "children": [
                        {
                            "status": "ASSERTED",
                            "expression": "@prefix : \\u003chttp://api.stardog.com/\\u003e .\\n@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .\\n@prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .\\n@prefix xsd: \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .\\n@prefix owl: \\u003chttp://www.w3.org/2002/07/owl#\\u003e .\\n@prefix stardog: \\u003ctag:stardog:api:\\u003e .\\n\\n\\u003chttp://www.w3.org/ns/r2rml#PredicateMap\\u003e rdfs:subClassOf \\u003chttp://www.w3.org/ns/r2rml#TermMap\\u003e .",
                            "children": [],
                            "namedGraphs": [],
                        }
                    ],
                    "namedGraphs": [],
                }
            ]
        }

        # noinspection PyUnusedLocal
        def json(arg):
            return payload

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")
            res = qm.explain_inference(
                GraphRaw(
                    "<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap>",
                    "data.ttl",
                )
            )

            assert self.check_call(
                mock_method,
                "/catalog/reasoning/explain",
                "<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap>",
            )

            assert res == payload["proofs"]

    def test_explain_inconsistency_db(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return {"proofs": []}

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.explain_inconsistency()

            assert self.check_call(
                mock_method, "/catalog/reasoning/explain/inconsistency"
            )

            assert res == []

    def test_explain_inconsistency_ng(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return {"proofs": []}

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            qm = QueryManager("catalog")

            res = qm.explain_inconsistency("https://example.org/ng")

            assert self.check_call(
                mock_method,
                "/catalog/reasoning/explain/inconsistency",
                {"graph-uri": "https://example.org/ng"},
            )

            assert res == []


class TestTransaction(Test):
    @staticmethod
    def get_tx():
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "tx_id_uuid"

        # noinspection PyUnusedLocal
        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            return Transaction("dbtest")

    def test_new_without_id(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "tx_id_uuid"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = Transaction("dbtest")

            assert self.check_call(mock_method, "/dbtest/transaction/begin")
            assert tx.id == "tx_id_uuid"

    def test_new_with_id(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "tx_id_uuid_given"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            # noinspection PyUnusedLocal
            tx = Transaction("dbtest", "tx_id_uuid_given")

            assert self.check_call(
                mock_method, "/dbtest/transaction/begin/tx_id_uuid_given"
            )

    def test_explain_with_default(self):
        text = "# reused\nFrom local\nFrom named local named\nProjection(?p, ?o) [#1]\n`─ Scan[SPOC](<https://example.org/subject>, ?p, ?o) [#1]\n"
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = text

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()
            plan = tx.explain("select * { <https://example.org/subject> ?p ?o}")
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/explain",
                {
                    "query": "select * { <https://example.org/subject> ?p ?o}",
                    "reasoning": False,
                    "profile": False,
                    "verbose": False,
                    "baseURI": None,
                },
            )
            assert text == plan

    def test_explain_with_override(self):
        text = """Profiling results:
Query executed in 68 ms and returned 0 result(s)
Total used memory: 320K
Pre-execution time: 15 ms (22.1%)
Post-processing time: 0 ms (0.0%)

prefix : <http://api.stardog.com/>
prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix stardog: <tag:stardog:api:>

Distinct [#24], memory: {total=256K (80.0%); max=224K}, results: 0, wall time: 2 ms (2.9%)
`─ Projection(?p, ?o) [#24], results: 0, wall time: 0 ms (0.0%)
   `─ Property(http://www.w3.org/ns/Resource, ?p, ?o), memory: {total=64K (20.0%)}, results: 0, wall time: 50 ms (73.5%)
"""

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = text

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()
            plan = tx.explain(
                "select * { <subject> ?p ?o}",
                reasoning=True,
                profile=True,
                verbose=True,
                base_uri="https://example.org/",
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/explain",
                {
                    "query": "select * { <subject> ?p ?o}",
                    "reasoning": True,
                    "profile": True,
                    "verbose": True,
                    "baseURI": "https://example.org/",
                },
            )
            assert text == plan

    def test_select_defaults(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return "json"

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = "content"
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.select("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == "json"

    def test_select_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.select(
                "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                content_type=SelectContentType.SPARQL_XML,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=True,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": True,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == b"content"

    def test_ask_default(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"true"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.ask(
                "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }"
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res

    def test_ask_override(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return "json"

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = "content"
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.ask(
                "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                content_type=AskContentType.SPARQL_JSON,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=False,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": False,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == "json"

    def test_ask_xml(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.ask(
                "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                content_type=AskContentType.SPARQL_XML,
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == b"content"

    def test_graph_default(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.graph("DESCRIBE <http://www.w3.org/ns/r2rml#class>")
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == b"content"

    def test_graph_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.graph(
                "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
                content_type=GraphContentType.TRIG,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=False,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )

            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": False,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == b"content"

    def test_paths_defaults(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return "json"

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = "content"
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.select(
                "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type"
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

            assert res == "json"

    def test_paths_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b"content"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.paths(
                "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
                content_type=SelectContentType.SPARQL_XML,
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                limit=3000,
                offset=3000,
                use_namespaces=False,
                default_graph_uri=[
                    "https://defaulturi1.com",
                    "https://defaulturi2.com",
                ],
                named_graph_uri=["https://ng1.com", "https://ng2.com"],
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/query",
                {
                    "query": "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "limit": 3000,
                    "offset": 3000,
                    "useNamespaces": False,
                    "default-graph-uri": [
                        "https://defaulturi1.com",
                        "https://defaulturi2.com",
                    ],
                    "named-graph-uri": ["https://ng1.com", "https://ng2.com"],
                    "reasoning": True,
                },
            )

            assert res == b"content"

    #############################
    #
    # Test update
    #
    #############################

    def test_update_default(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            tx.update("INSERT { :subj :pred :obj }")
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/update",
                {
                    "query": "INSERT { :subj :pred :obj }",
                    "reasoning": False,
                    "useNamespaces": True,
                },
            )

    def test_update_override(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            tx.update(
                "INSERT { :subj :pred :obj }",
                bindings={"?test": "test"},
                base_uri="https://baseuri.com",
                reasoning=True,
                schema_name="schemaTest",
                timeout=120,
                use_namespaces=False,
                graph_uri="https://graph-uri.com",
                using_graph_uri="https://using-graph-uri.com",
                default_graph_uri="https://default-uri.com",
                using_named_graph_uri="https://using-named-graph-uri.com",
                named_graph_uri="https://named-graph-uri.com",
                insert_graph_uri="https://insert-graph-uri.com",
                remove_graph_uri="https://remove-graph-uri.com",
            )
            assert self.check_call(
                mock_method,
                "/dbtest/tx_id_uuid/update",
                {
                    "query": "INSERT { :subj :pred :obj }",
                    "$?test": "test",
                    "baseUri": "https://baseuri.com",
                    "schema": "schemaTest",
                    "timeout": 120,
                    "useNamespaces": False,
                    "graph-uri": "https://graph-uri.com",
                    "using-graph-uri": "https://using-graph-uri.com",
                    "default-graph-uri": "https://default-uri.com",
                    "using-named-graph-uri": "https://using-named-graph-uri.com",
                    "named-graph-uri": "https://named-graph-uri.com",
                    "insert-graph-uri": "https://insert-graph-uri.com",
                    "remove-graph-uri": "https://remove-graph-uri.com",
                    "reasoning": True,
                },
            )

    #############################
    #
    # Test is_consistent, inference and inconsistencies
    #
    #############################

    def test_is_consistent_db(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "true"

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()
            res = tx.is_consistent()

            assert self.check_call(mock_method, "/dbtest/reasoning/consistency")
            assert res

    def test_is_consistent_ng(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "true"

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()
            res = tx.is_consistent("https://example.org/ng")

            assert self.check_call(
                mock_method,
                "/dbtest/reasoning/consistency",
                {"graph-uri": "https://example.org/ng"},
            )
            assert res

    # @pytest.mark.skip(reason="action not currently supported in transaction")
    def test_explain_inference(self):
        payload = {
            "proofs": [
                {
                    "status": "INFERRED",
                    "expression": "@prefix : \\u003chttp://api.stardog.com/\\u003e .\\n@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .\\n@prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .\\n@prefix xsd: \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .\\n@prefix owl: \\u003chttp://www.w3.org/2002/07/owl#\\u003e .\\n@prefix stardog: \\u003ctag:stardog:api:\\u003e .\\n\\n\\u003chttp://www.w3.org/ns/r2rml#PredicateMap\\u003e rdfs:subClassOf \\u003chttp://www.w3.org/ns/r2rml#TermMap\\u003e .",
                    "children": [
                        {
                            "status": "ASSERTED",
                            "expression": "@prefix : \\u003chttp://api.stardog.com/\\u003e .\\n@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .\\n@prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .\\n@prefix xsd: \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .\\n@prefix owl: \\u003chttp://www.w3.org/2002/07/owl#\\u003e .\\n@prefix stardog: \\u003ctag:stardog:api:\\u003e .\\n\\n\\u003chttp://www.w3.org/ns/r2rml#PredicateMap\\u003e rdfs:subClassOf \\u003chttp://www.w3.org/ns/r2rml#TermMap\\u003e .",
                            "children": [],
                            "namedGraphs": [],
                        }
                    ],
                    "namedGraphs": [],
                }
            ]
        }

        # noinspection PyUnusedLocal
        def json(arg):
            return payload

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()
            res = tx.explain_inference(
                GraphRaw(
                    "<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap>",
                    "data.ttl",
                )
            )

            assert self.check_call(
                mock_method,
                "/dbtest/reasoning/tx_id_uuid/explain",
                "<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap>",
            )

            assert res == payload["proofs"]

    def test_explain_inconsistency_db(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return {"proofs": []}

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()

            res = tx.explain_inconsistency()

            assert self.check_call(
                mock_method, "/dbtest/reasoning/tx_id_uuid/explain/inconsistency"
            )

            assert res == []

    def test_explain_inconsistency_ng(self):
        # noinspection PyUnusedLocal
        def json(arg):
            return {"proofs": []}

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json = MethodType(json, mock_response)

        with mock.patch.object(
            Client, "get", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()
            res = tx.explain_inconsistency("https://example.org/ng")

            assert self.check_call(
                mock_method,
                "/dbtest/reasoning/tx_id_uuid/explain/inconsistency",
                {"graph-uri": "https://example.org/ng"},
            )

            assert res == []

    def test_commit(self):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.text = "text"

        with mock.patch.object(
            Client, "post", return_value=mock_response
        ) as mock_method:
            tx = self.get_tx()
            tx.commit()

            assert self.check_call(mock_method, "/dbtest/transaction/commit/tx_id_uuid")
            assert tx.closed
            assert tx.reason == "This transaction was already committed"

        # noinspection PyUnusedLocal
        with pytest.raises(
            StardogException,
            match=re.escape("This transaction was already committed"),
        ) as e:
            tx.commit()

        # noinspection PyUnusedLocal
        with pytest.raises(
            StardogException,
            match=re.escape("This transaction was already committed"),
        ) as e:
            tx.rollback()

        # noinspection PyUnusedLocal
        with pytest.raises(
            StardogException,
            match=re.escape("This transaction was already committed"),
        ) as e:
            tx.select("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")

        # noinspection PyUnusedLocal
        with pytest.raises(
            StardogException,
            match=re.escape("This transaction was already committed"),
        ) as e:
            tx.add(
                GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
                "https://example.org/stardog#test",
            )

        # noinspection PyUnusedLocal
        with pytest.raises(
            StardogException,
            match=re.escape("This transaction was already committed"),
        ) as e:
            tx.remove(
                GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
                "https://example.org/stardog#test",
            )

        # noinspection PyUnusedLocal
        with pytest.raises(
            StardogException,
            match=re.escape("This transaction was already committed"),
        ) as e:
            tx.clear("https://example.org/stardog#test")
