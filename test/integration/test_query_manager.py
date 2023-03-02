import json
import re
import pytest
from stardog2.exceptions import StardogException
from stardog2.connector import client
from stardog2.content import (
    SelectContentType,
    AskContentType,
    GraphContentType,
    GraphRaw,
)

from stardog2 import QueryManager
from stardog2.query_manager import Transaction


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
        QueryManager.set_client(client.Client(**cls.get_connection_details()))

    @property
    def db_name(self) -> str:
        return "dbtest"

    # noinspection PyUnusedLocal
    # noinspection PyMethodMayBeStatic
    def teardown_method(self):
        QueryManager.set_idempotent(False)

        qm = QueryManager("catalog")

        # clean dangling tx
        txs = qm.list_tx()
        for tx in txs:
            tx.commit()

        # make sure the namegraph to test update is removed.
        qm.clear("https://example.org/stardog#test")


class TestQueryManager(Test):

    # test getting transaction
    def test_begins(self):
        qm = QueryManager("catalog")
        tx = qm.begin_tx()
        assert True
        tx.commit()

    #############################
    #
    # Test Explain
    #
    #############################
    def test_explain(self):
        qm = QueryManager("catalog")
        plan = qm.explain("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")
        assert re.search(r"Projection", plan)

    def test_explain_with_override(self):
        qm = QueryManager("catalog")
        plan = qm.explain(
            "select * { <Resource> ?p ?o}",
            reasoning=True,
            verbose=True,
            profile=True,
            base_uri="http://www.w3.org/ns/dcat#",
        )
        assert re.search(r"Projection", plan)
        assert re.search(r"^Profiling", plan)

    def test_explain_no_db(self):
        qm = QueryManager("nodb")
        with pytest.raises(
            StardogException,
            match=re.escape("[404] 0D0DU2: Database 'nodb' does not exist."),
        ) as e:
            qm.explain("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")

        assert e.value.http_code == 404
        assert e.value.stardog_code == "0D0DU2"

    def test_explain_syntax_error(self):
        qm = QueryManager("catalog")
        with pytest.raises(
            StardogException,
            match=re.escape(
                "[400] QE0PE2: com.complexible.stardog.plan.eval.ExecutionException:"
            ),
        ) as e:
            qm.explain("select *  <http://www.w3.org/ns/dcat#Resource> ?p ?o}")

        assert e.value.http_code == 400
        assert e.value.stardog_code == "QE0PE2"

    #############################
    #
    # Test select
    #
    #############################

    def test_select(self):
        expect_res = b'{"head":{"vars":["p","o"]},"results":{"bindings":[{"p":{"type":"uri","value":"http://www.w3.org/2000/01/rdf-schema#comment"},"o":{"type":"literal","value":"An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources."}},{"p":{"type":"uri","value":"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"},"o":{"type":"uri","value":"http://www.w3.org/2000/01/rdf-schema#Class"}}]}}'
        qm = QueryManager("catalog")
        res = qm.select("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")

        assert json.loads(expect_res) == res

    def test_select_xml(self):
        expect_res = b"<?xml version='1.0' encoding='UTF-8'?><sparql xmlns='http://www.w3.org/2005/sparql-results#'><head><variable name='p'/><variable name='o'/></head><results><result><binding name='p'><uri>http://www.w3.org/2000/01/rdf-schema#comment</uri></binding><binding name='o'><literal>An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources.</literal></binding></result><result><binding name='p'><uri>http://www.w3.org/1999/02/22-rdf-syntax-ns#type</uri></binding><binding name='o'><uri>http://www.w3.org/2000/01/rdf-schema#Class</uri></binding></result></results></sparql>"
        qm = QueryManager("catalog")
        res = qm.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.SPARQL_XML,
        )

        assert expect_res == res

    def test_select_binary(self):
        expect_res = b"SBQR\x00\x00\x00\x05\x00\x00\x00\x02\x00\x00\x00\x01p\x00\x00\x00\x01o\x02\x00\x00\x00\x00\x00\x00\x00%http://www.w3.org/2000/01/rdf-schema#\x03\x00\x00\x00\x00\x00\x00\x00\x07comment\x02\x00\x00\x00\x01\x00\x00\x00!http://www.w3.org/2001/XMLSchema#\x08\x00\x00\x00\xa7An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources.\x03\x00\x00\x00\x01\x00\x00\x00\x06string\x02\x00\x00\x00\x02\x00\x00\x00+http://www.w3.org/1999/02/22-rdf-syntax-ns#\x03\x00\x00\x00\x02\x00\x00\x00\x04type\x03\x00\x00\x00\x00\x00\x00\x00\x05Class\x7f"
        qm = QueryManager("catalog")
        res = qm.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.BINARY_RDF,
        )

        assert expect_res == res

    def test_select_csv(self):
        expect_res = b'p,o\nhttp://www.w3.org/2000/01/rdf-schema#comment,"An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources."\nhttp://www.w3.org/1999/02/22-rdf-syntax-ns#type,http://www.w3.org/2000/01/rdf-schema#Class\n'
        qm = QueryManager("catalog")
        res = qm.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.CSV,
        )

        assert expect_res == res

    def test_select_tsv(self):
        expect_res = b'?p\t?o\n<http://www.w3.org/2000/01/rdf-schema#comment>\t"An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources."\n<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://www.w3.org/2000/01/rdf-schema#Class>\n'
        qm = QueryManager("catalog")
        res = qm.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.TSV,
        )

        assert expect_res == res

    #############################
    #
    # Test ask
    #
    #############################

    def test_ask(self):
        qm = QueryManager("catalog")
        res = qm.ask(
            "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }"
        )

        assert res

    def test_ask_xml(self):
        expect_res = b"<?xml version='1.0' encoding='UTF-8'?><sparql xmlns='http://www.w3.org/2005/sparql-results#'><head></head><boolean>true</boolean></sparql>"
        qm = QueryManager("catalog")
        res = qm.ask(
            "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
            content_type=AskContentType.SPARQL_XML,
        )

        assert res == expect_res

    def test_ask_json(self):
        expect_res = {"head": {}, "boolean": True}
        qm = QueryManager("catalog")
        res = qm.ask(
            "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
            content_type=AskContentType.SPARQL_JSON,
        )

        assert expect_res == res

    #############################
    #
    # Test graph
    #
    #############################

    def test_graph_describe_turtle_default(self):
        expect_res = b'\n<http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> ;\n   <http://www.w3.org/2000/01/rdf-schema#comment> "Class" ;\n   a <http://www.w3.org/2002/07/owl#DatatypeProperty> ;\n   <http://www.w3.org/2000/01/rdf-schema#label> "class" ;\n   <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .'
        qm = QueryManager("catalog")
        res = qm.graph("DESCRIBE <http://www.w3.org/ns/r2rml#class>")

        assert expect_res == res

    def test_graph_describe_trig(self):
        expect_res = b'\n{\n    <http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> ;\n      <http://www.w3.org/2000/01/rdf-schema#comment> "Class" ;\n      a <http://www.w3.org/2002/07/owl#DatatypeProperty> ;\n      <http://www.w3.org/2000/01/rdf-schema#label> "class" ;\n      <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .\n}\n'
        qm = QueryManager("catalog")
        res = qm.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.TRIG,
        )

        assert expect_res == res

    def test_graph_describe_nquad(self):
        expect_res = b'<http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#comment> "Class" .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#label> "class" .\n<http://www.w3.org/ns/r2rml#class> <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .\n'
        qm = QueryManager("catalog")
        res = qm.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.NQUADS,
        )

        assert expect_res == res

    def test_graph_describe_ntriples(self):
        expect_res = b'<http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#comment> "Class" .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#label> "class" .\n<http://www.w3.org/ns/r2rml#class> <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .\n'
        qm = QueryManager("catalog")
        res = qm.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.NTRIPLES,
        )

        assert expect_res == res

    def test_graph_describe_xml(self):
        expect_res = b'<?xml version="1.0" encoding="UTF-8"?>\n<rdf:RDF\n\txmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">\n\n<rdf:Description rdf:about="http://www.w3.org/ns/r2rml#class">\n\t<rangeIncludes xmlns="https://schema.org/" rdf:resource="http://www.w3.org/2001/XMLSchema#string"/>\n\t<comment xmlns="http://www.w3.org/2000/01/rdf-schema#" rdf:datatype="http://www.w3.org/2001/XMLSchema#string">Class</comment>\n\t<rdf:type rdf:resource="http://www.w3.org/2002/07/owl#DatatypeProperty"/>\n\t<label xmlns="http://www.w3.org/2000/01/rdf-schema#" rdf:datatype="http://www.w3.org/2001/XMLSchema#string">class</label>\n\t<domainIncludes xmlns="https://schema.org/" rdf:resource="http://www.w3.org/ns/r2rml#TermMap"/>\n</rdf:Description>\n\n</rdf:RDF>'
        qm = QueryManager("catalog")
        res = qm.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.RDF_XML,
        )

        assert expect_res == res

    def test_graph_describe_ld_json(self):
        expect_res = {
            "@graph": [
                {
                    "@id": "http://www.w3.org/ns/r2rml#class",
                    "@type": "http://www.w3.org/2002/07/owl#DatatypeProperty",
                    "https://schema.org/rangeIncludes": {
                        "@id": "http://www.w3.org/2001/XMLSchema#string"
                    },
                    "https://schema.org/domainIncludes": {
                        "@id": "http://www.w3.org/ns/r2rml#TermMap"
                    },
                    "http://www.w3.org/2000/01/rdf-schema#comment": {"@value": "Class"},
                    "http://www.w3.org/2000/01/rdf-schema#label": {"@value": "class"},
                }
            ]
        }
        qm = QueryManager("catalog")
        res = qm.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.LD_JSON,
        )

        assert expect_res == res

    #############################
    #
    # Test path
    #
    #############################
    def test_paths_default(self):
        expect_res = {
            "head": {"vars": ["s", "s", "e", "e"]},
            "results": {
                "bindings": [
                    {
                        "s": {
                            "type": "uri",
                            "value": "http://www.w3.org/ns/r2rml#class",
                        },
                        "e": {
                            "type": "uri",
                            "value": "http://www.w3.org/2002/07/owl#DatatypeProperty",
                        },
                    }
                ]
            },
        }
        qm = QueryManager("catalog")
        res = qm.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type"
        )

        assert expect_res == res

    def test_paths_xml(self):
        expect_res = b"<?xml version='1.0' encoding='UTF-8'?><sparql xmlns='http://www.w3.org/2005/sparql-results#'><head><variable name='s'/><variable name='s'/><variable name='e'/><variable name='e'/></head><results><result><binding name='s'><uri>http://www.w3.org/ns/r2rml#class</uri></binding><binding name='e'><uri>http://www.w3.org/2002/07/owl#DatatypeProperty</uri></binding></result></results></sparql>"
        qm = QueryManager("catalog")
        res = qm.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.SPARQL_XML,
        )

        assert expect_res == res

    def test_paths_binary(self):
        expect_res = b"SBQR\x00\x00\x00\x05\x00\x00\x00\x04\x00\x00\x00\x01s\x00\x00\x00\x01s\x00\x00\x00\x01e\x00\x00\x00\x01e\x02\x00\x00\x00\x00\x00\x00\x00\x1bhttp://www.w3.org/ns/r2rml#\x03\x00\x00\x00\x00\x00\x00\x00\x05class\x03\x00\x00\x00\x00\x00\x00\x00\x05class\x02\x00\x00\x00\x01\x00\x00\x00\x1ehttp://www.w3.org/2002/07/owl#\x03\x00\x00\x00\x01\x00\x00\x00\x10DatatypeProperty\x03\x00\x00\x00\x01\x00\x00\x00\x10DatatypeProperty\x7f"
        qm = QueryManager("catalog")
        res = qm.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.BINARY_RDF,
        )

        assert expect_res == res

    def test_paths_csv(self):
        expect_res = b"s,s,e,e\nhttp://www.w3.org/ns/r2rml#class,http://www.w3.org/ns/r2rml#class,http://www.w3.org/2002/07/owl#DatatypeProperty,http://www.w3.org/2002/07/owl#DatatypeProperty\n"
        qm = QueryManager("catalog")
        res = qm.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.CSV,
        )

        assert expect_res == res

    def test_paths_tsv(self):
        expect_res = b"s,s,e,e\nhttp://www.w3.org/ns/r2rml#class,http://www.w3.org/ns/r2rml#class,http://www.w3.org/2002/07/owl#DatatypeProperty,http://www.w3.org/2002/07/owl#DatatypeProperty\n"
        qm = QueryManager("catalog")
        res = qm.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.CSV,
        )

        assert expect_res == res

    def test_update(self):
        qm = QueryManager("catalog")
        qm.update(
            "INSERT DATA { :subj_test :pred_test :obj_test }",
            insert_graph_uri="https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )

        assert res

        qm.update(
            "DELETE DATA { :subj_test :pred_test :obj_test }",
            remove_graph_uri="https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )

        assert not res

    def test_add_remove_data(self):
        qm = QueryManager("catalog")
        qm.add(
            GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
            "https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )

        assert res

        qm.remove(
            GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
            "https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )

        assert not res

    def test_clear(self):
        qm = QueryManager("catalog")
        qm.add(
            GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
            "https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )

        assert res

        qm.clear("https://example.org/stardog#test")
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )

        assert not res

    #############################
    #
    # Test is_consistent, inference and inconsistencies
    #
    #############################
    def test_is_consistent_db(self):
        qm = QueryManager("catalog")
        res = qm.is_consistent()
        assert res

    def test_explain_inference(self):
        expect_res = [
            {
                "status": "INFERRED",
                "expression": "@prefix : <http://api.stardog.com/> .\n@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix owl: <http://www.w3.org/2002/07/owl#> .\n@prefix stardog: <tag:stardog:api:> .\n\n<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap> .",
                "children": [
                    {
                        "status": "ASSERTED",
                        "expression": "@prefix : <http://api.stardog.com/> .\n@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix owl: <http://www.w3.org/2002/07/owl#> .\n@prefix stardog: <tag:stardog:api:> .\n\n<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap> .",
                        "children": [],
                        "namedGraphs": [],
                    }
                ],
                "namedGraphs": [],
            }
        ]
        qm = QueryManager("catalog")
        res = qm.explain_inference(
            GraphRaw(
                "<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap>",
                "data.ttl",
            )
        )

        assert res == expect_res

    def test_explain_inconsistency(self):
        qm = QueryManager("catalog")
        res = qm.explain_inconsistency()

        assert res == []


class TestTransaction(Test):
    def test_list(self):
        qm = QueryManager("catalog")
        res = qm.list_tx()
        assert len(res) == 0

        tx = Transaction("catalog")
        res = tx.list_tx()

        assert len(res) == 1

    #############################
    #
    # Test Explain
    #
    #############################
    def test_explain(self):
        qm = QueryManager("catalog")
        tx = qm.begin_tx()
        plan = tx.explain("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")
        assert re.search(r"Projection", plan)
        tx.commit()

    def test_explain_with_override(self):
        qm = QueryManager("catalog")
        tx = qm.begin_tx()
        plan = tx.explain(
            "select * { <Resource> ?p ?o}",
            reasoning=True,
            verbose=True,
            profile=True,
            base_uri="http://www.w3.org/ns/dcat#",
        )
        assert re.search(r"Projection", plan)
        assert re.search(r"^Profiling", plan)
        tx.commit()

    def test_begin_no_db(self):
        qm = QueryManager("nodb")
        with pytest.raises(
            StardogException,
            match=re.escape("[404] 0D0DU2: Database 'nodb' does not exist."),
        ) as e:
            tx = qm.begin_tx()

        assert e.value.http_code == 404
        assert e.value.stardog_code == "0D0DU2"

    def test_explain_syntax_error(self):
        tx = Transaction("catalog")

        with pytest.raises(
            StardogException,
            match=re.escape(
                "[400] QE0PE2: com.complexible.stardog.plan.eval.ExecutionException:"
            ),
        ) as e:
            tx.explain("select *  <http://www.w3.org/ns/dcat#Resource> ?p ?o}")

        assert e.value.http_code == 400
        assert e.value.stardog_code == "QE0PE2"

    #############################
    #
    # Test select
    #
    #############################

    def test_select(self):
        expect_res = b'{"head":{"vars":["p","o"]},"results":{"bindings":[{"p":{"type":"uri","value":"http://www.w3.org/2000/01/rdf-schema#comment"},"o":{"type":"literal","value":"An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources."}},{"p":{"type":"uri","value":"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"},"o":{"type":"uri","value":"http://www.w3.org/2000/01/rdf-schema#Class"}}]}}'
        tx = Transaction("catalog")
        res = tx.select("select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}")
        tx.commit()

        assert json.loads(expect_res) == res

    def test_select_xml(self):
        expect_res = b"<?xml version='1.0' encoding='UTF-8'?><sparql xmlns='http://www.w3.org/2005/sparql-results#'><head><variable name='p'/><variable name='o'/></head><results><result><binding name='p'><uri>http://www.w3.org/2000/01/rdf-schema#comment</uri></binding><binding name='o'><literal>An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources.</literal></binding></result><result><binding name='p'><uri>http://www.w3.org/1999/02/22-rdf-syntax-ns#type</uri></binding><binding name='o'><uri>http://www.w3.org/2000/01/rdf-schema#Class</uri></binding></result></results></sparql>"
        tx = Transaction("catalog")
        res = tx.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.SPARQL_XML,
        )
        tx.commit()

        assert expect_res == res

    def test_select_binary(self):
        expect_res = b"SBQR\x00\x00\x00\x05\x00\x00\x00\x02\x00\x00\x00\x01p\x00\x00\x00\x01o\x02\x00\x00\x00\x00\x00\x00\x00%http://www.w3.org/2000/01/rdf-schema#\x03\x00\x00\x00\x00\x00\x00\x00\x07comment\x02\x00\x00\x00\x01\x00\x00\x00!http://www.w3.org/2001/XMLSchema#\x08\x00\x00\x00\xa7An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources.\x03\x00\x00\x00\x01\x00\x00\x00\x06string\x02\x00\x00\x00\x02\x00\x00\x00+http://www.w3.org/1999/02/22-rdf-syntax-ns#\x03\x00\x00\x00\x02\x00\x00\x00\x04type\x03\x00\x00\x00\x00\x00\x00\x00\x05Class\x7f"
        tx = Transaction("catalog")
        res = tx.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.BINARY_RDF,
        )
        tx.commit()

        assert expect_res == res

    def test_select_csv(self):
        expect_res = b'p,o\nhttp://www.w3.org/2000/01/rdf-schema#comment,"An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources."\nhttp://www.w3.org/1999/02/22-rdf-syntax-ns#type,http://www.w3.org/2000/01/rdf-schema#Class\n'
        tx = Transaction("catalog")
        res = tx.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.CSV,
        )
        tx.commit()

        assert expect_res == res

    def test_select_tsv(self):
        expect_res = b'?p\t?o\n<http://www.w3.org/2000/01/rdf-schema#comment>\t"An entry in a Data Catalog corresponding to a source of data. Typically an RDBMS, but this can represent other databases, physical data assets, or third party sources."\n<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://www.w3.org/2000/01/rdf-schema#Class>\n'
        tx = Transaction("catalog")
        res = tx.select(
            "select * { <http://www.w3.org/ns/dcat#Resource> ?p ?o}",
            content_type=SelectContentType.TSV,
        )
        tx.commit()

        assert expect_res == res

    #############################
    #
    # Test ask
    #
    #############################

    def test_ask(self):
        tx = Transaction("catalog")
        res = tx.ask(
            "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }"
        )
        tx.commit()

        assert res

    def test_ask_xml(self):
        expect_res = b"<?xml version='1.0' encoding='UTF-8'?><sparql xmlns='http://www.w3.org/2005/sparql-results#'><head></head><boolean>true</boolean></sparql>"
        tx = Transaction("catalog")
        res = tx.ask(
            "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
            content_type=AskContentType.SPARQL_XML,
        )
        tx.commit()

        assert res == expect_res

    def test_ask_json(self):
        expect_res = {"head": {}, "boolean": True}
        tx = Transaction("catalog")
        res = tx.ask(
            "ASK { <http://www.w3.org/ns/r2rml#class> a owl:DatatypeProperty }",
            content_type=AskContentType.SPARQL_JSON,
        )
        tx.commit()

        assert expect_res == res

    #############################
    #
    # Test graph
    #
    #############################

    def test_graph_describe_turtle_default(self):
        expect_res = b'\n<http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> ;\n   <http://www.w3.org/2000/01/rdf-schema#comment> "Class" ;\n   a <http://www.w3.org/2002/07/owl#DatatypeProperty> ;\n   <http://www.w3.org/2000/01/rdf-schema#label> "class" ;\n   <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .'
        tx = Transaction("catalog")
        res = tx.graph("DESCRIBE <http://www.w3.org/ns/r2rml#class>")
        tx.commit()

        assert expect_res == res

    def test_graph_describe_trig(self):
        expect_res = b'\n{\n    <http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> ;\n      <http://www.w3.org/2000/01/rdf-schema#comment> "Class" ;\n      a <http://www.w3.org/2002/07/owl#DatatypeProperty> ;\n      <http://www.w3.org/2000/01/rdf-schema#label> "class" ;\n      <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .\n}\n'
        tx = Transaction("catalog")
        res = tx.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.TRIG,
        )
        tx.commit()

        assert expect_res == res

    def test_graph_describe_nquad(self):
        expect_res = b'<http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#comment> "Class" .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#label> "class" .\n<http://www.w3.org/ns/r2rml#class> <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .\n'
        tx = Transaction("catalog")
        res = tx.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.NQUADS,
        )
        tx.commit()

        assert expect_res == res

    def test_graph_describe_ntriples(self):
        expect_res = b'<http://www.w3.org/ns/r2rml#class> <https://schema.org/rangeIncludes> <http://www.w3.org/2001/XMLSchema#string> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#comment> "Class" .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n<http://www.w3.org/ns/r2rml#class> <http://www.w3.org/2000/01/rdf-schema#label> "class" .\n<http://www.w3.org/ns/r2rml#class> <https://schema.org/domainIncludes> <http://www.w3.org/ns/r2rml#TermMap> .\n'
        tx = Transaction("catalog")
        res = tx.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.NTRIPLES,
        )
        tx.commit()

        assert expect_res == res

    def test_graph_describe_xml(self):
        expect_res = b'<?xml version="1.0" encoding="UTF-8"?>\n<rdf:RDF\n\txmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">\n\n<rdf:Description rdf:about="http://www.w3.org/ns/r2rml#class">\n\t<rangeIncludes xmlns="https://schema.org/" rdf:resource="http://www.w3.org/2001/XMLSchema#string"/>\n\t<comment xmlns="http://www.w3.org/2000/01/rdf-schema#" rdf:datatype="http://www.w3.org/2001/XMLSchema#string">Class</comment>\n\t<rdf:type rdf:resource="http://www.w3.org/2002/07/owl#DatatypeProperty"/>\n\t<label xmlns="http://www.w3.org/2000/01/rdf-schema#" rdf:datatype="http://www.w3.org/2001/XMLSchema#string">class</label>\n\t<domainIncludes xmlns="https://schema.org/" rdf:resource="http://www.w3.org/ns/r2rml#TermMap"/>\n</rdf:Description>\n\n</rdf:RDF>'
        tx = Transaction("catalog")
        res = tx.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.RDF_XML,
        )
        tx.commit()

        assert expect_res == res

    def test_graph_describe_ld_json(self):
        expect_res = {
            "@graph": [
                {
                    "@id": "http://www.w3.org/ns/r2rml#class",
                    "@type": "http://www.w3.org/2002/07/owl#DatatypeProperty",
                    "https://schema.org/rangeIncludes": {
                        "@id": "http://www.w3.org/2001/XMLSchema#string"
                    },
                    "https://schema.org/domainIncludes": {
                        "@id": "http://www.w3.org/ns/r2rml#TermMap"
                    },
                    "http://www.w3.org/2000/01/rdf-schema#comment": {"@value": "Class"},
                    "http://www.w3.org/2000/01/rdf-schema#label": {"@value": "class"},
                }
            ]
        }
        tx = Transaction("catalog")
        res = tx.graph(
            "DESCRIBE <http://www.w3.org/ns/r2rml#class>",
            content_type=GraphContentType.LD_JSON,
        )
        tx.commit()

        assert expect_res == res

    #############################
    #
    # Test path
    #
    #############################
    def test_paths_default(self):
        expect_res = {
            "head": {"vars": ["s", "s", "e", "e"]},
            "results": {
                "bindings": [
                    {
                        "s": {
                            "type": "uri",
                            "value": "http://www.w3.org/ns/r2rml#class",
                        },
                        "e": {
                            "type": "uri",
                            "value": "http://www.w3.org/2002/07/owl#DatatypeProperty",
                        },
                    }
                ]
            },
        }
        tx = Transaction("catalog")
        res = tx.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type"
        )
        tx.commit()

        assert expect_res == res

    def test_paths_xml(self):
        expect_res = b"<?xml version='1.0' encoding='UTF-8'?><sparql xmlns='http://www.w3.org/2005/sparql-results#'><head><variable name='s'/><variable name='s'/><variable name='e'/><variable name='e'/></head><results><result><binding name='s'><uri>http://www.w3.org/ns/r2rml#class</uri></binding><binding name='e'><uri>http://www.w3.org/2002/07/owl#DatatypeProperty</uri></binding></result></results></sparql>"
        tx = Transaction("catalog")
        res = tx.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.SPARQL_XML,
        )
        tx.commit()

        assert expect_res == res

    def test_paths_binary(self):
        expect_res = b"SBQR\x00\x00\x00\x05\x00\x00\x00\x04\x00\x00\x00\x01s\x00\x00\x00\x01s\x00\x00\x00\x01e\x00\x00\x00\x01e\x02\x00\x00\x00\x00\x00\x00\x00\x1bhttp://www.w3.org/ns/r2rml#\x03\x00\x00\x00\x00\x00\x00\x00\x05class\x03\x00\x00\x00\x00\x00\x00\x00\x05class\x02\x00\x00\x00\x01\x00\x00\x00\x1ehttp://www.w3.org/2002/07/owl#\x03\x00\x00\x00\x01\x00\x00\x00\x10DatatypeProperty\x03\x00\x00\x00\x01\x00\x00\x00\x10DatatypeProperty\x7f"
        tx = Transaction("catalog")
        res = tx.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.BINARY_RDF,
        )
        tx.commit()

        assert expect_res == res

    def test_paths_csv(self):
        expect_res = b"s,s,e,e\nhttp://www.w3.org/ns/r2rml#class,http://www.w3.org/ns/r2rml#class,http://www.w3.org/2002/07/owl#DatatypeProperty,http://www.w3.org/2002/07/owl#DatatypeProperty\n"
        tx = Transaction("catalog")
        res = tx.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.CSV,
        )
        tx.commit()

        assert expect_res == res

    def test_paths_tsv(self):
        expect_res = b"s,s,e,e\nhttp://www.w3.org/ns/r2rml#class,http://www.w3.org/ns/r2rml#class,http://www.w3.org/2002/07/owl#DatatypeProperty,http://www.w3.org/2002/07/owl#DatatypeProperty\n"
        tx = Transaction("catalog")
        res = tx.paths(
            "PATHS START ?s = <http://www.w3.org/ns/r2rml#class> END ?e via rdf:type",
            content_type=SelectContentType.CSV,
        )
        tx.commit()

        assert expect_res == res

    def test_update(self):
        qm = QueryManager("catalog")
        tx = Transaction("catalog")
        tx.update(
            "INSERT DATA { :subj_test :pred_test :obj_test }",
            insert_graph_uri="https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert not res

        res = tx.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert res

        tx.commit()
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert res

        qm.update(
            "DELETE DATA { :subj_test :pred_test :obj_test }",
            remove_graph_uri="https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )

        assert not res

    def test_add_remove_data(self):
        qm = QueryManager("catalog")
        tx = Transaction("catalog")
        tx.add(
            GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
            "https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert not res
        res = tx.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert res

        tx.commit()
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert res

        tx = Transaction("catalog")
        tx.remove(
            GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
            "https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert res
        res = tx.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert not res
        tx.commit()

        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert not res

    def test_clear(self):
        qm = QueryManager("catalog")

        qm.add(
            GraphRaw(":subj_test :pred_test :obj_test", "data.ttl"),
            "https://example.org/stardog#test",
        )
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert res

        tx = Transaction("catalog")
        tx.clear("https://example.org/stardog#test")
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert res
        res = tx.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert not res

        tx.commit()
        res = qm.ask(
            "ASK { :subj_test :pred_test :obj_test }",
            default_graph_uri=["https://example.org/stardog#test"],
        )
        assert not res

    #############################
    #
    # Test is_consistent, inference and inconsistencies
    #
    #############################
    def test_explain_inference(self):
        expect_res = [
            {
                "status": "INFERRED",
                "expression": "@prefix : <http://api.stardog.com/> .\n@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix owl: <http://www.w3.org/2002/07/owl#> .\n@prefix stardog: <tag:stardog:api:> .\n\n<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap> .",
                "children": [
                    {
                        "status": "ASSERTED",
                        "expression": "@prefix : <http://api.stardog.com/> .\n@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix owl: <http://www.w3.org/2002/07/owl#> .\n@prefix stardog: <tag:stardog:api:> .\n\n<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap> .",
                        "children": [],
                        "namedGraphs": [],
                    }
                ],
                "namedGraphs": [],
            }
        ]
        tx = Transaction("catalog")

        # there a bug, at the moment we catch the error have give a more user friendly error. Once the bug is resolve
        # the code is expected to work
        try:
            res = tx.explain_inference(
                GraphRaw(
                    "<http://www.w3.org/ns/r2rml#PredicateMap> rdfs:subClassOf <http://www.w3.org/ns/r2rml#TermMap>",
                    "data.ttl",
                )
            )
            tx.commit()

            assert res == expect_res
        except StardogException as e:
            tx.commit()
            assert e.stardog_code == "PY999999"

    def test_explain_inconsistency(self):
        tx = Transaction("catalog")

        # there a bug, at the moment we catch the error have give a more user friendly error. Once the bug is resolve
        # the code is expected to work

        try:
            res = tx.explain_inconsistency()

            assert res == []
        except StardogException as e:
            tx.commit()
            assert e.stardog_code == "PY999999"

    def test_is_consistent_db(self):
        tx = Transaction("catalog")
        res = tx.is_consistent()
        tx.commit()
        assert res

    def test_open_two_transaction(self):
        tx1 = Transaction("catalog")
        tx2 = Transaction("catalog")
        tx1.commit()
        tx2.commit()

        assert tx1.id != tx2.id
