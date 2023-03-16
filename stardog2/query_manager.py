from __future__ import annotations

from distutils.util import strtobool
from typing import List, ForwardRef, Union

from pydantic import validate_arguments

from stardog2.content import (
    Content,
    SelectContentType,
    AskContentType,
    ContentType,
    GraphContentType,
    GraphContent,
)
from stardog2.exceptions import StardogException
from stardog2.resource import Resource, ResourceType, Database
from stardog2.utils.pydantic import sd_validate_arguments

Transaction = ForwardRef("Transaction")


def custom_validate_arguments(f):
    return sd_validate_arguments(f, globals())


class QueryManager(Resource):
    """A QueryManager object

    Args:
        db_name: The database to associate the QueryManager with

    Raises:
        StardogException: [404] 0D0DU2: Database '{name}' does not exist.
    """

    @validate_arguments()
    def __init__(self, db_name: str):
        super().__init__(db_name)
        self._resource_type = ResourceType.DATABASE
        self.transaction = None

    @property
    def db_name(self) -> str:
        """
        Returns the name of the database the interface is associated too.

        """
        return self.name

    @property
    def _path(self):
        return f"/{self.name}"

    def _query(self, query, method, options: dict, txid: str = None):
        params = {"query": query}

        keys = options.keys()

        for option in [
            ("base_uri", "baseUri"),
            "limit",
            "offset",
            "timeout",
            "reasoning",
            "prettyify",
            ("schema_name", "schema"),
            ("use_namespaces", "useNamespaces"),
            ("graph_uri", "graph-uri"),
            ("using_graph_uri", "using-graph-uri"),
            ("default_graph_uri", "default-graph-uri"),
            ("using_named_graph_uri", "using-named-graph-uri"),
            ("named_graph_uri", "named-graph-uri"),
            ("insert_graph_uri", "insert-graph-uri"),
            ("remove_graph_uri", "remove-graph-uri"),
        ]:
            if isinstance(option, tuple):
                if option[0] in keys and options[option[0]] is not None:
                    params[option[1]] = options[option[0]]
            else:
                if option in keys and options[option] is not None:
                    params[option] = options[option]

        # query bindings
        bindings = options["bindings"] if options["bindings"] is not None else {}
        content_type = options.get("content_type").value

        for k, v in bindings.items():
            params["${}".format(k)] = v

        url = (
            f"/{self.db_name}/{txid}/{method}" if txid else f"/{self.db_name}/{method}"
        )

        r = self.client().post(url, data=params, headers={"Accept": content_type})

        return (
            r.json()
            if content_type == ContentType.SPARQL_JSON.value
            or content_type == ContentType.LD_JSON.value
            else r.content
        )

    def _explain_inference(self, content, tx_id: str = None) -> dict:
        with content.data() as data:
            url = f"/{self.db_name}/reasoning/explain"

            if tx_id is not None:
                url = f"/{self.db_name}/reasoning/{tx_id}/explain"

            r = self.client().post(
                url,
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
            )

            return r.json()["proofs"]

    def _explain_inconsistency(self, graph_uri=None, tx_id: str = None) -> dict:
        url = f"/{self.db_name}/reasoning/explain/inconsistency"

        if tx_id is not None:
            url = f"/{self.db_name}/reasoning/{tx_id}/explain/inconsistency"

        r = self.client().get(url, params={"graph-uri": graph_uri})

        return r.json()["proofs"]

    def delete(self):
        short_name = type(self).__name__

        if short_name == "QueryManager":
            raise Exception("Method delete is not supported for QueryManager")
        else:
            return super().delete()

    def list_tx(self) -> List[Transaction]:
        def mapper(n):
            return Transaction(n["db"], n["id"], new=False)

        r = self.client().get(f"/{self.db_name}/transaction")

        res = list(map(mapper, r.json()["transactions"]))

        return res

    @validate_arguments()
    def begin_tx(
        self, tx_id: str = None, auto_rollback: bool = True, new: bool = True
    ) -> Transaction:
        """Return a transaction object

        Args:
            tx_id: the id of the transaction object, by default one is provided by the server
            auto_rollback: whether to rollback automatically, default True
            new: this is a new transaction, set to false if recycling

        Raises:
            StardogException: [400] 000IA2: Invalid UUID string: '{id}'
            StardogException: [404] 0D0DU2: Database '{name}' does not exist.
            StardogException: [500] 000012: Transaction with this ID already exists

        Returns:
         Transaction:  The transaction object
        """
        return Transaction(self.db_name, tx_id, auto_rollback, new)

    @validate_arguments()
    def explain(
        self,
        query: str,
        reasoning: bool = False,
        profile: bool = False,
        verbose: bool = False,
        base_uri: str = None,
    ):
        """Explains the evaluation of a SPARQL query.

        Args:
          query: SPARQL query to run
          reasoning: if true, reasoning will be enabled
          profile: if true, the profiler will be enabled
          verbose: increase verbosity if true
          base_uri (str, optional): Base URI for the parsing of the query

        Raises:
            ValidationError: if validation of field fails
            StardogException: [404] 0D0DU2: Database '{name}' does not exist.
            StardogException: [400] QE0PE2: com.complexible.stardog.plan.eval.ExecutionException: Encountered

        Returns:
         str: Query explanation
        """
        params = {
            "query": query,
            "reasoning": reasoning,
            "profile": profile,
            "verbose": verbose,
            "baseURI": base_uri,
        }

        r = self.client().post(
            f"{self._path}/explain",
            data=params,
        )

        return r.text

    @custom_validate_arguments
    def select(
        self,
        query,
        bindings: dict = None,
        content_type: Union[SelectContentType, str] = SelectContentType.SPARQL_JSON,
        base_uri: str = None,
        reasoning: bool = False,
        schema_name: str = None,
        timeout: int = None,
        limit: int = None,
        offset: int = None,
        use_namespaces: bool = True,
        default_graph_uri: List[str] = None,
        named_graph_uri: List[str] = None,
    ):
        """Executes a SPARQL select query.

        Args:
          query: SPARQL query
          base_uri: Base URI for the parsing of the query
          schema_name: The name of the schema
          limit: Maximum number of results to return
          offset: Offset into the result set
          timeout: Number of ms after which the query should time out. 0 or less implies no timeout
          reasoning: Enable reasoning for the query
          bindings: Map between query variables and their values
          content_type: Content type for results. Defaults to 'application/sparql-results+json'
          use_namespaces: Rquest query results with namespace substitution/prefix lines
          default_graph_uri: URI(s) to be used as the default graph (equivalent to FROM)
          named_graph_uri: URI(s) to be used as  named graph (equivalent to FROM NAMED)

        Raises:
            ValidationError: if validation of field fails
            StardogException: [404] 0D0DU2: Database '{name}' does not exist.
            StardogException: [400] QE0PE2: com.complexible.stardog.plan.eval.ExecutionException: Encountered

        Returns:
          dict: If content_type='application/sparql-results+json'
          bytes: Other content types

        Examples:
          >>> qm = QueryManager('test')
              qm.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)
          >>> db = Database('test')
              db.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)
          >>> db = Database('test')
              tx = db.begin_tx()
              tx.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)
          bindings

          >>> qm.select('select * {?s ?p ?o}', bindings={'o': '<urn:a>'})
          >>> db.select('select * {?s ?p ?o}', bindings={'o': '<urn:a>'})
        """

        return self._query(query, "query", locals())

    @custom_validate_arguments
    def graph(
        self,
        query,
        bindings: dict = None,
        content_type: Union[GraphContentType, str] = GraphContentType.TURTLE,
        base_uri: str = None,
        reasoning: bool = False,
        prettify: bool = False,
        schema_name: str = None,
        timeout: int = None,
        limit: int = None,
        offset: int = None,
        use_namespaces: bool = True,
        default_graph_uri: List[str] = None,
        named_graph_uri: List[str] = None,
    ) -> bytes | dict:
        """Executes a SPARQL 'CONSTRUCT' OR 'DESCRIBE' query.

        Args:
          query: SPARQL query
          base_uri: Base URI for the parsing of the query
          schema_name: The name of the schema
          limit: Maximum number of results to return
          offset: Offset into the result set
          timeout: Number of ms after which the query should time out. 0 or less implies no timeout
          reasoning: Enable reasoning for the query
          bindings: Map between query variables and their values
          prettify: Will ensure all predicate for a group together
          content_type: Content type for results. Defaults to 'application/sparql-results+json'
          use_namespaces: Rquest query results with namespace substitution/prefix lines
          default_graph_uri: URI(s) to be used as the default graph (equivalent to FROM)
          named_graph_uri: URI(s) to be used as  named graph (equivalent to FROM NAMED)

        Returns:
          dict: #if content_type is JSON_LD
          bytes: Any other content_type.

        Examples:
          >>> qm = QueryManager('test')
              qm.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)
          >>> db = Database('test')
              db.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)
          >>> db = Database('test')
              tx = db.begin_tx()
              tx.select('select * {?s ?p ?o}', offset=100, limit=100, reasoning=True)

          bindings

          >>> qm.select('construct { ?s ?p ?o } where { ?s ?p ?o } ', bindings={'o': '<urn:a>'})
          >>> db.select('construct { ?s ?p ?o } where { ?s ?p ?o }', bindings={'o': '<urn:a>'})
        """

        return self._query(query, "query", locals())

    @custom_validate_arguments
    def paths(
        self,
        query,
        bindings: dict = None,
        content_type: Union[SelectContentType, str] = SelectContentType.SPARQL_JSON,
        base_uri: str = None,
        reasoning: bool = False,
        schema_name: str = None,
        timeout: int = None,
        limit: int = None,
        offset: int = None,
        use_namespaces: bool = True,
        default_graph_uri: List[str] = None,
        named_graph_uri: List[str] = None,
    ):
        """Executes a SPARQL path query.

        Args:
          query: SPARQL query
          base_uri: Base URI for the parsing of the query
          schema_name: The name of the schema
          limit: Maximum number of results to return
          offset: Offset into the result set
          timeout: Number of ms after which the query should time out. 0 or less implies no timeout
          reasoning: Enable reasoning for the query
          bindings: Map between query variables and their values
          content_type: Content type for results. Defaults to 'application/sparql-results+json'
          use_namespaces: Rquest query results with namespace substitution/prefix lines
          default_graph_uri: URI(s) to be used as the default graph (equivalent to FROM)
          named_graph_uri: URI(s) to be used as  named graph (equivalent to FROM NAMED)

        Returns:
          dict: If content_type='application/sparql-results+json'

        Returns:
          str: Other content types

        Examples:
          >>> qm = QueryManager('test')
              qm.paths('paths start ?x = :subj end ?y = :obj via ?p')
          >>> db = Database('test')
              db.paths('paths start ?x = :subj end ?y = :obj via ?p')
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.paths('paths start ?x = :subj end ?y = :obj via ?p')
          >>> db = Database('test')
              tx = db.begin_tx()
              tx.paths('paths start ?x = :subj end ?y = :obj via ?p')

          bindings

          >>> qm.select('paths start ?x = :subj end ?y = :obj via ?p', bindings={'o': '<urn:a>'})
          >>> db.select('paths start ?x = :subj end ?y = :obj via ?p', bindings={'o': '<urn:a>'})
        """

        return self._query(query, "query", locals())

    @custom_validate_arguments
    def ask(
        self,
        query,
        bindings: dict = None,
        content_type: Union[AskContentType, str] = AskContentType.BOOLEAN,
        base_uri: str = None,
        reasoning: bool = False,
        schema_name: str = None,
        timeout: int = None,
        limit: int = None,
        offset: int = None,
        use_namespaces: bool = True,
        default_graph_uri: List[str] = None,
        named_graph_uri: List[str] = None,
    ) -> bool | dict | bytes:
        """Executes a SPARQL ask query.

        Args:
          query: SPARQL ask query
          base_uri: Base URI for the parsing of the query
          schema_name: The name of the schema
          limit: Maximum number of results to return
          offset: Offset into the result set
          timeout: Number of ms after which the query should time out. 0 or less implies no timeout
          reasoning: Enable reasoning for the query
          bindings: Map between query variables and their values
          content_type: Content type for results. Defaults to 'application/sparql-results+json'
          use_namespaces: Rquest query results with namespace substitution/prefix lines
          default_graph_uri: URI(s) to be used as the default graph (equivalent to FROM)
          named_graph_uri: URI(s) to be used as  named graph (equivalent to FROM NAMED)

        Returns:
            return True or False  # if content_type BOOLEAN
            return dict # if content_type SPARQL_JSON
            return bytes # if content_type SPARQL_XML


        Examples:
          >>> qm = QueryManager('test')
              qm.ask('ask {:subj :pred :obj}', reasoning=True)
          >>> db = Database('test')
              db.ask('ask {:subj :pred :obj}', reasoning=True)
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.ask('ask {:subj :pred :obj}', reasoning=True)
          >>> db = Database('test')
              tx = qm.begin_tx()
              tx.ask('ask {:subj :pred :obj}', reasoning=True)
        """

        r = self._query(query, "query", locals())

        if content_type == AskContentType.BOOLEAN:
            return bool(strtobool(r.decode()))
        else:
            return r

    @validate_arguments()
    def update(
        self,
        query,
        bindings: dict = None,
        base_uri: str = None,
        reasoning: bool = False,
        schema_name: str = None,
        timeout: int = None,
        use_namespaces: bool = True,
        graph_uri: str = None,
        using_graph_uri: str = None,
        default_graph_uri: str = None,
        using_named_graph_uri: str = None,
        named_graph_uri: str = None,
        insert_graph_uri: str = None,
        remove_graph_uri: str = None,
    ):
        """Executes a SPARQL update query.

        Args:
          query: SPARQL ask query
          base_uri: Base URI for the parsing of the query
          schema_name: The name of the schema
          timeout: Number of ms after which the query should time out. 0 or less implies no timeout
          reasoning: Enable reasoning for the query
          bindings: Map between query variables and their values
          use_namespaces: Rquest query results with namespace substitution/prefix lines
          graph_uri: Named Graph / Context
          using_graph_uri: URI to be used as the default graph (Equivalent to USING)
          default_graph_uri: URI to be used as the default graph (equivalent to FROM)
          using_named_graph_uri: URI to be used as the default graph (equivalent to USING NAMED)
          named_graph_uri: URI to be used as  named graph (equivalent to FROM NAMED)
          insert_graph_uri: URI of the graph to be inserted into
          remove_graph_uri: URI of the graph to be removed from


        Examples:
          >>> qm = QueryManager('test')
              qm.update('INSERT DATA { :subj :pred :obj }')
          >>> db = Database('test')
              db.update('INSERT DATA { :subj :pred :obj }')
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.update('INSERT DATA { :subj :pred :obj }')
          >>> db = Database('test')
              tx = db.begin_tx()
              tx.update('INSERT DATA { :subj :pred :obj }')
        """

        content_type = ContentType.JSON

        return self._query(query, "update", locals())

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def add(self, content: GraphContent, graph_uri: str = None):
        """Adds data to the database.

        Args:
          content (Content): Data to add
          graph_uri (str, optional): Named graph into which to add the data

        Examples:
          >>> db = Database('test')
              db.add(File('example.ttl'), graph_uri='urn:graph')
          >>> conn = QueryManager('test')
              qm.add(File('example.ttl'), graph_uri='urn:graph')
        """

        tx = self.begin_tx()
        tx.add(content, graph_uri)
        tx.commit()

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def remove(self, content: GraphContent, graph_uri: str = None):
        """Removes data from the database.

        Args:
          content (Content): Data to remove
          graph_uri (str, optional): Named graph into which to remove the data

        Examples:
          >>> db = Database('test')
              db.remove(File('example.ttl'), graph_uri='urn:graph')
          >>> conn = QueryManager('test')
              qm.remove(File('example.ttl'), graph_uri='urn:graph')
        """

        tx = self.begin_tx()
        tx.remove(content, graph_uri)
        tx.commit()

    @validate_arguments()
    def clear(self, graph_uri: str = None):
        """Clear all data from the database or namegraph.

        Args:
          graph_uri (str, optional): Named graph into which to remove the data

        Examples:
          >>> db = Database('test')
              db.clear()
          >>> conn = QueryManager('test')
              qm.remove(graph_uri='urn:graph')
        """

        tx = self.begin_tx()
        tx.clear(graph_uri)
        tx.commit()

    @validate_arguments()
    def is_consistent(self, graph_uri: str = None) -> bool:
        """Checks if the database or named graph is consistent wrt its schema.

        Args:
          graph_uri: Named graph from which to check consistency

        Returns:
          bool: Database consistency state

        Examples:
          >>> qm = QueryManager('test')
              qm.is_consistent()
          >>> db = Database('test')
              db.is_consistent()
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.is_consistent()
          >>> db = Database('test')
              tx = qm.begin_tx()
              tx.is_consistent()
        """
        r = self.client().get(
            f"/{self.db_name}/reasoning/consistency", params={"graph-uri": graph_uri}
        )

        return bool(strtobool(r.text))

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def explain_inference(self, content: Content) -> dict:
        """Explains the given inference results.

        Args:
          content (Content): Data from which to provide explanations

        Returns:
          dict: Explanation results

        Examples:
          >>> qm = QueryManager('test')
              qm.explain_inference()
          >>> db = Database('test')
              db.explain_inference()
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.explain_inference()
          >>> db = Database('test')
              tx = qm.begin_tx()
              tx.explain_inference()
        """

        return self._explain_inference(content)

    @validate_arguments()
    def explain_inconsistency(self, graph_uri: str = None) -> dict:
        """Explains why the database or a named graph is inconsistent.

        Args:
          graph_uri (optional): Named graph for which to explain inconsistency:

        Returns:
          dict: Explanation results
        """

        return self._explain_inconsistency(graph_uri)


# noinspection PyRedeclaration
class Transaction(QueryManager):
    """A transaction object

    Args:
        db_name: The database to associate the QueryManager with
        tx_id: the id of the transaction object, by default one is provided by the server
        auto_rollback: whether to rollback automatically, default True
        new: this is a new transaction, set to false if recycling

    Raises:
        StardogException: [400] 000IA2: Invalid UUID string: '{id}'
        StardogException: [404] 0D0DU2: Database '{name}' does not exist.
        StardogException: [500] 000012: Transaction with this ID already exists

    Returns:
     Transaction:  The transaction object
    """

    @validate_arguments()
    def __init__(
        self,
        db_name: str,
        tx_id: str = None,
        auto_rollback: bool = True,
        new: bool = True,
    ):
        url = f"/{db_name}/transaction/begin"

        if new:
            if tx_id is not None:
                url = url + f"/{tx_id}"

            r = Resource.client().post(url)
            self.__id = r.text
        else:
            self.__id = tx_id

        self.__auto_rollback = auto_rollback
        self.__closed = False
        self.__reason = None

        super().__init__(db_name)
        self._resource_type = ResourceType.TRANSACTION

    @property
    def id(self):
        """Returns the transaction's id"""
        return self.__id

    @property
    def closed(self):
        """Returns whether transaction is closed"""
        return self.__closed

    @property
    def reason(self):
        """Returns the reason is closed"""
        return self.__reason

    @property
    def _path(self):
        return f"/{self.db_name}/{self.__id}"

    # noinspection PyMethodOverriding
    def _query(self, query, method, options: dict):

        if self.__closed:
            raise StardogException(
                self.__reason, http_code=000, stardog_code="PY0000001"
            )

        try:
            return super()._query(query, method, options, self.id)
        except StardogException as e:
            if self.__auto_rollback:
                self.rollback()
            raise e

    def rollback(self):
        """Rolls back the current transaction.

        Raises:
            stardog.exceptions.TransactionException
              If currently not in a transaction

        """
        if self.__closed:
            raise StardogException(
                self.__reason, http_code=000, stardog_code="PY0000001"
            )

        self.client().post(f"/{self.db_name}/transaction/rollback/{self.__id}")

        self.__closed = True
        self.__reason = "This transaction was already rollbacked"

    def commit(self):
        """Commits the current transaction.

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction
        """
        if self.__closed:
            raise StardogException(
                self.__reason, http_code=000, stardog_code="PY0000001"
            )

        try:
            self.client().post(f"/{self.db_name}/transaction/commit/{self.__id}")

            self.__closed = True
            self.__reason = "This transaction was already committed"
        except StardogException as e:
            if self.__auto_rollback:
                self.rollback()
                self.__reason = f"This transaction was auto-rollbacked: {e}"
            raise e

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def add(self, content: Content, graph_uri: str = None):
        """Adds data to the database.

        Args:
          content (Content): Data to add
          graph_uri (str, optional): Named graph into which to add the data

        Raises:
          stardog.exceptions.TransactionException
            If not currently in a transaction

        Examples:
          >>> db = Database('test')
              tx = db.begin()
              tx.add(File('example.ttl'), graph_uri='urn:graph')
              tx.commit
          >>> conn = QueryManager('test')
              tx = conn.begin()
              tx.add(File('example.ttl'), graph_uri='urn:graph')
              tx.commit
        """
        if self.__closed:
            raise StardogException(
                self.__reason, http_code=000, stardog_code="PY0000001"
            )

        try:
            with content.data() as data:
                self.client().post(
                    f"{self._path}/add",
                    params={"graph-uri": graph_uri},
                    headers={
                        "Content-Type": content.content_type,
                        "Content-Encoding": content.content_encoding,
                    },
                    data=data,
                )
        except Exception as e:
            if self.__auto_rollback:
                self.rollback()
            self.__reason = f"This transaction was auto-rollbacked: {e}"
            raise e

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def remove(self, content, graph_uri: str = None):
        """Removes data from the database.

        Args:
          content (Content): Data to add
          graph_uri (str, optional): Named graph from which to remove the data

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction

        Examples:
          >>> db = Database('test')
              tx = db.begin()
              tx.remove(File('example.ttl'), graph_uri='urn:graph')
              tx.commit
          >>> conn = QueryManager('test')
              tx = conn.begin()
              tx.remove(File('example.ttl'), graph_uri='urn:graph')
              tx.commit
        """

        if self.__closed:
            raise StardogException(
                self.__reason, http_code=000, stardog_code="PY0000001"
            )

        try:
            with content.data() as data:
                self.client().post(
                    f"{self._path}/remove",
                    params={"graph-uri": graph_uri},
                    headers={
                        "Content-Type": content.content_type,
                        "Content-Encoding": content.content_encoding,
                    },
                    data=data,
                )
        except StardogException as e:
            if self.__auto_rollback:
                self.rollback()
                self.__reason = f"This transaction was auto-rollbacked: {e}"
            raise e

    @validate_arguments()
    def clear(self, graph_uri: str = None):
        """Removes all data from the database or specific named graph.

        Args:
          graph_uri (str, optional): Named graph from which to remove data

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction

        Examples:
          clear a specific named graph
          >>> db = Database('test')
              tx = db.begin()
              tx.clear('urn:graph')
              tx.commit()
          >>> conn = QueryManager('test')
              tx = conn.begin()
              tx.clear('urn:graph')
              tx.commit()

          clear the whole database
          >>> db = Database('test')
              tx = db.begin()
              tx.clear()
              tx.commit()
          >>> conn = QueryManager('test')
              tx = conn.begin()
              tx.clear()
              tx.commit()
        """
        if self.__closed:
            raise StardogException(
                self.__reason, http_code=000, stardog_code="PY0000001"
            )

        try:
            self.client().post(f"{self._path}/clear", params={"graph-uri": graph_uri})
        except StardogException as e:
            if self.__auto_rollback:
                self.rollback()
                self.__reason = f"This transaction was auto-rollbacked: {e}"
            raise e

    def explain_inference(self, content):
        """Explains the given inference results.

        Args:
          content (Content): Data from which to provide explanations

        Returns:
          dict: Explanation results

        Examples:
          >>> qm = QueryManager('test')
              tx = qm.begin_tx()
              tx.explain_inference(File('inferences.ttl'))
          >>> db = Database('test')
              tx = db.begin_tx()
              tx.explain_inference(File('inferences.ttl'))
        """

        try:
            return self._explain_inference(content, self.id)
        except StardogException as e:
            if e.stardog_code == "0D0012":
                raise StardogException(
                    "explain_inference not yet supported in transaction",
                    400,
                    "PY999999",
                )
            else:
                raise e

    def explain_inconsistency(self, graph_uri=None) -> dict:
        """Explains why the database or a named graph is inconsistent.

        Args:
          graph_uri (optional): Named graph for which to explain inconsistency:

        Returns:
          dict: Explanation results

        Examples"
          >>> db = Database('test')
              tx = db.begin()
              tx.explain_inconsistency()
          >>> qm = QueryManager('test')
              tx = qm.begin()
              tx.explain_inconsistency()
        """

        try:
            return self._explain_inconsistency(graph_uri, self.id)
        except StardogException as e:
            if e.stardog_code == "0D0012":
                raise StardogException(
                    "explain_inference not yet supported in transaction",
                    400,
                    "PY999999",
                )
            else:
                raise e

    def size(self, exact: bool = False):
        """Database size.

        Args:
          exact: Calculate the size exactly. Defaults to False

        Returns:
          int: The number of elements in database
        """
        r = self.client().get("/size", params={"exact": exact})
        return int(r.text)
