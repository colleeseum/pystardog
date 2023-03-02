"""Content that can be loaded into Stardog.
"""
import contextlib
import pathlib

import requests
from pydantic import validate_arguments,  HttpUrl, FilePath, ValidationError, BaseModel
from pydantic.error_wrappers import ErrorWrapper

from stardog2.utils.smart_enum import SmartEnum


class ContentType(SmartEnum):
    TURTLE = "text/turtle"
    RDF_XML = "application/rdf+xml"
    NTRIPLES = "application/n-triples"
    NQUADS = "application/n-quads"
    TRIG = "application/trig"
    LD_JSON = "application/ld+json"
    # ---------------------------
    SPARQL_XML = "application/sparql-results+xml"
    SPARQL_JSON = "application/sparql-results+json"
    BINARY_RDF = "application/x-binary-rdf-results-table"
    BOOLEAN = "text/boolean"
    # ---------------------------
    CSV = "text/csv"
    TSV = "text/tab-separated-values"
    JSON = "application/json"
    # ------------------------------


class GraphContentType(SmartEnum):
    TURTLE = "text/turtle"
    NQUADS = "application/n-quads"
    NTRIPLES = "application/n-triples"
    LD_JSON = "application/ld+json"
    RDF_XML = "application/rdf+xml"
    TRIG = "application/trig"


class SelectContentType(SmartEnum):
    SPARQL_XML = "application/sparql-results+xml"
    BINARY_RDF = "application/x-binary-rdf-results-table"
    SPARQL_JSON = "application/sparql-results+json"
    CSV = "text/csv"
    TSV = "text/tab-separated-values"


class AskContentType(SmartEnum):
    BOOLEAN = "text/boolean"
    SPARQL_JSON = "application/sparql-results+json"
    SPARQL_XML = "application/sparql-results+xml"


class ImportContentType(SmartEnum):
    CSV = "text/csv"
    TSV = "text/tab-separated-values"
    JSON = "application/json"


class MappingSyntax(SmartEnum):
    SMS2 = "SMS2"
    R2RML = "R2RML"
    STARDOG = "STARDOG"


class ImportType(SmartEnum):
    DELIMITED = "delimited"
    JSON = "json"


class ContentEncoding(SmartEnum):
    GZIP = "gzip"
    ZIP = "zip"
    BZIP2 = "bzip2"


class EncodingSuffix:
    __COMPRESSION_EXTENSIONS = {
        ".gz": ContentEncoding.GZIP,
        ".zip": ContentEncoding.ZIP,
        ".bz2": ContentEncoding.BZIP2
    }

    @classmethod
    def is_encoding_suffix(cls, suffix: str):
        return suffix in cls.__COMPRESSION_EXTENSIONS.keys()

    @classmethod
    def get_encoding(cls, suffix: str):
        if suffix in cls.__COMPRESSION_EXTENSIONS.keys():
            return cls.__COMPRESSION_EXTENSIONS[suffix]

        return None


class TypeSuffix:
    __FILEINFO = {
        "TURTLE": {"content_type": ContentType.TURTLE, "add": True, "result": True},
        "RDF_XML": {"content_type": ContentType.RDF_XML, "add": True, "result": True},
        "NTRIPLES": {"content_type": ContentType.NTRIPLES, "add": True, "result": True},
        "NQUADS": {"content_type": ContentType.NQUADS, "add": True, "result": True},
        "TRIG": {"content_type": ContentType.TRIG, "add": True, "result": True},
        "LD_JSON": {"content_type": ContentType.LD_JSON, "add": True, "result": True},

        # -----------------------------
        "SPARQL_XML": {"content_type": ContentType.SPARQL_XML, "result": True},
        "SPARQL_JSON": {"content_type": ContentType.SPARQL_JSON, "result": True},
        "BINARY_RDF": {"content_type": ContentType.BINARY_RDF, "result": True},
        "BOOLEAN": {"content_type": ContentType.BOOLEAN, "result": True},

        # ----------------------------
        "JSON": {"content_type": ContentType.JSON, "input_type": ImportType.JSON},
        "CSV": {"content_type": ContentType.CSV, "input_type": ImportType.DELIMITED, "separator": ',',
                "result": True},
        "TSV": {"content_type": ContentType.TSV, "input_type": ImportType.DELIMITED, "separator": ',',
                "result": True},

        # ----------------------------
        "SMS2": {"mapping": MappingSyntax.SMS2},
        "R2RML": {"mapping": MappingSyntax.R2RML},
        "STARDOG": {"mapping": MappingSyntax.STARDOG}
    }

    __RDF_EXTENSIONS = {
        ".ttl": __FILEINFO["TURTLE"],
        ".rdf": __FILEINFO["RDF_XML"],
        ".rdfs": __FILEINFO["RDF_XML"],
        ".owl": __FILEINFO["RDF_XML"],
        ".xml": __FILEINFO["RDF_XML"],
        ".nt": __FILEINFO["NTRIPLES"],
        ".nq": __FILEINFO["NQUADS"],
        ".nquads": __FILEINFO["NQUADS"],
        ".trig": __FILEINFO["TRIG"],
        ".jsonld": __FILEINFO["LD_JSON"]
    }

    __IMPORT_EXTENSIONS = {
        ".json": __FILEINFO["JSON"],
        ".csv": __FILEINFO["CSV"],
        ".tsv": __FILEINFO["TSV"]
    }

    __MAPPING_EXTENSIONS = {
        ".sms2": __FILEINFO["SMS2"],
        ".sms": __FILEINFO["SMS2"],
        ".rq": __FILEINFO["SMS2"],
        ".r2rml": __FILEINFO["R2RML"]
    }

    __ALL_EXTENSIONS = {}
    __ALL_EXTENSIONS.update(__RDF_EXTENSIONS)
    __ALL_EXTENSIONS.update(__IMPORT_EXTENSIONS)
    __ALL_EXTENSIONS.update(__MAPPING_EXTENSIONS)

    @classmethod
    def is_type_suffix(cls, suffix: str):
        return suffix in cls.__ALL_EXTENSIONS.keys()

    @classmethod
    def is_rdf_suffix(cls, suffix: str):
        return suffix in cls.__RDF_EXTENSIONS.keys()

    @classmethod
    def is_import_suffix(cls, suffix: str):
        return suffix in cls.__IMPORT_EXTENSIONS.keys()

    @classmethod
    def is_mapping_suffix(cls, suffix: str):
        return suffix in cls.__MAPPING_EXTENSIONS.keys()

    @classmethod
    def get_suffix_info(cls, suffix: str):
        if suffix in cls.__ALL_EXTENSIONS.keys():
            return cls.__ALL_EXTENSIONS[suffix]

        return None


class Init(BaseModel):
    """This class is simply for the validation error to look correct when printed"""
    pass


class Content:
    """Content base class."""

    def __init__(self, name: pathlib.Path):

        classname = self.__class__.__name__

        if classname == 'Content':
            raise ValidationError(
                [ErrorWrapper(TypeError("This class should not be instantiated directly"), loc=f"Class {classname}")],
                Init
            )

        self._name = name
        self.__auto_detect()

        if not self.is_mapping_file and not self.is_result_file and not self.is_add_file and not self.is_import_file:
            raise ValidationError(
                [ErrorWrapper(TypeError(
                    f"unknown file extension, use class Rdf{classname}, Mapping{classname} or Import{classname} "),
                    loc=f"Class {classname}")],
                Init
            )

    def __auto_detect(self) -> None:

        # if compressed, needs content encoding
        content_encoding = self.get_encoding()

        if content_encoding:
            if self.content_encoding is None:
                self._content_encoding = content_encoding

        # get content type
        info = self.get_suffix_info()

        if info:
            keys = info.keys()

            if 'content_type' in keys:
                self._content_type = info['content_type']
            if 'mapping' in keys:
                self._mapping_syntax = info['mapping']
            if 'separator' in keys:
                self._separator = info['separator']
            if 'input_type' in keys:
                self._input_type = info['input_type']
            if 'add' in keys:
                self._add = info['add']
            if 'result' in keys:
                self._result = info['result']

    @property
    def name(self) -> str:
        return self._name.name

    @property
    def content_type_enum(self) -> ContentType:
        if hasattr(self, '_content_type'):
            return self._content_type
        return None

    @property
    def content_encoding_enum(self) -> ContentType:
        if hasattr(self, '_content_encoding'):
            return self._content_encoding
        return None

    def mapping_syntax_enum(self) -> MappingSyntax:
        if hasattr(self, '_mapping_syntax'):
            return self._mapping_syntax
        return None

    def input_type_enum(self) -> str:
        if hasattr(self, '_input_type'):
            return self._input_type
        return None

    def separator_enum(self) -> str:
        if hasattr(self, '_separator'):
            return self._separator
        return None

    @property
    def content_type(self) -> str:
        if hasattr(self, '_content_type'):
            return self.__value(self._content_type)
        return None

    @property
    def content_encoding(self) -> str:
        if hasattr(self, '_content_encoding'):
            return self.__value(self._content_encoding)
        return None

    def mapping_syntax(self) -> str:
        if hasattr(self, '_mapping_syntax'):
            return self.__value(self._mapping_syntax)
        return None

    def input_type(self) -> str:
        if hasattr(self, '_input_type'):
            return self.__value(self._input_type)
        return None

    def separator(self) -> str:
        if hasattr(self, '_separator'):
            return self.__value(self._separator)
        return None

    @property
    def is_add_file(self) -> bool:
        if hasattr(self, '_add'):
            return self._add
        return False

    @property
    def is_result_file(self) -> bool:
        if hasattr(self, '_result'):
            return self._result
        return False

    @property
    def is_import_file(self) -> bool:
        if hasattr(self, '_input_type'):
            return self._input_type is not None
        return False

    @property
    def is_mapping_file(self) -> bool:
        if hasattr(self, '_mapping_syntax'):
            return self._mapping_syntax is not None
        return False

    def has_mapping_suffix(self, path: pathlib.Path = None) -> bool:
        path = path if path is not None else self._name

        if path is None:
            raise Exception("Must provided path when name is not set")

        for suffix in reversed(path.suffixes):
            if TypeSuffix.is_mapping_suffix(suffix):
                return True
        return False

    def has_rdf_suffix(self, path: pathlib.Path = None) -> bool:
        path = path if path is not None else self._name

        if path is None:
            raise Exception("Must provided path when name is not set")

        for suffix in reversed(path.suffixes):
            if TypeSuffix.is_rdf_suffix(suffix):
                return True
        return False

    def has_import_suffix(self, path: pathlib.Path = None) -> bool:
        path = path if path is not None else self._name

        if path is None:
            raise Exception("Must provided path when name is not set")

        for suffix in reversed(path.suffixes):
            if TypeSuffix.is_import_suffix(suffix):
                return True
        return False

    def has_type_suffix(self, path: pathlib.Path = None) -> bool:
        path = path if path is not None else self._name

        if path is None:
            raise Exception("Must provided path when name is not set")

        for suffix in reversed(path.suffixes):
            if TypeSuffix.is_import_suffix(suffix):
                return True
        return False

    def has_encoding_suffix(self, path: pathlib.Path = None) -> bool:
        path = path if path is not None else self._name

        if path is None:
            raise Exception("Must provided path when name is not set")

        for suffix in reversed(path.suffixes.reverse):
            if EncodingSuffix.is_encoding_suffix(suffix):
                return suffix
        return None

    def get_suffix_info(self) -> dict | None:
        for suffix in reversed(self._name.suffixes):
            info = TypeSuffix.get_suffix_info(suffix)
            if info is not None:
                return info
        return None

    def get_encoding(self) -> str | None:
        for suffix in reversed(self._name.suffixes):
            encoding = EncodingSuffix.get_encoding(suffix)
            if encoding is not None:
                return encoding
        return None

    @staticmethod
    def __value(value):
        if isinstance(value, str) or value is None:
            return value
        else:
            return value.value

    def data(self) -> str:
        pass

class _MappingMixIn:

    def _validate(self, name: pathlib.Path, syntax: MappingSyntax | None):

        # noinspection PyUnresolvedReferences
        if not self.has_mapping_suffix(name) and syntax is None:
            raise ValidationError(
                [ErrorWrapper(TypeError("must be provided for unknown mapping file extension"), loc="syntax")],
                Init
            )

        self._mapping_syntax = syntax


class _RdfMixIn:

    def _validate(self,
                  name: pathlib.Path,
                  content_type: ContentType | None,
                  content_encoding: ContentEncoding | None):

        # noinspection PyUnresolvedReferences
        if not self.has_rdf_suffix(name) and content_type is None:
            raise ValidationError(
                [ErrorWrapper(TypeError("must be provided for unknown rdf file extension"), loc="content_type")],
                Init
            )

        self._content_type = content_type
        self._content_encoding = content_encoding
        self._result = True
        self._add = True


class _ImportMixIn:
    def _validate(self,
                  name: pathlib.Path,
                  content_type: ContentType | None,
                  content_encoding: ContentEncoding | None,
                  input_type: ImportType | None,
                  separator: str | None
                  ):
        # noinspection PyUnresolvedReferences
        if not self.has_import_suffix(name):
            errors = []

            if content_type is None:
                errors.append(
                    ErrorWrapper(TypeError("must be provided for unknown import extension"), loc="content_type")
                )

            if input_type is None:
                errors.append(
                    ErrorWrapper(TypeError("must be provided for unknown import extension"), loc="input_type")
                )

            if separator is None:
                errors.append(
                    ErrorWrapper(TypeError("must be provided for unknown import extension"), loc="separator")
                )

            if len(errors) > 0:
                raise ValidationError(errors, Init)

        self._content_type = content_type
        self._content_encoding = content_encoding
        self._input_type = input_type
        self._separator = separator


class File(Content):
    @validate_arguments()
    def __init__(self, file: FilePath):
        """Initializes a File object.

        Args:
          file: Filename

        Base on the filename extension it will auto-detect information required for the API. This could be a file to
        load or import data, or a mapping file. This interface is awesome when dealing with well known file extension
        such as but not limited to .ttl, .trix, .owl, .csv, .tsv, .json, .rq, .sms2.

        It will raise an exception if it's unable to determine any valid configuration based on the extension.


        File instance.

        For more control, see

            * RdfFile(...) -- will only create file to add data
            * ImportFile(...) -- will only create file to i
            * MappingFile(...) -- will only create mapping file instance

        Examples:
            >>> GraphFile('data.ttl')

        """

        super().__init__(file)

    @property
    def file(self) -> pathlib.Path:
        return self._name

    @contextlib.contextmanager
    def data(self) -> str:
        with open(self.name, "rb") as f:
            yield f


class GraphFile(File, _RdfMixIn):
    @validate_arguments()
    def __init__(
            self, file: FilePath, content_type: GraphContentType = None, content_encoding: ContentEncoding = None
    ):
        """Initializes a File object of rdf file only.

        Args:
          file: Filename
          content_type: It will be automatically detected from the filename if possible, otherwise it's required
          content_encoding: It will be automatically detected from the filename if possible

        Examples:
          >>> GraphFile('data.ttl')
          >>> GraphFile('data.turtle', GraphContentType.TURTLE, ContentEncoding.GZIP)
        """
        self._validate(file, content_type, content_encoding)
        super(File, self).__init__(file)


class MappingFile(File, _MappingMixIn):
    """File-based content."""

    @validate_arguments()
    def __init__(self, file: pathlib.Path, syntax: MappingSyntax = None):
        """Initializes a File object.

        Args:
          file: Filename
          syntax: It will be automatically detected from the filename, if possible otherwise it's required

        Examples:
          >>> MappingFile('data.sms')
          >>> MappingFile('data.txt', MappingSyntax.SMS2)
        """

        self._validate(file, syntax)
        super().__init__(file)


class ImportFile(File, _ImportMixIn):
    """File-based content for Delimited and JSON file."""

    @validate_arguments()
    def __init__(
            self,
            file: pathlib.Path,
            content_type: ImportContentType = None,
            content_encoding: ContentEncoding = None,
            input_type: ImportType = None,
            separator: str = None
    ):
        """Initializes a File object.

        Args:
          file: Filename
          input_type: It will be automatically detedted from filename if possible, otherwise it's required.
          separator: It will be automatically detected from filename if possible, otherwise it's required.
          content_type: It will be automatically detected from the filename if possible, otherwise it's required.
          content_encoding: It will be automatically detected from the filename if possible.

        Examples:
          >>> ImportFile('data.tsv')
          >>> ImportFile('data.txt',ImportType.DELIMITED, separator="\t" )
        """

        self._validate(file, content_type, content_encoding, input_type, separator)
        super().__init__(file)


class Raw(Content):
    @validate_arguments()
    def __init__(self, content: str, name: pathlib.Path):
        """Initializes a Raw Content object.

        Args:
            content: The content string
            name: Pseudo filename

        Base on the name extension it will auto-detect information required for the API. This could be a raw object to
        load or import data, or a mapping file. This interface is awesome when dealing with well known file extension
        such as but not limited to .ttl, .trix, .owl, .csv, .tsv, .json, .rq, .sms2.

        It will raise an exception if it's unable to determine any valid configuration based on the name given.

        For more control, see

            * RdfRaw(...) -- will only create raw object to add data.
            * ImportRaw(...) -- will only create raw object to import data.
            * MappingRaw(...) -- will only create raw object for mapping rules.

        Examples:
          >>> Raw("content", 'data.ttl')

        """
        self.__raw = content
        super().__init__(name)

    @contextlib.contextmanager
    def data(self) -> str:
        yield self.__raw


class GraphRaw(Raw, _RdfMixIn):
    @validate_arguments()
    def __init__(
            self, content: str, name: pathlib.Path = pathlib.Path("default"), content_type: GraphContentType = None,
            content_encoding: ContentEncoding = None
    ):
        """Initializes a raw object of rdf content only.

        Args:
          content: raw data
          name: Id, pseudo filename . Default is 'default'
          content_type: It will be automatically detected from the name if possible, otherwise it's required
          content_encoding: It will be automatically detected from the name if possible

        Examples:
          >>> GraphRaw("content", 'data.ttl')
          >>> GraphRaw(content","'data.turtle', GraphContentType.TURTLE, ContentEncoding.GZIP)
        """

        self._validate(name, content_type, content_encoding)
        super().__init__(content, name)


class MappingRaw(Raw, _MappingMixIn):
    """Initializes a raw object of mapping content only."""

    @validate_arguments()
    def __init__(self,
                 content: str,
                 name: pathlib.Path = pathlib.Path("default"),
                 syntax: MappingSyntax = None):
        """Initializes a File object.

        Args:
          content: raw data
          name: Id, pseudo filename . Default is 'default'
          syntax: It will be automatically detected from the filename, if possible otherwise it's required

        Examples:
          >>> MappingRaw('content','data.sms')
          >>> MappingRaw('content', 'data.txt', MappingSyntax.SMS2)
        """

        self._validate(name, syntax)
        super().__init__(content, name)


class ImportRaw(Raw, _ImportMixIn):

    @validate_arguments()
    def __init__(
            self,
            content: str,
            name: pathlib.Path = pathlib.Path("default"),
            content_type: ImportContentType = None,
            content_encoding: ContentEncoding = None,
            input_type: ImportType = None,
            separator: str = None
    ):
        """Initializes a raw object of delimited or JSON content only.

        Args:
          content: raw data
          name: Id, pseudo filename . Default is 'default'
          input_type: It will be automatically detedted from filename if possible, otherwise it's required.
          separator: It will be automatically detected from filename if possible, otherwise it's required.
          content_type: It will be automatically detected from the filename if possible, otherwise it's required.
          content_encoding: It will be automatically detected from the filename if possible.

        Examples:
          >>> ImportFile('data.tsv')
          >>> ImportFile('data.txt',ImportType.DELIMITED, separator="\t" )
        """

        self._validate(name, content_type, content_encoding, input_type, separator)
        super().__init__(content, name)


class Url(Content):
    @validate_arguments()
    def __init__(self, url: HttpUrl):
        """Initializes a File object.

        Args:
          url: url

        Base on the filename extension it will auto-detect information required for the API. This could be a file to
        load or import data, or a mapping file. This interface is awesome when dealing with well known file extension
        such as but not limited to .ttl, .trix, .owl, .csv, .tsv, .json, .rq, .sms2.

        It will raise an exception if it's unable to determine any valid configuration based on the extension.


        File instance.

        For more control, see

            * RdfUrl(...) -- will only create file to add data
            * ImportUrl(...) -- will only create file to i
            * Mappingurl(...) -- will only create mapping file instance

        Examples:
          >>> GraphUrl(HttpUrl('https://example.org/data.ttl'))
          >>> GraphUrl('https://example.org/data.ttl')

        """

        self.__url = url

        super().__init__(pathlib.Path(url.path))

    @property
    def url(self) -> HttpUrl:
        return self.__url

    @contextlib.contextmanager
    def data(self):
        with requests.get(str(self.url), stream=True) as r:
            yield r.content


class GraphUrl(Url, _RdfMixIn):
    @validate_arguments()
    def __init__(
            self, url: HttpUrl, content_type: GraphContentType = None, content_encoding: ContentEncoding = None
    ):
        """Initializes an url object of rdf content only.

        Args:
          url: The url from where the data is located
          content_type: It will be automatically detected from the url if possible, otherwise it's required
          content_encoding: It will be automatically detected from the url if possible

        Examples:
          >>> GraphUrl(HttpUrl('https://example.org/data.ttl'))
          >>> GraphUrl('https://example.org/data.ttl')
          >>> GraphUrl('https://example.org/data.turtle', GraphContentType.TURTLE, ContentEncoding.GZIP)
        """

        self._validate(pathlib.Path(url.path), content_type, content_encoding)
        super().__init__(url)


class MappingUrl(Url, _MappingMixIn):

    @validate_arguments()
    def __init__(self,
                 url: HttpUrl,
                 syntax: MappingSyntax = None):
        """Initializes an url object of mapping content only.

        Args:
          url: The url from where the data is located
          syntax: It will be automatically detected from the url, if possible otherwise it's required

        Examples:
          >>> MappingUrl(HttpUrl('https://example.org/data.sms'))
          >>> MappingUrl('https://example.org/data.sms')
          >>> MappingUrl('https://example.org/data.txt', MappingSyntax.SMS2)
        """

        self._validate(pathlib.Path(url.path), syntax)
        super().__init__(url)


class ImportUrl(Url, _ImportMixIn):

    @validate_arguments()
    def __init__(
            self,
            url: HttpUrl,
            content_type: ImportContentType = None,
            content_encoding: ContentEncoding = None,
            input_type: ImportType = None,
            separator: str = None
    ):
        """Initializes an url object for delimited or JSON content only.

        Args:
          url: The url from where the data is located
          input_type: It will be automatically detedted from url if possible, otherwise it's required.
          separator: It will be automatically detected from url if possible, otherwise it's required.
          content_type: It will be automatically detected from the url if possible, otherwise it's required.
          content_encoding: It will be automatically detected from the url if possible.

        Examples:
          >>> ImportUrl(HttpUrl('https://example.org/data.ttl'))
          >>> ImportUrl('https://example.org/data.ttl')
          >>> ImportUrl('https://example.org/data.txt',ImportType.DELIMITED, separator="\t" )
        """
        self._validate(pathlib.Path(url.path), content_type, content_encoding, input_type, separator)
        super().__init__(url)
