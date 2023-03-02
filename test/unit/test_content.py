from unittest import mock

import pytest

from stardog2.content import *


# noinspection PyTypeChecker
@mock.patch("pathlib.Path.is_file", return_value=True)
@mock.patch("pathlib.Path.exists", return_value=True)
@mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
class TestFileAndContent:
    """ This test the base Content and File object.

    This test all the logic around name, and filename to autodetect, and most of the generic validation code
    """

    # noinspection PyUnusedLocal
    def test_new_block(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(ValidationError,
                           match="1 validation error for Init\nClass Content\n  This class should not be instantiated directly"):
            Content("test.ttl")

    # noinspection PyUnusedLocal
    def test_ttl(self, mock_open, mock_exists, mock_is_file):
        m = File("test.ttl")
        with m.data() as data:
            for line in data:
                assert line == "some_data"
        assert m.name == "test.ttl"
        assert m.file.name == 'test.ttl'
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_rdf(self, mock_open, mock_exists, mock_is_file):
        m = File("test.rdf")
        assert m.name == "test.rdf"
        assert m.file.name == 'test.rdf'
        assert m.content_type == ContentType.RDF_XML.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_rdfs(self, mock_open, mock_exists, mock_is_file):
        m = File("test.rdfs")
        assert m.name == "test.rdfs"
        assert m.file.name == 'test.rdfs'
        assert m.content_type == ContentType.RDF_XML.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_owl(self, mock_open, mock_exists, mock_is_file):
        m = File("test.owl")
        assert m.name == "test.owl"
        assert m.file.name == 'test.owl'
        assert m.content_type == ContentType.RDF_XML.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_xml(self, mock_open, mock_exists, mock_is_file):
        m = File("test.xml")
        assert m.name == "test.xml"
        assert m.file.name == 'test.xml'
        assert m.content_type == ContentType.RDF_XML.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_nt(self, mock_open, mock_exists, mock_is_file):
        m = File("test.nt")
        assert m.name == "test.nt"
        assert m.file.name == 'test.nt'
        assert m.content_type == ContentType.NTRIPLES.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_nq(self, mock_open, mock_exists, mock_is_file):
        m = File("test.nq")
        assert m.name == "test.nq"
        assert m.file.name == 'test.nq'
        assert m.content_type == ContentType.NQUADS.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_nquad(self, mock_open, mock_exists, mock_is_file):
        m = File("test.nquads")
        assert m.name == "test.nquads"
        assert m.file.name == 'test.nquads'
        assert m.content_type == ContentType.NQUADS.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_trig(self, mock_open, mock_exists, mock_is_file):
        m = File("test.trig")
        assert m.name == "test.trig"
        assert m.file.name == 'test.trig'
        assert m.content_type == ContentType.TRIG.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_jsonld(self, mock_open, mock_exists, mock_is_file):
        m = File("test.jsonld")
        assert m.name == "test.jsonld"
        assert m.file.name == 'test.jsonld'
        assert m.content_type == ContentType.LD_JSON.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_sms2(self, mock_open, mock_exists, mock_is_file):
        m = File("test.sms2")
        assert m.name == "test.sms2"
        assert m.file.name == 'test.sms2'
        assert m.content_encoding is None
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_sms(self, mock_open, mock_exists, mock_is_file):
        m = File("test.sms")
        assert m.name == "test.sms"
        assert m.file.name == 'test.sms'
        assert m.content_encoding is None
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_rq(self, mock_open, mock_exists, mock_is_file):
        m = File("test.rq")
        assert m.name == "test.rq"
        assert m.file.name == 'test.rq'
        assert m.content_encoding is None
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_r2rml(self, mock_open, mock_exists, mock_is_file):
        m = File("test.r2rml")
        assert m.name == "test.r2rml"
        assert m.file.name == 'test.r2rml'
        assert m.content_encoding is None
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_json(self, mock_open, mock_exists, mock_is_file):
        m = File("test.json")
        assert m.name == "test.json"
        assert m.file.name == 'test.json'
        assert m.content_type == ContentType.JSON.value
        assert m.content_encoding is None
        assert not m.is_result_file
        assert not m.is_add_file
        assert m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_gz(self, mock_open, mock_exists, mock_is_file):
        m = File("test.ttl.gz")
        assert m.name == "test.ttl.gz"
        assert m.file.name == 'test.ttl.gz'
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding == "gzip"
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_zip(self, mock_open, mock_exists, mock_is_file):
        m = File("test.ttl.zip")
        assert m.name == "test.ttl.zip"
        assert m.file.name == 'test.ttl.zip'
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding == "zip"
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_bz2(self, mock_open, mock_exists, mock_is_file):
        m = File("test.ttl.bz2")
        assert m.name == "test.ttl.bz2"
        assert m.file.name == 'test.ttl.bz2'
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding == "bzip2"
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_unknown(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(ValidationError,
                           match="1 validation error for Init\nClass File\n  unknown file extension, use class RdfFile, MappingFile or ImportFile"):
            File("test.unknown")


# noinspection PyTypeChecker
@mock.patch("pathlib.Path.is_file", return_value=True)
@mock.patch("pathlib.Path.exists", return_value=True)
@mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
class TestRdfFile:
    """ Test interface specific to RdfFile"""

    # noinspection PyUnusedLocal
    def test_ttl(self, mock_open, mock_exists, mock_is_file):
        m = GraphFile("test.ttl")
        assert m.name == "test.ttl"
        assert m.file.name == "test.ttl"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_with_content_type_encoding(self, mock_open, mock_exists, mock_is_file):
        m = GraphFile("test.turtle", GraphContentType.TURTLE, ContentEncoding.GZIP)
        assert m.name == "test.turtle"
        assert m.file.name == "test.turtle"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding == 'gzip'
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    def test_unknown(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(ValidationError,
                           match="1 validation error for Init\ncontent_type\n  must be provided for unknown rdf file extension"):
            GraphFile("test.unknown")


# noinspection PyTypeChecker
class TestImportFile:
    """ Test interface specific to ImportFile"""

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_simple(self, mock_open, mock_exists, mock_is_file):
        m = ImportFile("test.csv")
        assert m.name == "test.csv"
        assert m.file.name == "test.csv"
        assert m.content_type == ContentType.CSV.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert not m.is_add_file
        assert m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_with_override(self, mock_open, mock_exists, mock_is_file):
        m = ImportFile("test.txt",
                       input_type=ImportType.JSON,
                       separator=',',
                       content_type=ImportContentType.CSV,
                       content_encoding=ContentEncoding.GZIP)
        assert m.name == "test.txt"
        assert m.file.name == "test.txt"
        assert m.content_type == ContentType.CSV.value
        assert m.content_encoding == 'gzip'
        assert not m.is_result_file
        assert not m.is_add_file
        assert m.is_import_file
        assert not m.is_mapping_file

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_unknown_1(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(
                Exception,
                match="3 validation errors for Init\ncontent_type\n  must be provided for unknown import extension"):
            ImportFile("test.unknown")

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_unknown_missing_separator(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\nseparator\n  must be provided for unknown import extension"):
            ImportFile("test.unknown",
                       input_type=ImportType.DELIMITED,
                       content_type=ImportContentType.CSV)

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_unknown_missing_input_type(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\ninput_type\n  must be provided for unknown import extension"):
            ImportFile("test.unknown",
                       separator=",",
                       content_type=ImportContentType.CSV)

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_unknown_missing_content_type(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\ncontent_type\n  must be provided for unknown import extension"):
            ImportFile("test.unknown",
                       separator=",",
                       input_type=ImportType.DELIMITED)

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    def test_bad_args(self, mock_exists, mock_is_file):
        with pytest.raises(
                ValidationError,
                match='3 validation errors for Init\ncontent_type\n  value is not a valid enumeration member; permitted:'):
            ImportFile("test.sms2", 'test', 'test', 'test', '\n')

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=False)
    @mock.patch("pathlib.Path.exists", return_value=True)
    def test_file_arg_no_a_file(self, mock_exists, mock_is_file):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nfile\n  path "test.csv" does not point to a file'):
            ImportFile("test.csv")

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.exists", return_value=False)
    def test_file_arg_file_not_exists(self, mock_exists):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nfile\n  file or directory at path "test.csv" does not exist'):
            ImportFile("test.csv")


# noinspection PyTypeChecker
class TestMappingFile:
    """ Test interface specific to MappingFile"""

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_simple(self, mock_open, mock_exists, mock_is_file):
        m = MappingFile("test.sms2")
        assert m.name == "test.sms2"
        assert m.file.name == "test.sms2"
        assert m.mapping_syntax() == MappingSyntax.SMS2.value
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_with_override(self, mock_open, mock_exists, mock_is_file):
        m = MappingFile("test.txt", syntax=MappingSyntax.SMS2)
        assert m.name == "test.txt"
        assert m.file.name == "test.txt"
        assert m.mapping_syntax() == MappingSyntax.SMS2.value
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=True)
    @mock.patch("pathlib.Path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open(read_data="some_data"))
    def test_unknown_1(self, mock_open, mock_exists, mock_is_file):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\nsyntax\n  must be provided for unknown mapping file extension"):
            MappingFile("test.unknown")

    def test_bad_syntax_arg(self):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nsyntax\n  value is not a valid enumeration member; permitted:'):
            MappingFile("test.unknown", 'test')

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.is_file", return_value=False)
    @mock.patch("pathlib.Path.exists", return_value=True)
    def test_file_arg_no_a_file(self, mock_exists, mock_is_file):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nfile\n  path "test.sms2" does not point to a file'):
            MappingFile("test.sms2")

    # noinspection PyUnusedLocal
    @mock.patch("pathlib.Path.exists", return_value=False)
    def test_file_arg_file_not_exists(self, mock_exists):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nfile\n  file or directory at path "test.sms2" does not exist'):
            MappingFile("test.sms2")


# noinspection PyTypeChecker
class TestRaw:

    def test_simple(self):
        m = Raw("some_data", 'test.ttl')
        with m.data() as data:
            assert data == "some_data"
        assert m.name == "test.ttl"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    def test_unknown(self):
        with pytest.raises(ValidationError,
                           match="1 validation error for Init\nClass Raw\n  unknown file extension, use class RdfRaw, MappingRaw or ImportRaw"):
            Raw("some_data", 'test.unknown')


# noinspection PyTypeChecker
class TestRdfRaw:
    """ Test interface specific to RdfRaw"""

    def test_ttl(self):
        m = GraphRaw("some_data", name="test.ttl")
        assert m.name == "test.ttl"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    def test_with_content_type_encoding(self):
        m = GraphRaw("some_data", "test.turtle", GraphContentType.TURTLE, ContentEncoding.GZIP)
        assert m.name == "test.turtle"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding == 'gzip'
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    def test_unknown(self):
        with pytest.raises(Exception,
                           match="1 validation error for Init\ncontent_type\n  must be provided for unknown rdf file extension"):
            GraphRaw("some_data")


# noinspection PyTypeChecker
class TestImportRaw:
    """ Test interface specific to ImportRaw"""

    def test_simple(self):
        m = ImportRaw("some_data", "test.csv")
        assert m.name == "test.csv"
        assert m.content_type == ContentType.CSV.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert not m.is_add_file
        assert m.is_import_file
        assert not m.is_mapping_file

    def test_with_override(self):
        m = ImportRaw("some_data",
                      name="test.txt",
                      input_type=ImportType.JSON,
                      separator=',',
                      content_type=ImportContentType.CSV,
                      content_encoding=ContentEncoding.GZIP)
        assert m.name == "test.txt"
        assert m.content_type == ContentType.CSV.value
        assert m.content_encoding == 'gzip'
        assert not m.is_result_file
        assert not m.is_add_file
        assert m.is_import_file
        assert not m.is_mapping_file

    def test_unknown_1(self):
        with pytest.raises(
                Exception,
                match="3 validation errors for Init\ncontent_type\n  must be provided for unknown import extension"):
            ImportRaw("some_data", "test.unknown")

    def test_unknown_missing_separator(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\nseparator\n  must be provided for unknown import extension"):
            ImportRaw("some_data",
                      name="test.unknown",
                      input_type=ImportType.DELIMITED,
                      content_type=ImportContentType.CSV)

    def test_unknown_missing_input_type(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\ninput_type\n  must be provided for unknown import extension"):
            ImportRaw("some_data",
                      name="test.unknown",
                      separator=",",
                      content_type=ImportContentType.CSV)

    def test_unknown_missing_content_type(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\ncontent_type\n  must be provided for unknown import extension"):
            ImportRaw("some_data",
                      name="test.unknown",
                      separator=",",
                      input_type=ImportType.DELIMITED)

    def test_bad_args(self):
        with pytest.raises(
                ValidationError,
                match='3 validation errors for Init\ncontent_type\n  value is not a valid enumeration member; permitted:'):
            ImportRaw("some_data", 'test', 'test', 'test', '\n')


# noinspection PyTypeChecker
class TestMappingRaw:
    """ Test interface specific to MappingFile"""

    def test_simple(self):
        m = MappingRaw("some_data", "test.sms2")
        assert m.name == "test.sms2"
        assert m.mapping_syntax() == MappingSyntax.SMS2.value
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    def test_with_override(self):
        m = MappingRaw("some_data", syntax=MappingSyntax.SMS2)
        assert m.name == "default"
        assert m.mapping_syntax() == MappingSyntax.SMS2.value
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    def test_unknown_1(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\nsyntax\n  must be provided for unknown mapping file extension"):
            MappingRaw("some_data")

    def test_bad_syntax_arg(self):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nsyntax\n  value is not a valid enumeration member; permitted:'):
            MappingRaw("some_data", "test.unknown", 'test')


# noinspection PyTypeChecker
class TestUrl:

    @mock.patch('requests.get')
    def test_simple(self, mock_get):
        mock_get.return_value.__enter__.return_value.status_code = 200
        mock_get.return_value.__enter__.return_value.content = 'some_data'
        m = Url('https://example.org/hello/test.ttl')
        with m.data() as data:
            assert data == "some_data"
        assert m.name == "test.ttl"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    def test_unknown(self):
        with pytest.raises(Exception,
                           match="1 validation error for Init\nClass Url\n  unknown file extension, use class RdfUrl, MappingUrl or ImportUrl"):
            Url('https://example.org/hello/test.unknown')

    def test_invalid_url(self):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nurl\n  invalid or missing URL scheme'):
            Url("test.unknown")


# noinspection PyTypeChecker
class TestRdfUrl:
    """ Test interface specific to RdfUrl"""

    @mock.patch('requests.get')
    def test_simple(self, mock_get):
        mock_get.return_value.__enter__.return_value.status_code = 200
        mock_get.return_value.__enter__.return_value.content = 'some_data'
        m = GraphUrl('https://example.org/hello/test.ttl')
        with m.data() as data:
            assert data == "some_data"
        assert m.name == "test.ttl"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    def test_with_content_type_encoding(self):
        m = GraphRaw("some_data", "test.turtle", GraphContentType.TURTLE, ContentEncoding.GZIP)
        assert m.name == "test.turtle"
        assert m.content_type == ContentType.TURTLE.value
        assert m.content_encoding == 'gzip'
        assert m.is_result_file
        assert m.is_add_file
        assert not m.is_import_file
        assert not m.is_mapping_file

    def test_unknown(self):
        with pytest.raises(Exception,
                           match="1 validation error for Init\ncontent_type\n  must be provided for unknown rdf file extension"):
            GraphRaw("some_data")

    def test_invalid_url(self):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nurl\n  invalid or missing URL scheme'):
            GraphUrl("test.unknown")


# noinspection PyTypeChecker
class TestImportUrl:
    """ Test interface specific to ImportRaw"""

    @mock.patch('requests.get')
    def test_simple(self, mock_get):
        mock_get.return_value.__enter__.return_value.status_code = 200
        mock_get.return_value.__enter__.return_value.content = 'some_data'
        m = ImportUrl('https://example.org/hello/test.csv')
        with m.data() as data:
            assert data == "some_data"
        assert m.name == "test.csv"
        assert m.content_type == ContentType.CSV.value
        assert m.content_encoding is None
        assert m.is_result_file
        assert not m.is_add_file
        assert m.is_import_file
        assert not m.is_mapping_file

    def test_with_override(self):
        m = ImportUrl("https://example.org/hello/test.txt",
                      input_type=ImportType.JSON,
                      separator=',',
                      content_type=ImportContentType.CSV,
                      content_encoding=ContentEncoding.GZIP)
        assert m.name == "test.txt"
        assert m.content_type == ContentType.CSV.value
        assert m.content_encoding == 'gzip'
        assert not m.is_result_file
        assert not m.is_add_file
        assert m.is_import_file
        assert not m.is_mapping_file

    def test_unknown_1(self):
        with pytest.raises(
                Exception,
                match="3 validation errors for Init\ncontent_type\n  must be provided for unknown import extension"):
            ImportUrl("https://example.org/hello/test.ttl")

    def test_unknown_missing_separator(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\nseparator\n  must be provided for unknown import extension"):
            ImportUrl("https://example.org/hello/test.ttl",
                      input_type=ImportType.DELIMITED,
                      content_type=ImportContentType.CSV)

    def test_unknown_missing_input_type(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\ninput_type\n  must be provided for unknown import extension"):
            ImportUrl("https://example.org/hello/test.unknown",
                      separator=",",
                      content_type=ImportContentType.CSV)

    def test_unknown_missing_content_type(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\ncontent_type\n  must be provided for unknown import extension"):
            ImportUrl("https://example.org/hello/test.unknown",
                      separator=",",
                      input_type=ImportType.DELIMITED)

    def test_invalid_url(self):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nurl\n  invalid or missing URL scheme'):
            ImportUrl("test.unknown")

    def test_bad_args(self):
        with pytest.raises(
                ValidationError,
                match='3 validation errors for Init\ncontent_type\n  value is not a valid enumeration member; permitted:'):
            ImportUrl("https://example.org/hello/test.csv", 'test', 'test', 'test', '\n')


# noinspection PyTypeChecker
class TestMappingUrl:
    """ Test interface specific to MappingFile"""

    @mock.patch('requests.get')
    def test_simple(self, mock_get):
        mock_get.return_value.__enter__.return_value.status_code = 200
        mock_get.return_value.__enter__.return_value.content = 'some_data'
        m = MappingUrl("https://example.org/hello/test.sms2")
        with m.data() as data:
            assert data == "some_data"
        assert m.name == "test.sms2"
        assert m.mapping_syntax() == MappingSyntax.SMS2.value
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    def test_with_override(self):
        m = MappingUrl("https://example.org/hello/test.turtle", syntax=MappingSyntax.SMS2)
        assert m.name == "test.turtle"
        assert m.mapping_syntax() == MappingSyntax.SMS2.value
        assert not m.is_result_file
        assert not m.is_add_file
        assert not m.is_import_file
        assert m.is_mapping_file

    def test_unknown_1(self):
        with pytest.raises(
                Exception,
                match="1 validation error for Init\nsyntax\n  must be provided for unknown mapping file extension"):
            MappingUrl("https://example.org/hello/test.unknown")

    def test_bad_syntax_arg(self):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nsyntax\n  value is not a valid enumeration member; permitted:'):
            MappingUrl("https://example.org/hello/test.sms2", 'test')

    def test_invalid_url(self):
        with pytest.raises(
                ValidationError,
                match='1 validation error for Init\nurl\n  invalid or missing URL scheme'):
            MappingUrl("test.unknown")


class TestContentType:

    def test_all_type(self):
        assert ContentType.TRIG
        assert ContentType.TURTLE
        assert ContentType.NQUADS
        assert ContentType.RDF_XML
        assert ContentType.NTRIPLES
        assert ContentType.LD_JSON
        assert ContentType.BINARY_RDF
        assert ContentType.SPARQL_XML
        assert ContentType.SPARQL_JSON
        assert ContentType.BOOLEAN
        assert ContentType.CSV
        assert ContentType.TSV
        assert ContentType.LD_JSON
        assert len(ContentType.list()) == 13

    def test_select_type(self):
        assert SelectContentType.SPARQL_XML
        assert SelectContentType.BINARY_RDF
        assert SelectContentType.SPARQL_JSON
        assert SelectContentType.CSV
        assert SelectContentType.TSV
        assert len(SelectContentType.list()) == 5

    def test_ask_type(self):
        assert AskContentType.BOOLEAN
        assert AskContentType.SPARQL_JSON
        assert AskContentType.SPARQL_XML
        assert len(AskContentType.list()) == 3

    def test_graph_type(self):
        assert GraphContentType.TURTLE
        assert GraphContentType.NQUADS
        assert GraphContentType.NTRIPLES
        assert GraphContentType.LD_JSON
        assert GraphContentType.RDF_XML
        assert GraphContentType.TRIG
        assert len(GraphContentType.list()) == 6
