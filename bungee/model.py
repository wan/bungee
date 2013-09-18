"""
This module contains the main interfaces between the various querying helpers
and ElasticSearch, via the pyelasticsearch library.

A given SearchModel supports exactly ONE index.
"""
import const
import exception
import warnings

from field import SearchField
from query import SearchQuery
from json_document import JsonDocument, ResultSet
from util import make_identifier

from pyelasticsearch import ElasticSearch, ElasticHttpNotFoundError

CONNECTION_POOL = {}
INDEX_MAPPINGS = {}


class SearchModelMeta(type):
    """
    Metaclass that provides simple connection pooling and index mapping for
    SearchModel subclasses.
    """

    def __init__(cls, name, bases, dict_):
        super(SearchModelMeta, cls).__init__(name, bases, dict_)
        if name == 'SearchModel':
            return
        if const.INDEX_NAME not in dict_:
            raise (exception.ConfigError,
                    "Must specify index_name in class definition")
        if const.DOC_TYPE not in dict_:
            cls.doc_type = None
        if const.URL in dict_:
            cls._urls = [ dict_.get(const.URL) ]
        elif const.URLS in dict_:
            cls._urls = dict_.get(const.URLS)
        else:
            cls._urls = "http://localhost:9200"
        cls.initialize_search_fields()

    @property
    def connection(cls):
        """
        Return an pyelasticsearch.ElasticSearch instance for the class URL.
        Connections are pooled globally by URL in this process.
        """
        global CONNECTION_POOL
        key = str(cls._urls)
        if key not in CONNECTION_POOL:
            CONNECTION_POOL[key] = ElasticSearch(cls._urls)
        return CONNECTION_POOL[key]

    def generate_field_mappings(cls):
        """
        Return the processed search Fields for an index given its mappings.
        Fields are cached globally by index name in this process.
        """
        global INDEX_MAPPINGS
        key = cls.index_name
        if key not in INDEX_MAPPINGS:
            mappings = cls.connection.get_mapping(index=cls.index_name)
            INDEX_MAPPINGS[key] = cls.parse_mapping(mappings.values()[0])
        return INDEX_MAPPINGS[key]

    def delete_field_mappings(cls):
        global INDEX_MAPPINGS
        field_mapping = cls.generate_field_mappings()
        for fields in field_mapping.values():
            for field_name, search_field in fields.iteritems():
                delattr(cls, search_field._field_name)
        del INDEX_MAPPINGS[cls.index_name]

    def parse_mapping(cls, mapping):
        """
        Return a dictionary containing Fields for each mapped document type in
        the class's index.
        """
        field_mappings = {}
        for doc_type in mapping.keys():
            field_mappings[doc_type] = {}
            mapping_properties = mapping[doc_type][const.PROPERTIES]
            for field_name, sub_mapping in mapping_properties.items():
                if field_name in field_mappings[doc_type]:
                    continue
                field = SearchField(field_name, sub_mapping)
                field_mappings[doc_type][field._field_name] = field
        return field_mappings

    def initialize_search_fields(cls, force_reload=False):
        """
        Generate Search Fields and bind them to the given class.
        :param force_reload: if True, force a call to get mappings from ES.
        """
        if force_reload:
            cls.delete_field_mappings()
        mappings = cls.generate_field_mappings()

        if isinstance(cls.doc_type, (str, unicode)):
            doc_types = [ cls.doc_type ] 
        elif isinstance(cls.doc_type, (list, set, tuple)):
            doc_types = list(cls.doc_type)
        else:
            doc_types = mappings.keys()
        for doc_type in doc_types:
            mapping = mappings[doc_type]
            for field in mapping.values():
                if hasattr(cls, field._field_name):
                    warnings.warn('Field "%s" is already defined for document \
type %s' % (field._field_name, doc_type))
                setattr(cls, field._field_name, field)

        # Special cases - add fields for doc type and id
        setattr(cls, const.ID, SearchField(const.ID, {}))
        setattr(cls, const.UID, SearchField(const.UID, {}))
        setattr(cls, const.TYPE, SearchField(const.TYPE, {}))


class SearchModel(object):
    """
    An object which provides an interface to one ElasticSearch index.
    """

    __metaclass__ = SearchModelMeta

    @classmethod
    def wrap_es_docs(cls, docs):
        """
        Convert result JSON "sources" ResultSet object.
        """
        result_set = ResultSet()
        for doc in docs:
            source = doc[const.SOURCE]
            source[const.ID] = doc[const.ID]
            source[const.TYPE] = doc[const.TYPE]
            result_set.documents.append(JsonDocument(source))
        return result_set

    @classmethod
    def get(cls, doc_id, doc_type=None, return_raw=False, **request_params):
        """
        Get one document by id.
        :param doc_id: the document id string to retrieve.
        :param return_raw: if True, return pyelasticsearch response.
        :param request_params: pyelasticsearch request arguments.
        """
        if doc_type is None:
            if cls.doc_type:
                doc_type = cls.doc_type
            else:
                raise ValueError, "No document type specified"
        try:
            doc = cls.connection.get(cls.index_name, doc_type,
                    doc_id, **request_params)
        except ElasticHttpNotFoundError:
            return None
        source = doc[const.SOURCE]
        source[const.ID] = doc[const.ID]
        source[const.TYPE] = doc[const.TYPE]
        if return_raw:
            return source
        return JsonDocument(source)

    @classmethod
    def _has_field(cls, field_name):
        field_name = make_identifier(field_name)
        return hasattr(cls, field_name)

    @classmethod
    def multi_get(cls, doc_ids, doc_type=None, return_raw=False,
            **request_params):
        """
        Get documents by their ids.
        :param doc_ids: list of document id strings to retrieve.
        :param return_raw: if True, return pyelasticsearch response.
        :param request_params: pyelasticsearch request arguments.
        """
        if doc_type is None:
            if cls.doc_type:
                doc_type = cls.doc_type
            else:
                raise ValueError, "No document type specified"
        doc = cls.connection.multi_get(doc_ids, index=cls.index_name,
                doc_type=doc_type, **request_params)
        if return_raw:
            return doc
        result_set = cls.wrap_es_docs(doc[const.DOCS])
        result_set.total = len(result_set.documents)
        return result_set

    @classmethod
    def index(cls, doc, doc_id=None, doc_type=None, **request_params):
        """
        Add one document to the index.
        :param doc: dictionary / JsonDocument to index.
        :param doc_id: string unique id. If it already exists, the document will
            be updated (instead of created).
        :param doc_type: string  document type.
        :param request_params: pyelasticsearch request arguments.
        """
        if isinstance(doc, JsonDocument):
            doc = doc._document
        elif not isinstance(doc, dict):
            raise exceptions.InvalidDocument("Must index a dictionary or \
JsonObject instance")
        if doc_id is None and const.ID in doc:
            doc_id = doc[const.ID]

        update_fields = False
        for field_name in doc.keys():
            if not cls._has_field(field_name):
                update_fields = True

        if doc_type is None:
            if isinstance(cls.doc_type, (str, unicode)):
                doc_type = cls.doc_type
            else:
                raise ValueError, "No document type specified"

        response = cls.connection.index(cls.index_name, doc_type, doc,
                id=doc_id, **request_params)
        if response[const.OK]:
            if update_fields:
                cls.initialize_search_fields(force_reload=True)
            cls.connection.refresh()
            return response[const.ID]
        else:
            raise exceptions.IndexDocumentError("Failed to index doc.\
ES response: " + str(response))

    @classmethod
    def bulk_index(cls, docs, id_field=const.ID, doc_type=None,
            **request_params):
        """
        Add multiple documents of doc_type to index.
        :param doc_type: string document type
        :param docs: list of document dictionaries / JsonObjects
        :param id_field: string key in documents containing document ID.
        :param request_params: pyelasticsearch request arguments.
        """
        if isinstance(docs, (list, set)):
            docs = [ doc._document if isinstance(doc, JsonDocument) else doc
                    for doc in docs ]
        else:
            raise exceptions.InvalidDocument("Must index a list/set of dicts \
or JsonObject instances")
        if doc_type is None:
            if isinstance(cls.doc_type, (str, unicode)):
                doc_type = cls.doc_type
            else:
                raise ValueError, "No document type specified"

        update_fields = False
        field_names = docs[0].keys()
        for field_name in field_names:
            if not cls._has_field(field_name):
                update_fields = True

        response = cls.connection.bulk_index(cls.index_name, doc_type, docs,
                id_field, **request_params)
        items = response[const.ITEMS]
        if not all(item[const.INDEX][const.OK] for item in items):
            raise exceptions.IndexDocumentError("Failed to bulk index docs.\
ES response: " + str(response))

        ids = [ item[const.INDEX][const.ID] for item in items ]
        if update_fields:
            cls.initialize_search_fields(force_reload=True)

        cls.connection.refresh()
        return ids

    @classmethod
    def save(cls, doc_type, doc, doc_id=None, **request_params):
        """
        Alias for index.
        """
        return cls.index(doc_type, doc, id=doc_id, **request_params)

    @classmethod
    def delete(cls, doc_type, doc_id, **request_params):
        """
        Delete one document by its document type and id.
        """

        response = cls.connection.delete(cls.index_name, doc_type, doc_id,
                **request_params)
        if response[const.OK]:
            return True
        else:
            raise exceptions.DeleteDocumentError("Failed to delete doc %s: %s" %
                    (doc_id, str(response)))

    @classmethod
    def delete_all(cls, doc_type, **request_params):
        """
        Delete all documents of a given type.
        """
        response = cls.connection.delete_all(cls.index_name, doc_type,
                **request_params)
        if response[const.OK]:
            return True
        else:
            raise exceptions.DeleteDocumentError("Failed to delete doc type %s:\
%s" % (doc_type, str(response)))

    """Querying"""
    @classmethod
    def query(cls):
        """
        Return a new query object bound to this model.
        """
        return SearchQuery(cls)

    @classmethod
    def search(cls, query, return_raw=False):
        """
        Run one search and return a tuple of (total result count, result data).
        :param query: dict of raw ElasticSearch API query parameters
        :param return_raw: if True, return pyelasticsearch response.
        """
        results = cls.connection.search(query, index=cls.index_name,
                doc_type=cls.doc_type)
        if return_raw:
            return results
        total = results[const.HITS][const.TOTAL]
        hits = results[const.HITS][const.HITS]
        facets = results.get(const.FACETS)
        result_set = cls.wrap_es_docs(hits)
        result_set.total = total
        if facets:
            result_set.facets = JsonDocument(facets)
        return result_set

    @classmethod
    def count(cls, query, **request_params):
        """
        Run one count request with given query, class index and doc type(s).
        :param query: dict of raw ElasticSearch API query parameters
        :param request_params: pyelasticsearch request arguments.
        """
        count = cls.connection.count(query, index=cls.index_name,
                doc_type=cls.doc_type, **request_params)
        return count[const.COUNT]

    @classmethod
    def delete_by_query(cls, doc_type, query, **request_params):
        """
        Delete documents that match the given query.
        :param doc_type: string document type to query against.
        :param query: dictionary of raw ElasticSearch API query parameters
        :param request_params: pyelasticsearch request arguments.
        """
        response = cls.connection.delete_by_query(cls.index_name, doc_type,
                query, **request_params)
        if response[const.OK]:
            return True
        else:
            raise exceptions.DeleteDocumentError("Failed to delete by query %s:\
\n%s" % (query, str(response)))


    """Misc."""
    @classmethod
    def put_mapping(cls, doc_type, mapping, ignore_conflicts=False):
        """
        Add one mapping for the given document type.
        Causes a reload of class field mappings (calling the get_mapping API).
        :param doc_type: string document type to add mapping for.
        :param mapping: dictionary with ElasticSearch field mappings.
        :param ignore_conflicts: if True, new mappings will replace old ones.
        """
        response = cls.connection.put_mapping(cls.index_name, doc_type,
                mapping, ignore_conflicts=ignore_conflicts)
        if response[const.OK]:
            cls.initialize_search_fields(force_reload=True)
            return True
        else:
            raise exceptions.UpdateIndexError("Failed to put mapping: " +
                    str(response))

