import const
from json_document import ResultSet
from util import prettify


class SearchQuery(object):
    """
    Container class for constructing and executing ElasticSearch queries for
    some SeachModel class.

    Each SearchQuery instance may contain a number of query expressions
    (filters, queries, facet fields, sorting expressions and limit/offset
    preferences).

    Query expressions may be chained, e.g.:
        query = Model.query()
        query.filter( field1 == value1 ).filter( field2 == value2).limit(10)

    Each added query expression generates a new SearchQuery instance, so base
    queries may be reused for different purposes, e.g.:
        query = Model.query.filter( field1 == value1)
        query_foo = query.match(field2=="foo") #filter field1, match field2
        query_bar = query.match(field3.like("bar")) #filter field1, match field3
    """

    def __init__(self, search_model_class, must_queries=None,
            must_not_queries=None, should_queries=None, and_filters=None,
            or_filters=None, facet_queries=None, limit=None, offset=0,
            page_size=None, sort=None):

        self.search_model_class = search_model_class
        self.must_queries = must_queries or []
        self.must_not_queries = must_not_queries or []
        self.should_queries = should_queries or []
        self.and_filters = and_filters or []
        self.or_filters = or_filters or []
        self.facet_queries = facet_queries or []
        self._offset = offset
        self._limit = limit
        self._page_size = page_size
        self.sort = sort or []

    def _generate_subquery(self, must_queries=None, must_not_queries=None,
            should_queries=None, and_filters=None, or_filters=None,
            facet_queries=None, limit=None, offset=None, page_size=None,
            sort=None):
        """
        Creates a new query object based on this one, with extra arguments
        appended (or overriden, in the case of limit, offset, page_size, sort).
        """

        # Additive fields
        must_queries = self.must_queries + (must_queries or [])
        must_not_queries = self.must_not_queries + (must_not_queries or [])
        should_queries = self.should_queries + (should_queries or [])
        and_filters = self.and_filters + (and_filters or [])
        or_filters = self.or_filters + (or_filters or [])
        facet_queries = self.facet_queries + (facet_queries or [])
        sort = self.sort + (sort or [])

        # Last added takes precedence
        limit = limit if limit else self._limit
        offset = offset if offset else self._offset
        page_size = page_size if page_size else self._page_size

        return self.__class__(self.search_model_class, must_queries,
                must_not_queries, should_queries, and_filters, or_filters,
                facet_queries, limit, offset, page_size, sort)

    def _generate_es_query(self, count_query=False):
        """
        Create ES query dictionary from stored filters, query expressions and
        sort expressions.

        Note that this dictionary will NOT include limit, offset or any other
        "search api" related settings.

        :param count_query: if True, do not include facet/sort parameters.
        """
        es_dict = {}
        query_arguments = {}
        filter_arguments = {}

        if self.must_queries or self.should_queries or self.must_not_queries:
            match_query = {}
            if self.must_queries:
                match_query[const.MUST] = self.must_queries
            if self.should_queries:
                match_query[const.SHOULD] = self.should_queries
            if self.must_not_queries:
                match_query[const.MUST_NOT] = self.must_not_queries
            query_arguments[const.BOOL] = match_query

        if self.and_filters or self.or_filters:
            if len(self.and_filters):
                filter_arguments = { const.AND: self.and_filters }
            if len(self.or_filters):
                filter_arguments = { const.OR: self.or_filters }

        if query_arguments and filter_arguments:
            es_dict[const.FILTERED] = { const.QUERY: query_arguments }
            es_dict[const.FILTERED][const.FILTER] = filter_arguments
        else:
            if query_arguments:
                es_dict.update(query_arguments)
            elif filter_arguments:
                es_dict[const.FILTERED] = { const.FILTER : filter_arguments }
            else:
                es_dict[const.MATCH_ALL] = {}

        if count_query:
            return es_dict

        es_dict = { const.QUERY: es_dict }
        if self.facet_queries:
            facets = {}
            for field in self.facet_queries:
                facets.update(field)
            es_dict[const.FACETS] = facets

        if self.sort:
            sort = self.sort
        else:
            sort = [{const.ID: { const.ORDER: const.ASC }}]
        es_dict[const.SORT] = sort
#        print 'es_dict', es_dict

        return es_dict

    """
    Sort / limit preferences.
    """
    def offset(self, amount):
        """
        Set the result offset (in number of documents from start).
        """
        return self._generate_subquery(offset=amount)

    def limit(self, amount):
        """
        Set the maximum number of documents returned by this query.
        """
        return self._generate_subquery(limit=amount)

    def order_by(self, sort_expr):
        """
        Add a sort order; sorting precedent is sequential, with each subsequent
        sort breaking ties from the previous.
        """
        return self._generate_subquery(sort=sort_expr)

    def page_size(self, amount):
        """
        Set the number of documents to return with each network request.
        """
        return self._generate_subquery(page_size=amount)

    """
    Filtering: The presence of one of these expressions will generate a
    "filtered" query on execution.
    """
    def filter_and(self, query_expression):
        """
        Add AND filter expression.
        """
        return self._generate_subquery(and_filters=[query_expression])

    def filter_or(self, query_expression):
        """
        Add OR filter expression.
        """
        return self._generate_subquery(or_filters=[query_expression])

    def filter(self, query_expression):
        """
        Alias for filter_and
        """
        return self.filter_and(query_expression)

    """
    Matching: The presence of on of these expressions will generate a "bool"
    query on execution.
    """
    def match(self, query_expression):
        """
        Alias for must_match.
        """
        return self.must_match(query_expression)

    def must_match(self, query_expression):
        """
        Add query expression that documents MUST match.
        """
        return self._generate_subquery(must_queries=[query_expression])

    def should_match(self, query_expression):
        """
        Add query expression that documents SHOULD match (mostly effects
        document scoring).
        """
        return self._generate_subquery(should_queries=[query_expression])

    def must_not_match(self, query_expression):
        """
        Add query expression that documents MUST NOT match.
        """
        return self._generate_subquery(must_not_queries=[query_expression])
 
    def _add_facet(self, facet_type, search_field, facet_name=None,
            facet_filters=None):
        if not facet_name:
            facet_name = search_field.hierarchy
        q = { facet_name: { 
            facet_type: { const.FIELD: search_field.hierarchy }
        }}

        if facet_filters:
            facet_filter_dict = {}
            for facet_filter in facet_filters:
                facet_filter_dict.update(facet_filter)
            q[facet_name][const.FACET_FILTER] = facet_filter_dict
        return q

    """Faceting."""
    def term_facet(self, search_field, facet_name=None, facet_filters=None,
            facet_size=None):
        """
        Add a term facet.
        :param search_field: SearchField to count distinct values for
        :param facet_name: string, optional name for the facet result
            (will default to the full name of the search field)
        :param facet_filters: list of QueryExpressions to filter facet results
        """
        q = self._add_facet(const.TERMS, search_field,
            facet_name=facet_name, facet_filters=facet_filters)
        return self._generate_subquery(facet_queries=[q])

    """Query execution."""
    def count(self, **request_params):
        """
        Fetch the number of matching documents with the ES count API. See ES
        documentation for supported request_params.
        """
        es_query = self._generate_es_query(count_query=True)
        return self.search_model_class.count(es_query, **request_params)

    def all(self):
        """
        Fetch all documents for given query and return a tuple containing
        (total document count, document sources).

        Queries will be executed one <page_size> at a time, until there are no
        more documents, or a specific <limit> has been reached.
        """
        es_query = self._generate_es_query()

        page_size = self._page_size or const.DEFAULT_PAGE_SIZE
        es_query[const.SIZE] = page_size

        offset = 0
        if self._offset:
            offset = self._offset * page_size

        es_query[const.FROM] = offset
        start = 0
        done = False
        unique_ids = set()

        result_set = ResultSet()

        while True:
            es_query[const.FROM] = start * page_size
            results = self.search_model_class.search(es_query, return_raw=False)

            total = results.total
            if result_set.total is None:
                result_set.total = total

            for document in results.documents:
                if document._id not in unique_ids:
                    unique_ids.add(document._id)
                    result_set.documents.append(document)
                if self._limit:
                    if len(unique_ids) >= self._limit:
                        done = True
                        break
            if results.facets and not result_set.facets:
                result_set.facets = results.facets

            if done:
                break
            if len(results.documents) == 0:
                break
            if total <= (page_size * (start+1)):
                break

            start += 1

        return result_set

    def delete(cls, **request_params):
        """
        Delete all documents that match this query.
        """
        es_query = self._generate_es_query()
        return cls.search_model_class.delete_by_query(doc_type, es_query,
                **request_params)

    def __repr__(self):
        return "SearchQuery:[\n%s\n]" % self._generate_es_query()

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return repr(self)

