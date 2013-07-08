"""
This module contains constains and classes for generating objects that represent
ElasticSearch document fields. Comparisons and function calls on these Fields
evaluate to JSON, which can be used to generate JSON query expressions.
"""
import const
import exception
from util import make_identifier

MATCH = 'match'
MATCH_WILDCARD = 'wildcard'
MATCH_QUERY_STRING = 'query_string'
MATCH_REGEXP = 'regexp'
VALUE = 'value'
BOOST = 'boost'
FLAGS = 'flags'
ANALYZE_WILDCARD = 'analyze_wildcard'

FILTER_TERM = 'term'
FILTER_TERMS = 'terms'
FILTER_EXISTS = 'exists'
FILTER_MISSING = 'missing'
FILTER_RANGE = 'range'
FILTER_NOT = 'not'
FILTER_GT = 'gt'
FILTER_GTE = 'gte'
FILTER_LT = 'lt'
FILTER_LTE = 'lte'
FILTER_FROM = 'from'
FILTER_TO = 'to'
FILTER_FIELD = 'field'

RESERVED_PROPERTIES = set((
    '_is_parent',
    '_is_multi_field',
    '_mapping',
    '_search_model',
    '_mapping_name',
    '_parent',
    '_field_name'))


def not_(query_expression):
    """
    Filter out given query expression.
    """
    return { FILTER_NOT: query_expression }


def range_filter(query_expression):
    return { FILTER_RANGE: query_expression }


def query_expression(func):
    """
    Ensure that the given operator is applied to a field that has no
    submappings. multi_field mappings are still valid.
    """
    def wrap(self, *args, **kwargs):
        if self._is_parent:
            mapped_subfields = self._mapping[const.PROPERTIES].keys()
            raise exception.InvalidQueryExpression, "Cannot create query \
expressions on fields with nested subtypes. Submapped fields include %s" % (
                    str(mapped_subfields))
        if len(args):
            # TODO: seems questionable, but makes datetime stuff easier
            args = map(str, args)
        return func(self, *args, **kwargs)
    return wrap


class SearchField(object):
    """
    SearchField provides access to object values, and an interface
    for query DSL operators.
    """

    def __init__(self, field_name, mapping, parent=None, search_model=None):
        self._is_parent = const.PROPERTIES in mapping
        self._is_multi_field = (const.PROPERTY_TYPE in mapping
                and mapping[const.PROPERTY_TYPE] == const.MAPPING_MULTI_FIELD)
        self._search_model = search_model
        self._parent = parent
        self._mapping = mapping
        self._field_name = make_identifier(field_name)
        self._mapping_name = field_name

    def __getattr__(self, field):
        """
        Overriding to support multi_field and nested subdocuments mappings.
        """
        if field in RESERVED_PROPERTIES:
            return getattr(super(SearchField, self), field)
        if self._is_multi_field:
            if field in self._mapping[const.FIELDS]:
                return self.__class__(field,
                        self._mapping[const.FIELDS][field],
                        self, self._search_model)
        elif self._is_parent:
            mapping_props = self._mapping[const.PROPERTIES]
            if field in mapping_props:
                prop = self.__class__(field, mapping_props[field],
                    self, self._search_model)
                return prop
        return getattr(super(SearchField, self), field)

    @property
    def hierarchy(self):
        """
        Return the concatenated names of this field and all its ancestors.
        """
        prop = self
        fields = [prop._mapping_name]
        while prop._parent:
            fields.append(prop._parent._mapping_name)
            prop = prop._parent
        fields.reverse()
        return '.'.join(fields)

    def __repr__(self):
        return "SearchField[%s]" % self.hierarchy

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return repr(self)

    """
    Filter-type Expressions
    """
    @query_expression
    def __eq__(self, rhs):
        """
        Adds a term query (unanalyzed).
        """
        return { FILTER_TERM: { self.hierarchy: rhs } }

    @query_expression
    def __ne__(self, rhs):
        """
        Adds a negative term query (unanalyzed).
        """
        return not_(self.__eq__(rhs))

    @query_expression
    def in_(self, rhs):
        rhs = map(str, rhs)
        return { FILTER_TERMS: { self.hierarchy: rhs } }

    @query_expression
    def __ge__(self, rhs):
        q = { self.hierarchy: { FILTER_GTE: rhs } }
        return range_filter(q)

    @query_expression
    def __gt__(self, rhs):
        q = { self.hierarchy: { FILTER_GT: rhs } }
        return range_filter(q)

    @query_expression
    def __le__(self, rhs):
        q = { self.hierarchy: { FILTER_LTE: rhs } }
        return range_filter(q)

    @query_expression
    def __lt__(self, rhs):
        q = { self.hierarchy: { FILTER_LT: rhs } }
        return range_filter(q)

    @query_expression
    def range(self, lhs, rhs):
        q = { self.hierarchy: { FILTER_FROM: lhs, FILTER_TO: rhs}}
        return range_filter(q)

    @query_expression
    def exists(self):
        return { FILTER_EXISTS: { FILTER_FIELD: self.hierarchy }}

    @query_expression
    def missing(self):
        return { FILTER_MISSING: { FILTER_FIELD: self.hierarchy }}

    """Matching"""
    @query_expression
    def like(self, rhs, wildcard=True, **es_query_string_params):
        """
        Adds a "query_string" query on this field.
        :params rhs: string to match against
        :param wildcard: if True, analyze wildcard characters (*, ?)
        :param es_query_params: dictionary of any ElasticSearch parameters for
            the query_string query. See ES documentation.
        """
        q = { const.FIELDS: [ self.hierarchy ],
            const.QUERY: rhs,
            ANALYZE_WILDCARD: wildcard }
        q.update(es_query_string_params)
        return { MATCH_QUERY_STRING: q }

    @query_expression
    def regexp(self, pattern, boost=None, flags=None):
        """
        Adds a regexp query on this field.
        :param pattern: string regular expression to query with
        :param boost: number that multiplies scoring weight of query
        :param flags: Lucene regexp field flags, joined by "|"
        """
        q = { VALUE: pattern }
        if boost:
            q[BOOST] = boost
        if flags:
            q[FLAGS] = flags
        return { MATCH_REGEXP:  { self.hierarchy : q } }

    """Order By"""
    @query_expression
    def desc(self):
        """
        Sort by this field, in desc order.

        Special case for id: must sort on "_uid" instead
        """
        field_name = self.hierarchy
        if field_name == const.ID:
            field_name = const.UID
        return [{field_name: {const.ORDER: const.DESC}}]

    @query_expression
    def asc(self):
        """
        Sort by this field, in asc order.

        Special case for id: must sort on "_uid" instead
        """
        field_name = self.hierarchy
        if field_name == const.ID:
            field_name = const.UID
        return [{field_name: {const.ORDER: const.ASC}}]

