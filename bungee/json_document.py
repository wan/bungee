import json

from util import prettify, make_identifier


class ResultSet(object):
    """
    Container class for search results.
    """

    def __init__(self):
        self.documents = []
        self.total = None
        self.facets = {}


class JsonDocument(object):
    """
    Class that recursively "objectifies" a given JSON dictionary.
    """

    def __init__(self, document):

        if isinstance(document, dict):
            self._document = document
        else:
            self._document = json.loads(document)

        def list_helper(ls):
            out = []
            for item in ls:
                if isinstance(item, dict):
                    out.append(JsonDocument(item))
                elif isinstance(item, list):
                    out.append(list_helper(item))
                else:
                    out.append(item)
            return out

        for key, value in self._document.items():
            key = make_identifier(key)
            if isinstance(value, list):
                list_values = list_helper(value)
                setattr(self, key, list_values)
            elif isinstance(value, dict):
                sub_document = JsonDocument(value)
                setattr(self, key, sub_document)
            else:
                setattr(self, key, value)

    def __getattr__(self, key):
        key = make_identifier(key)
        val = self.__dict__.get(key)
        return val

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def __repr__(self):
        return repr(self._document)

    def __unicode__(self):
        data = prettify(self._document)
        return '%s[%d]:\n%s' % (self.__class__.__name__, id(self), data)

    def __str__(self):
        return unicode(self).encode('utf-8')


