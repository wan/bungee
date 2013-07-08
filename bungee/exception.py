class SearchModelException(Exception):
    pass

class ConfigError(SearchModelException):
    pass

class InvalidQueryExpression(SearchModelException):
    pass

class InvalidDocument(SearchModelException):
    pass

class IndexDocumentError(SearchModelException):
    pass

class DeleteDocumentError(SearchModelException):
    pass

class UpdateIndexError(SearchModelException):
    pass

