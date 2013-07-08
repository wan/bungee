"""
bungee tests

These require an ElasticSearch server running on localhost:9200.
"""
import unittest

from bungee import SearchModel

from pyelasticsearch import ElasticSearch


class BungeeTestCase(unittest.TestCase):

    books = [
        {   '_id': 'A',
            'title': 'Heart of Darkness',
            'author': {
                'first': 'Joseph',
                'last': 'Conrad',
                'born': '1857-12-03'
            },
            'published': '1900-07-01',
            'pages': 72
        },
        {   '_id': 'B',
            'title': 'Catch-22',
            'author': {
                'first': 'Joseph',
                'last': 'Heller',
                'born': '1923-05-01'},
            'published': '1961-11-11',
            'pages': 453
        },
        {   '_id': 'C',
            'title': 'Infinite Jest',
            'author': {
                'first': 'David',
                'last': 'Wallace',
                'born': '1962-02-21'},
            'published': '1996-02-01',
            'pages': 515
        }
    ]

    multi_field_mapping = {
        'book': {
            'properties': {
                'title': {
                    'type': 'multi_field',
                    'fields': {
                        'title': { 'type': 'string' },
                        'untouched': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'omit_norms': True,
                            'index_options': 'docs',
                            'type': 'string'
                        }
                    }
                },
                'author': {
                    'properties': {
                        'first': { 'type': 'string' },
                        'last': { 'type': 'string' },
                        'born': { 'type': 'date', 'format': 'YYYY-MM-dd' }
                    }
                },
                'year': { 'type': 'date', 'format': 'YYYY-MM-dd' },
                'pages': { 'type': 'integer' }
            }
        }
    }

#    Figure out if / how to support this
#    nested_mapping = {
#        'book': {
#            'properties': {
#                'title': { 'type': 'string' },
#                'author': { 'type': 'nested' },
#                'year': { 'type': 'date', 'format': 'YYYY-MM-dd' },
#                'pages': { 'type': 'integer' }
#            }
#        }
#    }

    def setUp(self):
        es_connection = ElasticSearch('http://localhost:9200')
        try:
            es_connection.delete_index('unit_tests')
        except:
            pass
        es_connection.create_index('unit_tests')

        class TestModel(SearchModel):
            index_name = 'unit_tests'

        self.model = TestModel

    def tearDown(self):
        try:
            self.model.connection.delete_index(self.model.index_name)
            self.model.delete_field_mappings()
        except:
            pass

