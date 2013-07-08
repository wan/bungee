import unittest

from bungee.tests import BungeeTestCase
from bungee.field import SearchField


class QueryExpressionTestCase(BungeeTestCase):

    def setUp(self):
        super(QueryExpressionTestCase, self).setUp()

    def test_querying(self):
        ids = self.model.bulk_index(self.books, doc_type='book')
        q = self.model.query()
        q = q.should_match(self.model.title.like('heart'))
        results = q.all()
        docs = results.documents
        self.assertTrue(len(docs) == 1)
        self.assertTrue(results.total == 1)
        self.assertTrue(docs[0].author.first == 'Joseph')
        self.assertTrue(docs[0].author.last == 'Conrad')

        q = self.model.query()
        q = q.filter(self.model.author.born > '1923-05-01')
        results = q.all()
        docs = results.documents
        self.assertTrue(results.total == 1)
        self.assertTrue(docs[0]._id == 'C')

        q = self.model.query()
        q = q.filter(self.model.author.born >= '1923-05-01')
        results = q.all()
        self.assertTrue(results.total == 2)

        doc_ids = [ doc._id for doc in results.documents ]
        self.assertTrue(set(doc_ids), set(['B', 'C']))

    def test_multi_field_querying(self):
        self.model.put_mapping('book', self.multi_field_mapping,
                ignore_conflicts=True)
        ids = self.model.bulk_index(self.books, doc_type='book')

        q = self.model.query()
        q = q.filter(self.model.title.untouched=='Catch-22')
        results = q.all()
        self.assertTrue(results.total == 1)
        self.assertTrue(results.documents[0]._id == 'B')

        count = q.count()
        self.assertEqual(count, 1)

        q = self.model.query()
        q = q.filter(self.model.title=='Catch-22')
        results = q.all()
        self.assertTrue(results.total == 0)

        count = q.count()
        self.assertEqual(count, 0)

    def test_order(self):
        ids = self.model.bulk_index(self.books, doc_type='book')
        q = self.model.query()
        q = q.order_by(self.model._id.asc())
        results = q.all()
        docs = results.documents
        self.assertTrue(docs[0]._id < docs[1]._id < docs[2]._id)

        q = self.model.query()
        q = q.order_by(self.model._id.desc())
        results = q.all()
        docs = results.documents
        self.assertTrue(docs[0]._id > docs[1]._id > docs[2]._id)

        count = q.count()
        self.assertEqual(count, 3)

    def test_faceting(self):
        ids = self.model.bulk_index(self.books, doc_type='book')
        q = self.model.query()

        q = q.term_facet(self.model.author.first)
        results = q.all()
        stats = results.facets['author.first'].terms
        self.assertEqual(stats[0].count, 2)
        self.assertEqual(stats[0].term, 'joseph')
        self.assertEqual(len(stats), 2)

        self.assertEqual(results.facets.author_first.terms, stats)

        q = self.model.query()
        q = q.term_facet(self.model.author.first,
                'first_name',
                facet_filters=[self.model.author.last == 'conrad'])
        results = q.all()
        stats = results.facets.first_name.terms
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0].count, 1)
        self.assertEqual(stats[0].term, 'joseph')

        count = q.count()
        self.assertEqual(count, 3)

