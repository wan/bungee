from bungee.tests import BungeeTestCase
from bungee.field import SearchField


class ModelTestCase(BungeeTestCase):

    def setUp(self):
        super(ModelTestCase, self).setUp()

    def test_index_get(self):
        for book in self.books:
            _id = self.model.index(book, doc_type='book')
            self.assertEqual(_id, str(book['_id']))
        self.assertRaises(ValueError, self.model.index, (self.books[0]))

        self.assertRaises(ValueError, self.model.get, ('A'))
        book = self.model.get('A', doc_type='book')
        self.assertEqual(book._id, 'A')
        self.assertEqual(book.title, 'Heart of Darkness')
        self.assertEqual(book.author.last, 'Conrad')
        self.assertEqual(book.published, '1900-07-01')

        book = self.model.get('C', doc_type='book')
        self.assertEqual(book._id, 'C')
        self.assertEqual(book.title, 'Infinite Jest')

    def test_bulk_index_multi_get(self):
        ids = self.model.bulk_index(self.books, doc_type='book')
        self.assertEqual(set(ids), set(book['_id'] for book in self.books))
        self.assertRaises(ValueError, self.model.bulk_index, (self.books))

        self.assertRaises(ValueError, self.model.multi_get, (['A','B']))
        results = self.model.multi_get(['A','B'], doc_type='book')
        docs = results.documents
        self.assertEqual(results.total, 2)

        self.assertEqual(docs[0]._id, 'A')
        self.assertEqual(docs[0].title, 'Heart of Darkness')
        self.assertEqual(docs[0].author.last, 'Conrad')
        self.assertEqual(docs[1]._id, 'B')
        self.assertEqual(docs[1].title, 'Catch-22')
        self.assertEqual(docs[1].author.last, 'Heller')

    def test_put_mapping(self):
        self.model.put_mapping('book', self.multi_field_mapping,
                ignore_conflicts=True)
        self.model.bulk_index(self.books, doc_type='book')
        self.assertIsInstance(self.model.title.untouched, SearchField)
        self.assertIsInstance(self.model.title, SearchField)

    def test_delete(self):
        self.model.bulk_index(self.books, doc_type='book')
        self.model.delete('book', 'A')
        self.assertIsNone(self.model.get('A', doc_type='book'))

        self.model.delete_all('book')
        self.assertIsNone(self.model.get('B', doc_type='book'))
        self.assertIsNone(self.model.get('C', doc_type='book'))

