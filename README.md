Bungee
======

Python models and query expressions for ElasticSearch built on pyelasticsearch.

This software is **VERY** alpha, and does not directly support many ElasticSearch features.

Installation
------------
First things first: You'll need a running ElasticSearch server! See elasticsearch.org

With pip:

    $ pip install git+https://github.com/wan/bungee


Or from source:

    $ git clone git://github.com/wan/bungee
    $ cd bungee
    $ python setup.py install


Usage
-----
Basic usage of Bungee includes implementing a SearchModel class with and index name. Optionally, you may specify a document type (or list of document types) and a connection url or urls (defaults to localhost:9200)
    
```python
    from bungee import SearchModel

    class Book(SearchModel):
        index_name = 'example'
        doc_type = 'book' # optional; if left out, you'll have to specify doc_type in calls to index, get, multi_get, etc.
```

Load JSON documents (dictionaries) with the index() function:

```python
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
    for book in books:
        document_id = Book.index(book)
```

Or bulk index:
```python
    Book.bulk_index(books)
```
    	
Build queries with the query() object:

```python
    q = Book.query()
    q = q.filter(Book.published > '1900-01-01')
    q = q.should_match(Book.author.first.like('joseph'))
    q = q.order_by(Book.published.asc())
    q = q.term_facet(Book.published, facet_name='publish_date')
```
    
Execute queries with all(), or count() to get the result count (with no other data):

```python
    results = q.all()
    print 'Found %s documents' % results.total
    for document in results.documents:
        print document._id, document.date
    for facet in results.facets.publish_data.terms:
        print facet.term, facet.count
    
    # alternatively, use count() to get total
    total_books_matched = q.count()
```
    	
To access other features, or issue your own custom queries, you can exectue any query via the SearchModel class "execute" function:

```python
    query = {'more_like_this': { ... }}
    results = Book.execute(query)
```
    
Or if you really need it, a pyelasticsearch connection object, e.g.:

```python
    connection = Book.connection
    cluster_state = connection.cluster_state()
```

Notes
-----
- Mapped ElasticSearch field names will be converted to Python identifiers.
For example, given the mapping:
```json
    { user: {
        properties: {
            user name: { type: string }
        }
    }
```
The "user name" field will be accessible as "user_name" in the model class:
```python
    query = User.query().filter(User.user_name == "Bob")
```

- Supported APIs include search, indexing, bulk indexing, delete, count, facet and put_mapping; the rest can generally be accessed through the connection object directly, e.g.:
```python
    User.connection.get_status()
```

- Connections to a given URL are pooled globally per process.

TODO
----
- Much more functional test coverage
- Support for nested mapping/queries?
- Support for percolate
- Support for settings API, various query analyzers
- Support for more queries, filters (especially geospatial)
- Add Sphinx documentation

License
-------
This software is licensed under the **Simplified BSD License**. See the LICENSE file in the top directory for the full license text.