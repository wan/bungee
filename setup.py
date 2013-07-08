from setuptools import setup, find_packages

setup(
        name='bungee',
        version='0.1',
        author='Andrew Wan',
        author_email='andrew@thatsnumberwan.com',
        description='Python models and query expressions for ElasticSearch',
        url='https://github.com/wan/bungee',
        packages=find_packages(),
        license='BSD',
        install_requires=['pyelasticsearch>=0.5'],
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python',
            'Operating System :: OS Independent',
            'Topic :: Internet :: WWW/HTTP :: Indexing/Search'
        ],
        tests_require=["nose"]
)

