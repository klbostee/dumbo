from setuptools import setup, find_packages

setup(
    name = 'dumbo',
    version = '0.21.0',
    author = 'Klaas Bosteels',
    author_email = 'klaas@last.fm',
    license = 'Apache Software License (ASF)',
    packages = find_packages(),
    entry_points = {
        'console_scripts': [
            'dumbo = dumbo:dumbo',
        ]
    },
    zip_safe = True,
    test_suite = 'nose.collector',
    tests_require = ['nose']
)
