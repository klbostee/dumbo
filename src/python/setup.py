from distutils.core import setup

setup(name='dumbo',
      version='0.20.12',
      py_modules=['dumbo'],
      author='Klaas Bosteels',
      author_email='klaas@last.fm',
      license="Apache Software License (ASF)",
      data_files=[('/usr/bin', ['../../bin/dumbo'])]
      )
