from setuptools import setup
import os, io

from onetsdb import __version__

here = os.path.abspath(os.path.dirname(__file__))
README = io.open(os.path.join(here, 'README.md'), encoding='UTF-8').read()
CHANGES = io.open(os.path.join(here, 'CHANGES.md'), encoding='UTF-8').read()
setup(name="onetsdb",
      version=__version__,
      keywords=('TSDB', 'MongoDB', 'InfluxDB', 'NoSQL', 'Sqlite3', 'Time Series Database'),
      description="A Uniform Interface for Timeseries Database Python Library.",
      long_description=README + '\n\n\n' + CHANGES,
      long_description_content_type="text/markdown",
      url='https://github.com/sintrb/onetsdb/',
      author="trb",
      author_email="sintrb@gmail.com",
      packages=['onetsdb'],
      install_requires=['six'],
      zip_safe=False
      )
