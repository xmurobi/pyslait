#!/usr/bin/env python

import ast
import re
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('pyslait/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


with open('README.md') as readme_file:
    README = readme_file.read()


setup(
    name='pyslait',
    version=version,
    description='Slait python driver',
    long_description=README,
    long_description_content_type='text/markdown',
    author='robi',
    author_email='xmurobi@gmail.com',
    url='https://github.com/xmurobi/pyslait',
    keywords='database,pandas,financial,timeseries',
    packages=['pyslait', ],
    install_requires=[
        'numpy',
        'requests',
        'pandas',
        'six',
        'urllib3',
        'pytest',
        'websocket-client',
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'coverage>=4.4.1',
        'mock>=1.0.1'
    ],
    setup_requires=['pytest-runner', 'flake8'],
)
