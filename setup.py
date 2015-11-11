# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="DBaccess",
    version="0.3.0.dev1",
    description="Simplify your database access.",
    long_description=long_description,
    url="https://github.com/merry-bits/DBaccess",
    author="merry-bits",
    author_email="merry-bits@users.noreply.github.com",
    license="GPLv2",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Programming Language :: Python :: 3.5",
    ],
    keywords="easy database access SQLite PostgreSQL wrap SQL connection",
    packages=find_packages("src"),
    package_dir = {"": "src"},
    install_requires=[],
    # $ pip install -e .[postgres,test]
    extras_require={
        "postgres": ["psycopg2"],
    },
)