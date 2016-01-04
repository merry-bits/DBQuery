# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="DBQuery",
    version="0.3.1",
    description="Simplify your database access.",
    long_description=long_description,
    url="https://github.com/merry-bits/DBQuery",
    author="merry-bits",
    author_email="merry-bits@users.noreply.github.com",
    license="GPLv2",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
        "Natural Language :: English",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
    ],
    keywords=(
        "easy database access SQLite PostgreSQL wrap SQL connection query"),
    packages=find_packages("src"),
    package_dir = {"": "src"},
    install_requires=[],
    # $ pip install -e .[postgres,test]
    extras_require={
        "postgres": ["psycopg2>=2.4.2"],
    },
)
