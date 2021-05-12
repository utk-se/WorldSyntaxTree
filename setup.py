#!/usr/bin/env python

from setuptools import setup, find_packages

import wsyntree as mainmodule

with open("README.md", "r") as f:
    readme = f.read()

setup(
    name='WorldSyntaxTree',
    version=mainmodule.__version__,
    description="WorldSyntaxTree common library",
    long_description=readme,
    long_description_content_type="text/markdown",
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='utk research',
    author='Ben Klein, Aiden Rutter',
    author_email='bklein3@vols.utk.edu',
    url='https://github.com/utk-se/WorldSyntaxTree',
    # license='MIT',
    # dependency_links=[
    #     # 'https://github.com/utk-se/py-tree-sitter/tarball/master#egg=tree_sitter',
    #     "git+https://github.com/utk-se/py-tree-sitter.git@master#egg=tree_sitter_utk",
    # ],
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests', 'vendor', 'venv']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "python-arango",
        "coloredlogs",
        "pygit2",
        # "tree_sitter",
        "tree_sitter@git+https://github.com/utk-se/py-tree-sitter.git@master",
        "tqdm",
        "Pebble",
        "filelock",
        "cachetools",
        "enlighten",
    ],
    entry_points={
        'console_scripts': [
            'wsyntree-collector=wsyntree_collector.__main__:__main__',
            'wsyntree-selector=wsyntree_selector.__main__:__main__'
        ]
    },
    python_requires='>=3.8'
)
