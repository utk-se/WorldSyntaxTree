from setuptools import setup, find_packages

import wsyntree as mainmodule

with open("README.md", "r") as f:
    readme = f.read()

setup(
    name='WorldSyntaxTree Lib',
    version=mainmodule.__version__,
    description="WorldSyntaxTree common library",
    long_description=readme,
    long_description_content_type="text/markdown",
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='utk research',
    author='Ben Klein, Aiden Rutter',
    author_email='bklein3@vols.utk.edu',
    url='',
    # license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "coloredlogs",
        "pygit2",
        "tree_sitter"
    ],
    # entry_points={
    #     'console_scripts': [
    #         'wsyntree-test=tests.test'
    #     ]
    # },
    python_requires='>=3.6'
)
