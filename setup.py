import os

import setuptools

HERE = os.path.abspath(os.path.dirname(__file__))

classifiers = [
    "Development Status :: 1 - Planning",
    "Intended Audience :: Academics & Developers",
    "License :: OSI Approved :: MIT License",
    "Natural Language :: English",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Scientific/Engineering :: Process Mining",
]

packages = ['feldspar']

requires = [
    "filetype==1.0.6",
    "lxml==4.9.1",
    "python-dateutil==2.8.1"
]

test_requirements = [
    'pytest>=3',
]

about = {}
with open(os.path.join(HERE, 'feldspar', '__version__.py'), 'r') as f:
    exec(f.read(), about)

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    license=about['__license__'],

    packages=packages,
    package_data={'': ['LICENSE']},

    include_package_data=True,
    install_requires=requires,

    classifiers=classifiers,
    python_requires=">=3.6",
    project_urls={
        'Source': 'https://github.com/xcavation/feldspar',
    },
)
