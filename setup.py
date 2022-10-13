"""Setup file for ArchFX Broker data-tools package."""

from distutils.util import convert_path
from setuptools import setup, find_packages

version = {}


with open(convert_path("cfx_recorder/version.py")) as fp:
    exec(fp.read(), version)

setup(
    name="cfx-recorder",
    packages=find_packages(exclude=("test",)),
    version=version["__version__"],
    license="proprietary",
    install_requires=[
        "amqp>=5.0.0,<6.0.0",
        "python-dateutil>=2.8.0,<3"
    ],
    package_data={
        'cfx_recorder': ['cfx_recorder/py.typed'],
    },
    entry_points={
        'console_scripts': [
            'cfx-recorder = cfx_recorder.scripts.cfx_recorder:main'
        ]
    },
    include_package_data=True,
    description="Administrative tools and utilities for managing data on the ArchFX Broker",
    author="Arch",
    author_email="info@archsys.io",
    url="https://github.com/iotile/archfx_broker/src/data_tools",
    keywords=["iotile", "arch", "industrial"],
    python_requires=">=3.7, <4",
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules"
        ],
    long_description="""\
ArchFX Broker Data Tools
--------------------------

Python based classes and tools for inspecting and exporting data stored on the archfx broker.

See https://www.archsys.io
"""
)
