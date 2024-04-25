#!/usr/bin/env python
import re
import sys

if sys.version_info < (3, 8):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)


try:
    from setuptools import find_namespace_packages
except ImportError:
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" and try again')
    sys.exit(1)


from pathlib import Path
from setuptools import setup


# pull the long description from the README
README = Path(__file__).parent / "README.md"


# used for this adapter's version and in determining the compatible dbt-core version
VERSION = Path(__file__).parent / "dbt/adapters/redshift/__version__.py"


def _plugin_version() -> str:
    """
    Pull the package version from the main package version file
    """
    attributes = {}
    exec(VERSION.read_text(), attributes)
    return attributes["version"]


def _plugin_version_trim() -> str:
    """
    Pull the package version from the main package version file
    """
    attributes = {}
    exec(VERSION.read_text(), attributes)
    pattern = r"\+build\d+$"
    return re.sub(pattern, "", attributes["version"])


setup(
    name="dbt-redshift",
    version=_plugin_version(),
    description="The Redshift adapter plugin for dbt",
    long_description=README.read_text(),
    long_description_content_type="text/markdown",
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-redshift",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-common>=0.1.0a1,<2.0",
        "dbt-adapters>=0.1.0a1,<2.0",
        f"dbt-postgres~={_plugin_version_trim()}",
        # dbt-redshift depends deeply on this package. it does not follow SemVer, therefore there have been breaking changes in previous patch releases
        # Pin to the patch or minor version, and bump in each new minor version of dbt-redshift.
        "redshift-connector<2.0.918,>=2.0.913,!=2.0.914",
        # add dbt-core to ensure backwards compatibility of installation, this is not a functional dependency
        "dbt-core>=1.8.0b3",
        # installed via dbt-core but referenced directly; don't pin to avoid version conflicts with dbt-core
        "sqlparse>=0.5.0,<0.6.0",
        "agate",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
