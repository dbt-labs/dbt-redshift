#!/usr/bin/env python
import sys
from pathlib import Path
from setuptools import setup

from dbt.adapters.redshift.__version__ import version


if sys.version_info < (3, 7):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.7 or higher.")
    sys.exit(1)


try:
    from setuptools import find_namespace_packages
except ImportError:
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" and try again')
    sys.exit(1)


# require a compatible minor version (~=) and prerelease if this is a prerelease
def dbt_core_version():
    try:
        major, minor, plugin_patch = version.split(".")
    except ValueError:
        raise ValueError(f"Invalid version: {version}")

    pre_release_phase = "".join([i for i in plugin_patch if not i.isdigit()])
    if pre_release_phase:
        if pre_release_phase not in ["a", "b", "rc"]:
            raise ValueError(f"Invalid version: {version}")
        core_patch = f"0{pre_release_phase}1"
    else:
        core_patch = "0"

    return f"{major}.{minor}.{core_patch}"


setup(
    name="dbt-redshift",
    version=version,
    description="The Redshift adapter plugin for dbt",
    long_description=Path(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-redshift",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        f"dbt-core~={dbt_core_version()}",
        f"dbt-postgres~={dbt_core_version()}",
        "boto3>=1.4.4,<2.0.0",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
)
