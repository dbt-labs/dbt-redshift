# dbt-tests-adapter

This is where we store the adapter tests that will be used by
plugin repos. It should be included in the dbt-core and plugin
repos via git-subrepo or by installing using pip. Changes in this
plugin will be stored in a separate dbt-tests-adapter repository 
at https://github.com/dbt-labs/dbt-tests-adapter.

Tests in this repo will be packaged as classes, with a base class
that can be imported by adapter test repositories. Tests that might
need to be overridden by a plugin will be separated into separate
test methods or test classes as they are discovered.

This plugin is installed in the dbt-core repo by pip install -e tests/adapter,
which is included in the editable-requirements.txt file.

The dbt.tests.adapter.basic tests originally came from the earlier
dbt-adapter-tests repository. Additional test directories will be
added as they are converted from dbt-core integration tests, so that
they can be used in adapter test suites without copying and pasting.

Documentation of git-subrepo can be found at https://github.com/ingydotnet/git-subrepo
and https://github.com/ingydotnet/git-subrepo/wiki. Using git-subrepo in
adapter repositories is not required.

This is packaged as a plugin using a python namespace package so it
cannot have an __init__.py file in the part of the hierarchy to which it
needs to be attached.
