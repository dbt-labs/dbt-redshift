"""Capture logbook log messages during tests.

There are three logbook.TestHandlers used by this plugin: one for test
setup, one for test execution and one for test teardown.  Initially
the setup instance gets installed during the session start to catch
log messages emitted during early setup, session fixtures etc.

Does not (yet) capture log records during conftest loading or test
collection.
"""

import contextlib
import logging

import logbook
import pytest


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addini(
        name='logbook_stdlib',
        help='redirect stdlib logging to logbook [true]',
        default='true',
    )


def pytest_configure(config):
    """Create the actual plugin and register it under a known name.

    This allows us to expose the plugin instance itself as a public
    API.
    """
    logman = LogbookManager()
    config.pluginmanager.register(logman, 'logbook')


class LogbookManager:
    """The manager for logbook record capturing."""

    @contextlib.contextmanager
    def capturing(self, item, when):
        """Context manager to enable capturing.

        The core of the plugin.  This uses the handlers which are
        attached to the item by our pytest_runtest_setup hook.  It
        enables log capturing while inside the context manager.
        """
        handler = getattr(item, 'logbook_handler_' + when)
        stdlibredir = item.config.getini('logbook_stdlib') == 'true'
        if stdlibredir:
            orig_logging_handlers = logging.root.handlers[:]
            orig_logging_level = logging.root.level
            del logging.root.handlers[:]
            redir_handler = logbook.compat.RedirectLoggingHandler()
            logging.root.addHandler(redir_handler)
            logging.root.setLevel(logging.NOTSET)
        try:
            with handler.applicationbound():
                yield handler
        except AssertionError:
            item.add_report_section(
                when,
                'logbook-error',
                'Application corrupted the logbook appstack.\n'
                '\n'
                'Make sure for each .push_application() call you also\n'
                'call .pop_application().',
            )
            item.warn('logbook',
                      'Application corrupted the logbook appstack')
        if handler.records:
            item.add_report_section(when, 'logbook',
                                    '\n'.join(handler.formatted_records))
        if stdlibredir:
            logging.root.setLevel(orig_logging_level)
            logging.root.handlers[:] = orig_logging_handlers

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item):
        """Enable logbook capturing during test setup.

        This needs to prepare all the handlers up front so that the
        fixtures, which are executed during the setup, can find their
        respective handlers.
        """
        for when in ['setup', 'call', 'teardown']:
            handler = logbook.TestHandler()
            setattr(item, 'logbook_handler_' + when, handler)
        with self.capturing(item, 'setup'):
            yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item):
        """Enable logbook capturing during test call."""
        with self.capturing(item, 'call'):
            yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_teardown(self, item):
        """Enable logbook capturing during test teardown."""
        with self.capturing(item, 'teardown'):
            yield


@pytest.fixture
def loghandler(request):
    """Access to the registered TestHandler instance during item call.

    This returns the logbook.TestHandler instance in use during the
    execution of the test itself.  It will contain log records emitted
    during the test execution but not any emitted during test setup or
    teardown.
    """
    return request.node.logbook_handler_call


@pytest.fixture
def loghandler_setup(request):
    """Access to the registerd TestHandler instance for item setup.

    This returns the logbook.TestHandler instance in use during
    fixture setup.  It will contain the log records emitted during
    item setup only, e.g. from other fixtures.
    """
    return request.node.logbook_handler_setup


@pytest.fixture
def loghandler_teardown(request):
    """Access to the registered TestHandler instance during item teardown.

    This returns the logbook.TestHandler instance in use during
    execution of the teardown executed for the test.  It will only
    contain log records emitted during this time.
    """
    return request.node.logbook_handler_teardown
