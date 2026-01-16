from test.util.conftest_util import configure_session, session_setup, session_teardown  # noqa: F401


def pytest_configure():
    configure_session()
