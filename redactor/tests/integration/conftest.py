from tests.util.conftest_util import (  # noqa: F401
    configure_session,
    session_setup,
    session_teardown,
)


def pytest_configure():
    configure_session()
