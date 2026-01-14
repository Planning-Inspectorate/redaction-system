from core.redaction.redactor import RedactorFactory
from core.redaction.redactor import Redactor
from core.redaction.exceptions import (
    RedactorNameNotFoundException,
    DuplicateRedactorNameException,
)
import pytest
import mock


"""
Create some mock redactor classes for testing
"""


class MockRedactorA(Redactor):
    @classmethod
    def get_name(cls) -> str:
        return "redactorA"


class MockRedactorB(Redactor):
    @classmethod
    def get_name(cls) -> str:
        return "redactorB"


class MockRedactorC(Redactor):
    @classmethod
    def get_name(cls) -> str:
        return "redactorC"


class MockRedactorD(Redactor):
    @classmethod
    def get_name(cls) -> str:
        # This one has a name that duplicates the name of MockRedactorA
        return MockRedactorA.get_name()


def test__redactor_factory__validate_redactor_types__successful():
    """
    - Given we have a RedactorFactory with REDACTOR_TYPES [MockRedactorA, MockRedactorB, MockRedactorC]
    - When we call _validate_redactor_types
    - Then we should receive a dictionary of the form <redactor_class.get_name(): redactor_class>
    """
    with mock.patch.object(
        RedactorFactory, "REDACTOR_TYPES", [MockRedactorA, MockRedactorB, MockRedactorC]
    ):
        expected_result = {
            "redactorA": MockRedactorA,
            "redactorB": MockRedactorB,
            "redactorC": MockRedactorC,
        }
        actual_result = RedactorFactory._validate_redactor_types()
        assert expected_result == actual_result


def test__redactor_factory__validate_redactor_types__with_duplicate_names():
    """
    - Given we have a RedactorFactory with a duplicate Redactor name "redactorA"
    - When we call _validate_redactor_types
    - Then a DuplicateRedactorNameException exception should be raised
    """
    with mock.patch.object(
        RedactorFactory,
        "REDACTOR_TYPES",
        [MockRedactorA, MockRedactorB, MockRedactorC, MockRedactorD],
    ):
        with pytest.raises(DuplicateRedactorNameException):
            RedactorFactory._validate_redactor_types()


def test__redactor_factory__get__successful():
    """
    - Given we have a RedactorFactory with REDACTOR_TYPES [MockRedactorA, MockRedactorB, MockRedactorC]
    - When we call get() with name="redactorB"
    - Then we should received MockRedactorB
    """
    with mock.patch.object(
        RedactorFactory, "REDACTOR_TYPES", [MockRedactorA, MockRedactorB, MockRedactorC]
    ):
        expected_factory = MockRedactorB
        actual_factory = RedactorFactory.get("redactorB")
        assert expected_factory == actual_factory


def test__redactor_factory__get__name_not_found():
    """
    - Given we have a RedactorFactory with some redactor types
    - When we call get() with a redactor name that is not defined in any of our Redactor classes
    - Then a RedactorNameNotFoundException exception should be raised
    """
    with mock.patch.object(
        RedactorFactory, "REDACTOR_TYPES", [MockRedactorA, MockRedactorB, MockRedactorC]
    ):
        with pytest.raises(RedactorNameNotFoundException):
            RedactorFactory.get("redactorD")


def test__redactor_factory__get__invalid_input():
    """
    - When I call redactor factory with an input that is not a string
    - Then a value error should be raised to alert the user about invalid input
    """
    with mock.patch.object(
        RedactorFactory, "REDACTOR_TYPES", [MockRedactorA, MockRedactorB, MockRedactorC]
    ):
        with pytest.raises(ValueError):
            RedactorFactory.get(1)
