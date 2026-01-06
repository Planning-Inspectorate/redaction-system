from redactor.core.redaction.file_processor import FileProcessor, FileProcessorFactory
import pytest
import mock


class FileProcessorA(FileProcessor):
    @classmethod
    def get_name(cls):
        return "A"


class FileProcessorB(FileProcessor):
    @classmethod
    def get_name(cls):
        return "B"


class FileProcessorC(FileProcessor):
    @classmethod
    def get_name(cls):
        return "C"


class FileProcessorADuplicate(FileProcessor):
    @classmethod
    def get_name(cls):
        return "A"


@pytest.mark.parametrize("expected_name", ["pdf"])
def test__file_processor_factory__get(expected_name: str):
    assert issubclass(FileProcessorFactory.get(expected_name), FileProcessor)


def test__file_processor_factory__get__invalid_input():
    with mock.patch.object(
        FileProcessorFactory,
        "PROCESSORS",
        {FileProcessorA, FileProcessorB, FileProcessorC},
    ):
        with pytest.raises(Exception):
            FileProcessorFactory.get(1)


def test__file_processor_factory__get__missing_name():
    with mock.patch.object(
        FileProcessorFactory,
        "PROCESSORS",
        {FileProcessorA, FileProcessorB, FileProcessorC},
    ):
        with pytest.raises(Exception):
            FileProcessorFactory.get("bah")


def test__file_processor_factory__get__all_unique():
    all_names = [x.get_name() for x in FileProcessorFactory.PROCESSORS]
    name_values = []
    failed_names = []
    for name in all_names:
        try:
            val = FileProcessorFactory.get(name)
            name_values.append(val)
        except ValueError:
            failed_names.append(name)
    if failed_names:
        raise AssertionError(
            f"The following names are missing from FileProcessorFactory: {failed_names}"
        )
    assert len(set(name_values)) == len(name_values), (
        "There should be no duplicate return values for the FileProcessorFactory"
    )


def test__file_processor_factory__validate_processor_types__with_duplicate_type():
    with mock.patch.object(
        FileProcessorFactory,
        "PROCESSORS",
        {FileProcessorA, FileProcessorB, FileProcessorADuplicate},
    ):
        with pytest.raises(Exception):
            FileProcessorFactory.get("A")


def test__file_processor_factory__get_all():
    with mock.patch.object(
        FileProcessorFactory,
        "PROCESSORS",
        {FileProcessorA, FileProcessorB, FileProcessorADuplicate},
    ):
        assert FileProcessorFactory.get_all() == {
            FileProcessorA,
            FileProcessorB,
            FileProcessorADuplicate,
        }
