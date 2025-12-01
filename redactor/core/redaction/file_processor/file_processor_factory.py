from redactor.core.redaction.file_processor.file_processor import FileProcessor
from redactor.core.redaction.file_processor.pdf_processor import PDFProcessor
from redactor.core.redaction.file_processor.exceptions import DuplicateFileProcessorNameException, FileProcessorNameNotFoundException
from typing import Type, Set, Dict, List
import json


class FileProcessorFactory():
    PROCESSORS: Set[Type[FileProcessor]] = {
        PDFProcessor
    }

    @classmethod
    def _validate_processor_types(cls):
        """
        Validate the PROCESSORS and return a map of type_name: Type[FileProcessor]
        """
        name_map: Dict[str, List[Type[FileProcessor]]] = dict()
        for processor_type in cls.PROCESSORS:
            type_name = processor_type.get_name()
            if type_name in name_map:
                name_map[type_name].append(processor_type)
            else:
                name_map[type_name] = [processor_type]
        invalid_types = {
            k: v
            for k, v in name_map.items()
            if len(v) > 1
        }
        if invalid_types:
            raise DuplicateFileProcessorNameException(
                f"The following FileProcessor implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {
            k: v[0]
            for k, v in name_map.items()
        }

    @classmethod
    def get(cls, processor_type: str) -> Type[FileProcessor]:
        """
        Return the FileProcessor class that is identified by the provided type name
        
        :param str processor_type: The FileProcessor type name (which aligns with the get_name method of the FileProcessor)
        :return Type[FileProcessor]: The file processor class identified by the provided processor_type
        :raises FileProcessorNameNotFoundException: If the given processor_type is not found
        :raises DuplicateFileProcessorNameException: If there is a problem with the underlying config defined in FileProcessorFactory.PROCESSORS
        """
        if not isinstance(processor_type, str):
            raise ValueError(f"FileProcessorFactory.get expected a str, but got a {type(processor_type)}")
        name_map = cls._validate_processor_types()
        if processor_type not in name_map:
            raise FileProcessorNameNotFoundException(
                f"No file processor could be found for processor type '{processor_type}'"
            )
        return name_map[processor_type]
    
    @classmethod
    def get_all(cls) -> Set[Type[FileProcessor]]:
        return cls.PROCESSORS