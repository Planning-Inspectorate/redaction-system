from test.util.test_case import TestCase
from filelock import FileLock
from dotenv import load_dotenv
from uuid import uuid4
from typing import List, Type
import os
import logging
import pytest
import sys
import inspect
import importlib


def configure_session():
    load_dotenv(verbose=True, override=True)
    if "RUN_ID" not in os.environ:
        run_id = str(uuid4())[:8]
        os.environ["RUN_ID"] = str(run_id)
    logging.info(f"Running with run_id='{os.environ['RUN_ID']}'")
    import_all_testing_modules()


def import_all_testing_modules():
    """
    Import all test modules
    """
    # Extract all testing modules under the `test/` directory
    module_content_to_exclude = {"__init__.py", "__pycache__", "conftest.py"}
    python_modules_to_load = []
    files_to_explore = [
        x
        for x in os.listdir(os.path.join("test"))
        if "test" in x and os.path.isdir(os.path.join("test", x))
    ]
    while files_to_explore:
        next_file = files_to_explore.pop(0)
        if os.path.isfile(os.path.join("test", next_file)):
            if next_file.endswith(".py") and all(
                x not in next_file for x in module_content_to_exclude
            ):
                python_modules_to_load.append(next_file)
        else:
            files_to_explore.extend(
                [
                    os.path.join(next_file, x)
                    for x in os.listdir(os.path.join("test", next_file))
                ]
            )
    python_modules_to_load_cleaned = sorted(
        [x.replace(".py", "").replace("/", ".") for x in python_modules_to_load]
    )
    python_modules_to_load_cleaned = [
        f"test.{x}" if not x.startswith("test") else x
        for x in python_modules_to_load_cleaned
    ]
    # Import all testing modules
    for module_to_import in python_modules_to_load_cleaned:
        importlib.import_module(module_to_import)
    return python_modules_to_load_cleaned


def extract_all_test_cases():
    """
    Iterate through the imported modules to select all TestCase instances
    """
    test_modules = [module for module in sys.modules if module.startswith("test.")]
    return {
        obj
        for module in test_modules
        for name, obj in inspect.getmembers(sys.modules[module])
        if inspect.isclass(obj) and issubclass(obj, TestCase) and not obj == TestCase
    }


def process_arguments(session) -> List[Type[TestCase]]:
    """
    Process the pytest invocation parameters to return a list of test cases whos
    module setup/teardown functions need to be called
    """
    test_cases = extract_all_test_cases()
    pytest_args = session.config.invocation_params.args
    directory_args = [x.replace(".py", "") for x in pytest_args if x.startswith("test")]
    # If no arguments were given or if no specific python files were given then
    # all unit tests are being executed
    if not directory_args:
        return test_cases
    test_case_module_map = {
        test_case.__module__.replace(".", "/"): test_case for test_case in test_cases
    }
    matched_modules = []
    for directory in directory_args:
        matches = [
            module for module in test_case_module_map.keys() if directory in module
        ]
        matched_modules += matches
    return [test_case_module_map[directory] for directory in set(matched_modules)]


def _session_setup_task(session):
    logging.info("Setting up pytest session for unit tests")
    # Test-specific resources
    for test_case in process_arguments(session):
        logging.info("    Running setup for " + test_case.__module__)
        test_case().session_setup()


@pytest.fixture(scope="session", autouse=True)
def session_setup(tmp_path_factory, worker_id, request):
    # Code based on example from docs at
    # https://pytest-xdist.readthedocs.io/en/latest/how-to.html#making-session-scoped-fixtures-execute-only-once
    if worker_id == "master":
        return _session_setup_task(request.session)
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "setupsession.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            master_worker_written_value = fn.read_text()
            if "Failed" in master_worker_written_value:
                pytest.fail("Master thread failed during setup")
        else:
            fn.write_text("Starting session setup")
            try:
                _session_setup_task(request.session)
                fn.write_text("Complete")
            except KeyboardInterrupt:
                sys.exit()
            except Exception as e:
                fn.write_text("Failed")
                raise e


def _session_teardown_task(session):
    logging.info("Tearing down pytest session for unit tests")
    for test_case in process_arguments(session):
        logging.info("    Running teardown for " + test_case.__module__)
        test_case().session_teardown()


@pytest.fixture(scope="session", autouse=True)
def session_teardown(tmp_path_factory, worker_id, request):
    # Code based on example from docs at
    # https://pytest-xdist.readthedocs.io/en/latest/how-to.html#making-session-scoped-fixtures-execute-only-once
    yield
    if worker_id == "master":
        return _session_teardown_task(request.session)
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "teardownsession.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            number_of_completed_workers = int(fn.read_text()) + 1
            fn.write_text(str(number_of_completed_workers))
        else:
            number_of_completed_workers = 1
            fn.write_text(str(number_of_completed_workers))
    # Call teardown if this call was initiated by the last worker that is still running
    num_tests = len(request.session.items)
    if num_tests < request.config.workerinput["workercount"]:
        last_worker = number_of_completed_workers >= num_tests
    else:
        last_worker = (
            number_of_completed_workers >= request.config.workerinput["workercount"]
        )
    logging.info("Num tests: " + str(num_tests))
    logging.info("completed workers count: " + str(number_of_completed_workers))
    logging.info("Last worker: " + str(last_worker))
    if last_worker:
        _session_teardown_task(request.session)
