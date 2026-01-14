from dotenv import load_dotenv
import os
from uuid import uuid4


def pytest_configure():
    load_dotenv(verbose=True, override=True)
    if "RUN_ID" not in os.environ:
        run_id = str(uuid4())[:8]
        os.environ["RUN_ID"] = str(run_id)
    print(f"Running with run_id='{os.environ['RUN_ID']}'")
