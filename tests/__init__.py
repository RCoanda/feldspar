import os

DATASETS_PATH = os.path.join(os.path.abspath(
    os.path.dirname(os.path.dirname(__file__))), "data")
RUNNING_EXAMPLE_XES_PATH = os.path.join(DATASETS_PATH, "running-example.xes")
RUNNING_EXAMPLE_XES_GZ_PATH = os.path.join(DATASETS_PATH, "running-example.xes.gz")
RUNNING_EXAMPLE_XES_ZIP_PATH = os.path.join(DATASETS_PATH, "running-example.zip")
RUNNING_EXAMPLE_CSV_PATH = os.path.join(DATASETS_PATH, "running-example.csv")
RUNNING_EXAMPLE_XLS_PATH = os.path.join(DATASETS_PATH, "running-example.xls")