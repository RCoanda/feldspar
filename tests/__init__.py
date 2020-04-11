import os
import pytest
import calendar
import time

HERE = os.path.abspath(os.path.dirname(__file__))
DATASETS_PATH = os.path.join(
    os.path.dirname(HERE), "data")

RUNNING_EXAMPLE_XES_PATH = os.path.join(DATASETS_PATH, "running-example.xes")
RUNNING_EXAMPLE_XES_GZ_PATH = os.path.join(
    DATASETS_PATH, "running-example.xes.gz")
RUNNING_EXAMPLE_XES_ZIP_PATH = os.path.join(
    DATASETS_PATH, "running-example.zip")
RUNNING_EXAMPLE_CSV_PATH = os.path.join(DATASETS_PATH, "running-example.csv")
RUNNING_EXAMPLE_XLS_PATH = os.path.join(DATASETS_PATH, "running-example.xls")
RUNNING_EXAMPLE_TRACES_LEN_LTEQ_5_PATH = os.path.join(DATASETS_PATH, "running-example-traces-len-lteq-5.xes")
