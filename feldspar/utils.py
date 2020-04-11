import os

import filetype


def infer_compression(filepath):
    return filetype.guess(filepath)
    
def validate_filepath(filepath):
    return os.path.exists(filepath)
