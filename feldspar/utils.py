import filetype

def infer_compression(filepath):
    return filetype.guess(filepath)
    