import multiprocessing

import line_profiler

from benchmarks.utils import timerfunc
from feldspar import TraceGenerator
from feldspar.generators import XESImporter

BPI_CHALLENGE_2012_PATH = "/mnt/a/personal/feldspar/data/bpi_challenge_2012.xes.gz"


class BenchmarkMappingElementGenerator:
    
    @staticmethod
    def label_initial(trace):
        for event in trace:
            event["concept:name"] = event["concept:name"][0]
        return trace

    @timerfunc
    def benchmark_map_serial(self, L):
        L = L.map(BenchmarkMappingElementGenerator.label_initial)
        list(L)


def benchmark_bottleneck(self):
    p = multiprocessing.Pool(4)
    L = XESImporter(BPI_CHALLENGE_2012_PATH, "gz")
    p.map(BenchmarkMappingElementGenerator.label_initial, L)

@timerfunc
def setup(cache=False):
    L = TraceGenerator.from_file(BPI_CHALLENGE_2012_PATH, "gz")
    if cache: 
        L = L.cache()
        list(L)
    return L


if __name__ == "__main__":
    L = setup(cache=True)
    print("Setup complete!\n")

    BenchmarkMappingElementGenerator().benchmark_map_serial(L)
    