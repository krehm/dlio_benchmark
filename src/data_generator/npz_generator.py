from src.data_generator.data_generator import DataGenerator

import numpy as np
from numpy import random


class NPZGenerator(DataGenerator):
    def __init__(self):
        super().__init__()

    def generate(self):
        super().generate()
        records = random.random((self._dimension, self._dimension, self.num_samples))
        record_labels = [0] * self.num_samples
        for i in range(0, int(self.num_files)):
            out_path_spec = "{}_{}_of_{}.csv".format(self._file_prefix, i, self.num_files)
            np.savez(out_path_spec, x=records, y=record_labels)