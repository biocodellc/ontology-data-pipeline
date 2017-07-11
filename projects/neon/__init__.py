from .preprocessor import NEONPreProcessor

import os
import pandas as pd

NEON_DATA_DIR = 'NEON_obs-phenology-plant'
INTENSITY_FILE = 'intensity_values.csv'
INTENSITY_VALUE_FRAME = pd.read_csv(INTENSITY_FILE, skipinitialspace=True, header=0) if os.path.exists(
    INTENSITY_FILE) else None


def walk_files(input_dir):
    for root, dirs, files in os.walk(os.path.join(input_dir, NEON_DATA_DIR)):

        for file in files:
            if file.endswith(".zip"):
                yield os.path.join(root, file)
