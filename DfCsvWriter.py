import os
import pandas as pd
from os import path
from logger import logger


class DfCsvWriter:
    def write_csv(self, df: pd.DataFrame, csv_path):
        if path.exists(csv_path):
            logger.warn(f"File exist and will be removed: {csv_path}")
            os.remove(csv_path)

        logger.info(f"Writing to file {csv_path}")
        df.to_csv(csv_path, index=False)
