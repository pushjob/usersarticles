import pandas as pd
from logger import logger


class DfCsvProvider:
    def provide_from_csv(self, csv_path, sep=",", header=0):
        logger.info(f"Reading file {csv_path}")
        df = pd.read_csv(csv_path, sep=sep, engine="python", header=header)

        return df
