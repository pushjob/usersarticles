import pandas as pd
from DfCsvWriter import DfCsvWriter

csv_path = "./resources/log_cleaned.csv"


class TrackingEventLogCleanedDataWriter(DfCsvWriter):
    def write(self, df: pd.DataFrame):
        self.write_csv(df, csv_path)
