import pandas as pd
from DfCsvWriter import DfCsvWriter

csv_path = "./resources/user_articles_report.csv"


class UserArticlesReportWriter(DfCsvWriter):
    def write(self, df: pd.DataFrame):
        self.write_csv(df, csv_path)
