from DfCsvProvider import DfCsvProvider

csv_path = "./resources/log_cleaned.csv"


class TrackingEventLogDataProvider(DfCsvProvider):
    def provide(self):
        return self.provide_from_csv(csv_path)
