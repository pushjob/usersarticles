from DfCsvProvider import DfCsvProvider

csv_path = "./resources/data_sample.log"
csv_new_line = "\r\n"


class TrackingEventLogRawDataProvider(DfCsvProvider):
    def provide(self):
        return self.provide_from_csv(csv_path, sep=csv_new_line, header=None)
